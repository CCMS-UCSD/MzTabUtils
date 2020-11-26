package edu.ucsd.mztab.ui;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import edu.ucsd.mztab.MzTabReader;
import edu.ucsd.mztab.TaskMzTabContext;
import edu.ucsd.mztab.exceptions.UnverifiableNativeIDException;
import edu.ucsd.mztab.model.MzTabConstants;
import edu.ucsd.mztab.model.MzTabFile;
import edu.ucsd.mztab.model.MzTabMsRun;
import edu.ucsd.mztab.model.MzTabSectionHeader;
import edu.ucsd.mztab.model.MzTabConstants.MzTabSection;
import edu.ucsd.mztab.processors.PSMValidationProcessor;
import edu.ucsd.mztab.processors.SpectraRefValidationProcessor;
import edu.ucsd.mztab.util.CommonUtils;

public class MzTabValidator
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String USAGE =
		"java -cp MzTabUtils.jar edu.ucsd.mztab.ui.MzTabValidator" +
		"\n\t-params              <ParameterFile>" +
		"\n\t[-mztab              <MzTabDirectory> " +
			"(if not provided, then validation is skipped]" +
		"\n\t[-mztabPath          <MzTabRelativePath> " +
			"(if not under MzTabDirectory)]" +
		"\n\t[-peak               <PeakListFilesDirectory>]" +
		"\n\t[-peakPath           <PeakListRelativePath> " +
			"(if not under PeakListFilesDirectory)]" +
		"\n\t[-peakCollection     <PeakListCollectionName> " +
			"(default \"peak_list_files\")]" +
		"\n\t[-scans              <ScansDirectory>]" +
		"\n\t[-result             <UploadedResultDirectory>]" +
        "\n\t[-conversionErrors   <ResultFileConversionErrorsDirectory>]" +
        "\n\t[-validationErrors   <ResultFileValidationErrorsDirectory>]" +
		"\n\t[-dataset            <DatasetID>|<DatasetIDFile>]" +
		"\n\t-output              <ValidatedMzTabDirectory>" +
		"\n\t[-log                <LogFile> " +
			"(if not specified, log output will be printed to stdout)]" +
		"\n\t[-threshold          <InvalidPSMPercentageToFail: 0-100> " +
			"(default 10)]" +
        "\n\t[-bypass             true/false (default false; " +
            "if set to true, will allow mzTab validation errors)]";
	public static final Double DEFAULT_FAILURE_THRESHOLD = 10.0;
	public static final String MZTAB_VALIDATION_LOG_HEADER_LINE =
		"MzTab_file\tUploaded_file\tFile_descriptor\t" +
		"PSM_rows\tInvalid_PSM_rows\tFound_PSMs";
	private static final String[] PSM_VALIDITY_COLUMNS = new String[]{
		MzTabConstants.VALID_COLUMN, MzTabConstants.INVALID_REASON_COLUMN
	};
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public static void main(String[] args) {
		MzTabValidationOperation validation = extractArguments(args);
		if (validation == null)
			die(USAGE);
		// if no mzTab files were found, then this is a partial
		// submission and no validation needs to occur
		if (validation.context == null || validation.mzTabDirectory == null)
			return;
		// read all mzTab files, ensure that all referenced spectra
		// are present in the provided peak list files
		File[] files = validation.mzTabDirectory.listFiles();
		if (files == null || files.length < 1) {
			System.out.println("No files were submitted in the " +
				"\"RESULT\" category for this dataset, so it will be " +
				"marked as an unsupported (i.e. partial) submission.");
			return;
		}
        // check result file conversion errors directory, note any files
        // that failed and copy into validation errors directory
        Collection<String> failedMangledNames = null;
        if (validation.conversionErrorsDirectory != null &&
            validation.validationErrorsDirectory != null) {
            File[] errorFiles = validation.conversionErrorsDirectory.listFiles();
            if (errorFiles != null && errorFiles.length > 0) {
                failedMangledNames = new LinkedHashSet<String>(errorFiles.length);
                for (File file : errorFiles) {
                    failedMangledNames.add(FilenameUtils.getBaseName(file.getName()));
                    try {
                        FileUtils.copyFileToDirectory(file, validation.validationErrorsDirectory);
                    } catch (Throwable error) {
                        die(String.format("Could not copy result file conversion error file [%s] " +
                            "to result file validation errors directory [%s].", file.getName(),
                            validation.validationErrorsDirectory.getAbsolutePath()));
                    }
                }
            }
        }
		// sort files alphabetically
		Arrays.sort(files);
		// set up log output stream
		PrintWriter writer = null;
		try {
			// if a log file was specified on the command line, write to it
			if (validation.logFile != null) {
				// ensure that log file exists
				if (validation.logFile.exists() == false &&
					validation.logFile.createNewFile() == false)
					die(String.format(
						"Could not create mzTab validation log file [%s]",
						validation.logFile.getAbsolutePath()));
				writer = new PrintWriter(new BufferedWriter(
					new FileWriter(validation.logFile, false)));
			}
			// otherwise, just write to stdout
			else writer = new PrintWriter(System.out);
			// write the header line of the log file
			writer.println(MZTAB_VALIDATION_LOG_HEADER_LINE);
			// validate all PSM rows, write proper line for each to the log
			for (File file : files) {
                // if this file failed to convert, don't validate it
                String mangledBase = FilenameUtils.getBaseName(file.getName());
                if (failedMangledNames != null && failedMangledNames.contains(mangledBase))
                    continue;
				// get this mzTab file
				MzTabFile mzTabFile = validation.context.getMzTabFile(file);
				// set up output file
				File outputFile =
					new File(validation.outputDirectory, file.getName());
				// validate this mzTab File
				validateMzTabFile(mzTabFile, outputFile,
					validation.uploadedResultDirectory, validation.validationErrorsDirectory,
					validation.scansDirectory, validation.failureThreshold,
					validation.bypassValidation, writer);
			}
			// write peak list stats to log
			logPeakListStats(
				validation.scansDirectory, validation.context, writer);
		} catch (Throwable error) {
			die(String.format(
				"There was an error validating mzTab directory [%s].",
				validation.mzTabDirectory.getAbsolutePath()), error);
		} finally {
			try { writer.close(); }
			catch (Throwable error) {}
		}
	}
	
	public static void validateMzTabFile(
		MzTabFile inputFile, File outputFile, File uploadedResultDirectory,
		File validationErrorsDirectory, File scansDirectory,
		double failureThreshold, boolean bypassValidation, PrintWriter writer
	) {
		if (inputFile == null || outputFile == null || writer == null)
			return;
		// set up reader
		MzTabReader reader = new MzTabReader(inputFile, outputFile);
		reader.addProcessor(new SpectraRefValidationProcessor(
			//resultDirectory,
			null,	// using null here for performance reasons
			scansDirectory));
		Map<String, Integer> counts = new HashMap<String, Integer>(2);
		reader.addProcessor(new PSMValidationProcessor(counts));
		// validate file
		try { reader.read(); }
		catch (RuntimeException error) {
			// if a RuntimeException is caught, and its parent
			// is an UnverifiableNativeIDException, then that
			// means that the validator tried to interpret
			// ambiguous nativeIDs as scan numbers but failed
			// somewhere along the way; in this case, try again
			// with a hard-coded interpretation scheme of indices
			Throwable parent = error.getCause();
			if (parent != null && parent.getClass().isInstance(
				UnverifiableNativeIDException.class)) {
				// be sure to clear the output file, since the previous
				// attempt may have written some rows to it
				try {
					if (outputFile.delete() == false)
						throw new RuntimeException(
							"File.delete() returned false.");
				} catch (Throwable innerError) {
					die(String.format("Could not delete output " +
						"file [%s] to attempt a second validation " +
						"pass on input mzTab file [%s].",
						outputFile.getAbsolutePath(),
						inputFile.getMzTabPath()), innerError);
				}
				reader = new MzTabReader(inputFile, outputFile);
				reader.addProcessor(new SpectraRefValidationProcessor(
					uploadedResultDirectory, scansDirectory, false));
				counts = new HashMap<String, Integer>(2);
				reader.addProcessor(new PSMValidationProcessor(counts));
				reader.read();
			}
			// otherwise, it's some other kind of
			// RuntimeException, so just throw it
			else throw error;
		}
		// calculate invalid percentage, apply specified threshold
		Integer psmRows = counts.get("PSM");
		if (psmRows == null)
			psmRows = 0;
		Integer invalidRows = counts.get("invalid_PSM");
		if (invalidRows == null)
			invalidRows = 0;
		// if the mzTab file has more than the indicated
		// percentage of invalid PSMs, then fail
		Double percentage = null;
		if (psmRows == 0)
			percentage = 0.0;
		else percentage = (double)invalidRows / (double)psmRows * 100.0;
		if (percentage > failureThreshold) {
			//System.err.println(validation.context.toString());
			// get the printed reason for this file's first invalid row
			ImmutablePair<Integer, String> reason = null;
			try { reason = getFirstInvalidReason(outputFile); }
			catch (Throwable error) {}
			// build error message
			StringBuilder message = new StringBuilder("Result file [");
			message.append(inputFile.getUploadedResultPath()).append("] ");
			message.append("contains ").append(percentage).append("% ");
			message.append("invalid PSM rows (").append(invalidRows);
			message.append(" invalid out of ").append(psmRows).append(").");
			if (reason != null) {
				message.append("\n\nThe first invalid PSM row is on line ");
				message.append(reason.getLeft());
				message.append(" of the converted mzTab file, with the ");
				message.append("following reason given for invalidity:");
				message.append("\n----------\n").append(reason.getRight());
				message.append("\n----------");
			}
			message.append("\n\nPlease correct the file and ");
			message.append("ensure that its referenced spectra are ");
			message.append("accessible within linked peak list files, ");
			message.append("and then re-submit.");
            // if validation bypass is not set, fail as normal
            if (bypassValidation == false)
                die(message.toString());
            // otherwise, log error and move on
            else {
                System.out.println(message.toString());
                if (validationErrorsDirectory != null) {
                    File errorFile =
                        new File(validationErrorsDirectory, inputFile.getMangledMzTabFilename());
                    PrintWriter errorWriter = null;
                    try {
                        errorWriter = new PrintWriter(errorFile);
                        errorWriter.println(message.toString());
                    } catch (Throwable error) {
                        die(String.format("Could not write validation error message " +
                            "for mzTab file [%s] to result file validation errors directory [%s].",
                            inputFile.getMzTabPath(), validationErrorsDirectory.getAbsolutePath()),
                            error);
                    } finally {
                        try { errorWriter.close(); }
                        catch (Throwable error) {}
                    }
                }
                return;
            }
		}
		// get relevant file name to print to output file
		String uploadedFilename = inputFile.getUploadedResultPath();
		if (uploadedFilename == null)
			uploadedFilename = inputFile.getMzTabFilename();
		// get counted number of unique PSMs
		Integer uniquePSMs = counts.get("PSM_ID");
		if (uniquePSMs == null)
			uniquePSMs = 0;
		// write log line
		writer.println(String.format("%s\t%s\t%s\t%d\t%d\t%d",
			inputFile.getFile().getName(), uploadedFilename,
			inputFile.getDescriptor(), psmRows, invalidRows, uniquePSMs));
		writer.flush();
	}
	
	public static void logPeakListStats(
		File scansDirectory, TaskMzTabContext context, PrintWriter writer
	) {
		if (scansDirectory == null || scansDirectory.canRead() == false ||
			context == null || writer == null)
			return;
		// write to the log the spectrum counts from all scans files
		writer.println("Scans_file\tUploaded_file\tFile_descriptor\tSpectra");
		File[] files = scansDirectory.listFiles();
		if (files != null && files.length > 0) {
			// sort files alphabetically
			Arrays.sort(files);
			// read through each scans file and log its line count
			BufferedReader reader = null;
			for (File file : files) try {
				// get uploaded peak list file descriptor
				MzTabMsRun msRun = context.getPeakListFile(file.getName());
				// if no peak list file mapping could be found for this scans
				// file, then that means the corresponding peak list file was
				// not actually searched by this set of mzTabs, and therefore
				// we do not want to include it in this summary
				if (msRun == null)
					continue;
				// read through file, counting the MS2 spectra
				reader = new BufferedReader(new FileReader(file));
				String line = null;
				int lineNumber = 0;
				int ms2Spectra = 0;
				while (true) {
					line = reader.readLine();
					if (line == null)
						break;
					lineNumber++;
					if (line.trim().equals(""))
						continue;
					String[] tokens = line.split("\\t");
					if (tokens == null || tokens.length != 3)
						throw new IllegalArgumentException(String.format(
							"Line %d of spectrum IDs file [%s] is invalid:\n" +
							"----------\n%s\n----------\n" +
							"Each non-empty line is expected to consist of " +
							"three tokens separated by whitespace.",
							lineNumber, file.getName(), line));
					// only count MS2 spectra (i.e. having value = 2 in the MS level column)
					try {
						if (Integer.parseInt(tokens[1]) == 2)
							ms2Spectra++;
					} catch (NumberFormatException error) { continue; }
				}
				// write log line
				writer.println(String.format("%s\t%s\t%s\t%d",
					file.getName(), msRun.getUploadedPeakListPath(),
					msRun.getDescriptor(), ms2Spectra));
				writer.flush();
			} catch (RuntimeException error) {
				throw error;
			} catch (Throwable error) {
				throw new RuntimeException(error);
			} finally {
				try { reader.close(); }
				catch (Throwable error) {}
			}
		}
	}
	
	/*========================================================================
	 * Convenience classes
	 *========================================================================*/
	/**
	 * Struct to maintain context data for each mzTab validation operation.
	 */
	private static class MzTabValidationOperation {
		/*====================================================================
		 * Properties
		 *====================================================================*/
		private TaskMzTabContext context;
		private File             mzTabDirectory;
		private File             scansDirectory;
		private File             uploadedResultDirectory;
        private File             conversionErrorsDirectory;
        private File             validationErrorsDirectory;
		private File             outputDirectory;
		private File             logFile;
		private double           failureThreshold;
		private boolean          bypassValidation;
		
		/*====================================================================
		 * Constructors
		 *====================================================================*/
		public MzTabValidationOperation(
			File parameters, File mzTabDirectory, String mzTabRelativePath,
			File peakListDirectory, String peakListRelativePath,
			String peakListCollection, File scansDirectory, File resultDirectory,
			File conversionErrorsDirectory, File validationErrorsDirectory,
			String datasetID, File outputDirectory, File logFile,
			String failureThreshold, Boolean bypassValidation
		) {
			// validate parameters file
			if (parameters == null)
				throw new NullPointerException(
					"Parameters file cannot be null.");
			else if (parameters.isFile() == false)
				throw new IllegalArgumentException(
					String.format(
						"Parameters file [%s] must be a regular file.",
						parameters.getAbsolutePath()));
			else if (parameters.canRead() == false)
				throw new IllegalArgumentException(
					String.format("Parameters file [%s] must be readable.",
						parameters.getAbsolutePath()));
			// validate mzTab directory (if null,
			// then no validation is necessary)
			if (mzTabDirectory != null) {
				if (mzTabDirectory.isDirectory() == false)
					throw new IllegalArgumentException(String.format(
						"MzTab directory [%s] must be a directory.",
						mzTabDirectory.getAbsolutePath()));
				else if (mzTabDirectory.canRead() == false)
					throw new IllegalArgumentException(String.format(
						"MzTab directory [%s] must be readable.",
						mzTabDirectory.getAbsolutePath()));
			}
			this.mzTabDirectory = mzTabDirectory;
			// validate peak list files directory (can be null)
			if (peakListDirectory != null) {
				if (peakListDirectory.isDirectory() == false)
					throw new IllegalArgumentException(String.format(
						"Peak list files directory [%s] must be a directory.",
						peakListDirectory.getAbsolutePath()));
				else if (peakListDirectory.canRead() == false)
					throw new IllegalArgumentException(String.format(
						"Peak list files directory [%s] must be readable.",
						peakListDirectory.getAbsolutePath()));
			}
			// build mzTab file-mapping context, if applicable
			if (mzTabDirectory != null)
				context = new TaskMzTabContext(
					mzTabDirectory, mzTabRelativePath,
					peakListDirectory, peakListRelativePath,
					peakListCollection, parameters, datasetID);
			else context = null;
			// validate scans directory (can be null)
			if (scansDirectory != null) {
				if (scansDirectory.isDirectory() == false)
					throw new IllegalArgumentException(String.format(
						"Spectrum ID files directory [%s] must be a directory.",
						scansDirectory.getAbsolutePath()));
				else if (scansDirectory.canRead() == false)
					throw new IllegalArgumentException(String.format(
						"Spectrum ID files directory [%s] must be readable.",
						scansDirectory.getAbsolutePath()));
			}
			this.scansDirectory = scansDirectory;
			// validate uploaded result directory (can be null)
			if (resultDirectory != null) {
				if (resultDirectory.isDirectory() == false)
					throw new IllegalArgumentException(String.format(
						"Uploaded result files directory [%s] must be a " +
						"directory.", resultDirectory.getAbsolutePath()));
				else if (resultDirectory.canRead() == false)
					throw new IllegalArgumentException(String.format(
						"Uploaded result files directory [%s] must be " +
						"readable.", resultDirectory.getAbsolutePath()));
			}
			// commenting this out for now, since the performance of parsing
			// mzid files over NFS is pretty awful, and we don't get much out
			// of it since we can resolve ambiguous nativeIDs by just checking
			// against the spectrum IDs map as both scan and index anyway
//			this.uploadedResultDirectory = resultDirectory;
			this.uploadedResultDirectory = null;
            // validate result file conversion errors directory (may be null)
            if (conversionErrorsDirectory != null) {
                if (conversionErrorsDirectory.isDirectory() == false)
                    throw new IllegalArgumentException(String.format(
                        "Result file conversion errors directory [%s] must be a directory.",
                        conversionErrorsDirectory.getAbsolutePath()));
                else if (conversionErrorsDirectory.canRead() == false)
                    throw new IllegalArgumentException(String.format(
                        "Result file conversion errors directory [%s] must be readable.",
                        conversionErrorsDirectory.getAbsolutePath()));
                this.conversionErrorsDirectory = conversionErrorsDirectory;
            }
            // validate result file validation errors output directory (may be null)
            if (validationErrorsDirectory != null) {
                if (validationErrorsDirectory.isDirectory() == false)
                    throw new IllegalArgumentException(String.format(
                        "Result file validation errors directory [%s] must be a directory.",
                        validationErrorsDirectory.getAbsolutePath()));
                else if (validationErrorsDirectory.canWrite() == false)
                    throw new IllegalArgumentException(String.format(
                        "Result file validation errors directory [%s] must be writable.",
                        validationErrorsDirectory.getAbsolutePath()));
                this.validationErrorsDirectory = validationErrorsDirectory;
            }
			// validate processed mzTab output directory
			if (outputDirectory == null)
				throw new NullPointerException(
					"Output directory cannot be null.");
			else if (outputDirectory.isDirectory() == false)
				throw new IllegalArgumentException(
					String.format("Output directory [%s] must be a directory.",
						outputDirectory.getAbsolutePath()));
			else if (outputDirectory.canWrite() == false)
				throw new IllegalArgumentException(
					String.format("Output directory [%s] must be writable.",
						outputDirectory.getAbsolutePath()));
			else this.outputDirectory = outputDirectory;
			// validate log file (may be null)
			if (logFile != null && logFile.isDirectory())
				throw new IllegalArgumentException(String.format(
					"mzTab validation log file [%s] must be a normal " +
					"(non-directory) file.", logFile.getAbsolutePath()));
			else this.logFile = logFile;
			// determine failure threshold for mzTab validation
			if (failureThreshold != null) try {
				this.failureThreshold = Double.parseDouble(failureThreshold);
			} catch (NumberFormatException error) {
				throw new IllegalArgumentException(String.format(
					"Failure threshold [%s] must be a real number " +
					"between 0 and 100.", failureThreshold));
			} else this.failureThreshold = DEFAULT_FAILURE_THRESHOLD;
            // initialize bypass flag
            if (bypassValidation == null)
                this.bypassValidation = false;
            else this.bypassValidation = bypassValidation;
		}
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private static MzTabValidationOperation extractArguments(String[] args) {
		if (args == null || args.length < 1)
			return null;
		File params = null;
		File mzTabDirectory = null;
		String mzTabRelativePath = null;
		File peakListDirectory = null;
		String peakListRelativePath = null;
		String peakListCollection = null;
		File scansDirectory = null;
		File resultDirectory = null;
        File conversionErrorsDirectory = null;
        File validationErrorsDirectory = null;
		String datasetID = null;
		File outputDirectory = null;
		File logFile = null;
		String failureThreshold = null;
        Boolean bypassValidation = null;
		for (int i=0; i<args.length; i++) {
			String argument = args[i];
			if (argument == null)
				return null;
			else {
				i++;
				if (i >= args.length)
					return null;
				String value = args[i];
				if (argument.equals("-params"))
					params = new File(value);
				else if (argument.equals("-mztab"))
					mzTabDirectory = new File(value);
				else if (argument.equals("-mztabPath"))
					mzTabRelativePath = value;
				else if (argument.equals("-peak"))
					peakListDirectory = new File(value);
				else if (argument.equals("-peakPath"))
					peakListRelativePath = value;
				else if (argument.equals("-peakCollection"))
					peakListCollection = value;
				else if (argument.equals("-scans"))
					scansDirectory = new File(value);
				else if (argument.equals("-result"))
					resultDirectory = new File(value);
                else if (argument.equals("-conversionErrors"))
                    conversionErrorsDirectory = new File(value);
                else if (argument.equals("-validationErrors"))
                    validationErrorsDirectory = new File(value);
				else if (argument.equals("-dataset")) {
					// if this argument is a file, read it to get dataset ID
					File datasetIDFile = new File(value);
					if (datasetIDFile.isFile() && datasetIDFile.canRead()) {
						RandomAccessFile input = null;
						try {
							input = new RandomAccessFile(datasetIDFile, "r");
							datasetID = input.readLine();
						} catch (Throwable error) {
							die(String.format(
								"Could not read dataset ID from file [%s].",
								datasetIDFile.getAbsolutePath()), error);
						} finally {
							try { input.close(); } catch (Throwable error) {}
						}
					}
					// otherwise treat the argument as the literal dataset ID
					else datasetID = value;
				} else if (argument.equals("-output"))
					outputDirectory = new File(value);
				else if (argument.equals("-log"))
					logFile = new File(value);
				else if (argument.equals("-threshold"))
					failureThreshold = value;
                else if (argument.equals("-bypass"))
                    bypassValidation = CommonUtils.parseBooleanColumn(value);
				else return null;
			}
		}
		try {
			return new MzTabValidationOperation(params, mzTabDirectory,
				mzTabRelativePath, peakListDirectory, peakListRelativePath,
				peakListCollection, scansDirectory, resultDirectory,
				conversionErrorsDirectory, validationErrorsDirectory,
				datasetID, outputDirectory, logFile, failureThreshold, bypassValidation);
		} catch (Throwable error) {
			die("There was an error reading command line parameters " +
				"to set up mzTab validation operation.", error);
			return null;
		}
	}
	
	private static ImmutablePair<Integer, String> getFirstInvalidReason(
		File mzTabFile
	) {
		if (mzTabFile == null)
			return null;
		String mzTabFilename = mzTabFile.getAbsolutePath();
		// read through mzTab file line by line until
		// reaching the first invalid PSM row
		BufferedReader reader = null;
		try {
			MzTabSectionHeader psmHeader = null;
			int validIndex = -1;
			int invalidReasonIndex = -1;
			reader = new BufferedReader(new FileReader(mzTabFile));
			String line = null;
			int lineNumber = 0;
			while (true) {
				line = reader.readLine();
				if (line == null)
					break;
				lineNumber++;
				// set up PSM section header
				if (line.startsWith("PSH")) {
					if (psmHeader != null)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:" +
							"\n----------\n%s\n----------\n" +
							"A \"PSH\" row was already seen previously in " +
							"this file.", lineNumber, mzTabFilename, line));
					psmHeader = new MzTabSectionHeader(line);
					psmHeader.validateHeaderExpectations(
						MzTabSection.PSM, Arrays.asList(PSM_VALIDITY_COLUMNS));
					// record all relevant column indices
					List<String> headers = psmHeader.getColumns();
					for (int i=0; i<headers.size(); i++) {
						String header = headers.get(i);
						if (header == null)
							continue;
						else if (header.equalsIgnoreCase(
							MzTabConstants.VALID_COLUMN))
							validIndex = i;
						else if (header.equalsIgnoreCase(
							MzTabConstants.INVALID_REASON_COLUMN))
							invalidReasonIndex = i;
					}
					// ensure that both validity columns were found
					if (validIndex < 0)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:" +
							"\n----------\n%s\n----------\n" +
							"No \"%s\" column was found.",
							lineNumber, mzTabFilename, line,
							MzTabConstants.VALID_COLUMN));
					else if (invalidReasonIndex < 0)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:" +
							"\n----------\n%s\n----------\n" +
							"No \"%s\" column was found.",
							lineNumber, mzTabFilename, line,
							MzTabConstants.INVALID_REASON_COLUMN));
				}
				// note the validity status of this PSM row
				else if (line.startsWith("PSM")) {
					if (psmHeader == null)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:" +
							"\n----------\n%s\n----------\n" +
							"A \"PSM\" row was found before any \"PSH\" row.",
							lineNumber, mzTabFilename, line));
					else psmHeader.validateMzTabRow(line);
					// ensure that both validity columns are present in this row
					String[] row = line.split("\\t");
					if (validIndex >= row.length)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:" +
							"\n----------\n%s\n----------\n" +
							"The \"%s\" column was found at index %d in the " +
							"PSM section header, but this PSM row contains " +
							"only %d elements.", lineNumber, mzTabFilename,
							line, MzTabConstants.VALID_COLUMN,
							validIndex, row.length));
					else if (invalidReasonIndex >= row.length)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:" +
							"\n----------\n%s\n----------\n" +
							"The \"%s\" column was found at index %d in the " +
							"PSM section header, but this PSM row contains " +
							"only %d elements.", lineNumber, mzTabFilename,
							line, MzTabConstants.INVALID_REASON_COLUMN,
							invalidReasonIndex, row.length));
					// if this PSM row has been marked as invalid,
					// then return its recorded reason
					String valid = row[validIndex];
					if (valid != null &&
						valid.trim().equalsIgnoreCase("INVALID")) {
						String reason = row[invalidReasonIndex];
						if (reason == null || reason.trim().isEmpty())
							reason = "null";
						return new ImmutablePair<Integer, String>(
							lineNumber, reason);
					}
				}
			}
			return null;
		} catch (RuntimeException error) {
			throw error;
		} catch (Throwable error) {
			throw new RuntimeException(error);
		} finally {
			try { reader.close(); } catch (Throwable error) {}
		}
	}
	
	private static void die(String message) {
		die(message, null);
	}
	
	private static void die(String message, Throwable error) {
		if (message == null)
			message = "There was an error validating mzTab files.";
		if (error == null) {
			if (message.endsWith(".") == false)
				message += ".";
		} else {
			if (message.endsWith("."))
				message = message.substring(0, message.length() - 1);
			if (message.endsWith(":") == false)
				message += ":";
		}
		System.err.println(message);
		if (error != null)
			error.printStackTrace();
		System.exit(1);
	}
}
