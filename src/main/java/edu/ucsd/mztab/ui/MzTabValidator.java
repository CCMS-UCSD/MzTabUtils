package edu.ucsd.mztab.ui;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import edu.ucsd.mztab.MzTabReader;
import edu.ucsd.mztab.TaskMzTabContext;
import edu.ucsd.mztab.exceptions.UnverifiableNativeIDException;
import edu.ucsd.mztab.model.MzTabFile;
import edu.ucsd.mztab.processors.PSMValidationProcessor;
import edu.ucsd.mztab.processors.SpectraRefValidationProcessor;

public class MzTabValidator
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String USAGE =
		"java -cp MzTabUtils.jar edu.ucsd.mztab.ui.MzTabValidator" +
		"\n\t-params     <ParameterFile>" +
		"\n\t[-mztab     <MzTabDirectory> " +
			"(if not provided, then validation is skipped]" +
		"\n\t[-mztabPath <MzTabRelativePath> (if not under MzTabDirectory)]" +
		"\n\t[-peak      <PeakListFilesDirectory>]" +
		"\n\t[-peakPath  <PeakListRelativePath> " +
			"(if not under PeakListFilesDirectory)]" +
		"\n\t[-scans     <ScansDirectory>]" +
		"\n\t[-result    <UploadedResultDirectory>]" +
		"\n\t-output     <ValidatedMzTabDirectory>" +
		"\n\t[-log       <LogFile> " +
			"(if not specified, log output will be printed to stdout)]" +
		"\n\t[-threshold <InvalidPSMPercentageToFail: 0-100> (default 10)]";
	public static final Double DEFAULT_FAILURE_THRESHOLD = 10.0;
	
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
			writer.println(
				"MzTab_file\tUploaded_file\tPSM_rows\tInvalid_PSM_rows");
			// validate all PSM rows, write proper line for each to the log
			for (File file : files) {
				// get this mzTab file
				MzTabFile mzTabFile = validation.context.getMzTabFile(file);
				// set up output file
				File outputFile =
					new File(validation.outputDirectory, file.getName());
				// set up reader
				MzTabReader reader = new MzTabReader(mzTabFile, outputFile);
				reader.addProcessor(new SpectraRefValidationProcessor(
					validation.uploadedResultDirectory,
					validation.scansDirectory));
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
								mzTabFile.getMzTabPath()), innerError);
						}
						reader = new MzTabReader(mzTabFile, outputFile);
						reader.addProcessor(new SpectraRefValidationProcessor(
							validation.uploadedResultDirectory,
							validation.scansDirectory, false));
						counts = new HashMap<String, Integer>(2);
						reader.addProcessor(new PSMValidationProcessor(counts));
						reader.read();
					}
					// otherwise, it's some other kind of
					// RuntimeException, so just throw it
					else throw error;
				}
				// calculate invalid percentage, apply specified threshold
				Integer psmRows = counts.get("psmRows");
				if (psmRows == null)
					psmRows = 0;
				Integer invalidRows = counts.get("invalidRows");
				if (invalidRows == null)
					invalidRows = 0;
				// if the mzTab file has more than the indicated
				// percentage of invalid PSMs, then fail
				Double percentage = null;
				if (psmRows == 0)
					percentage = 0.0;
				else percentage = (double)invalidRows / (double)psmRows * 100.0;
				if (percentage > validation.failureThreshold) {
					//System.err.println(validation.context.toString());
					die(String.format("Result file [%s] contains %s%% " +
						"invalid PSM rows. Please correct the file and " +
						"ensure that its referenced spectra are accessible " +
						"within linked peak list files, and then re-submit.",
						mzTabFile.getUploadedResultPath(), percentage));
				}
				// get relevant file name to print to output file
				String uploadedFilename = mzTabFile.getUploadedResultPath();
				if (uploadedFilename == null)
					uploadedFilename = mzTabFile.getMzTabFilename();
				// write log line
				writer.println(String.format("%s\t%s\t%d\t%d",
					file.getName(), uploadedFilename, psmRows, invalidRows));
				writer.flush();
			}
		} catch (Throwable error) {
			die(error.getMessage(), error);
		} finally {
			try { writer.close(); }
			catch (Throwable error) {}
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
		private File             outputDirectory;
		private File             logFile;
		private double           failureThreshold;
		
		/*====================================================================
		 * Constructors
		 *====================================================================*/
		public MzTabValidationOperation(
			File parameters, File mzTabDirectory, String mzTabRelativePath,
			File peakListDirectory, String peakListRelativePath,
			File scansDirectory, File resultDirectory, File outputDirectory,
			File logFile, String failureThreshold
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
					peakListDirectory, peakListRelativePath, parameters);
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
		File scansDirectory = null;
		File resultDirectory = null;
		File outputDirectory = null;
		File logFile = null;
		String failureThreshold = null;
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
				else if (argument.equals("-scans"))
					scansDirectory = new File(value);
				else if (argument.equals("-result"))
					resultDirectory = new File(value);
				else if (argument.equals("-output"))
					outputDirectory = new File(value);
				else if (argument.equals("-log"))
					logFile = new File(value);
				else if (argument.equals("-threshold"))
					failureThreshold = value;
				else return null;
			}
		}
		try {
			return new MzTabValidationOperation(params, mzTabDirectory,
				mzTabRelativePath, peakListDirectory, peakListRelativePath,
				scansDirectory, resultDirectory, outputDirectory, logFile,
				failureThreshold);
		} catch (Throwable error) {
			die("There was an error reading command line parameters " +
				"to set up mzTab validation operation.", error);
			return null;
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
