package edu.ucsd.mztab;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.xpath.XPathAPI;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import edu.ucsd.mztab.model.MzTabConstants;
import edu.ucsd.mztab.model.MzTabFile;
import edu.ucsd.mztab.model.MzTabMsRun;
import edu.ucsd.util.CommonUtils;
import edu.ucsd.util.FileIOUtils;

public class MzTabValidator
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String USAGE =
		"java -cp MassIVEUtils.jar edu.ucsd.mztab.MzTabValidator" +
		"\n\t-params     <ParameterFile>" +
		"\n\t[-mztab     <MzTabDirectory> " +
			"(if not provided, then validation is skipped]" +
		"\n\t[-mztabPath <MzTabRelativePath> (if not under MzTabDirectory)]" +
		"\n\t[-peak      <PeakListFilesDirectory>]" +
		"\n\t[-peakPath  <PeakListRelativePath> " +
			"(if not under PeakListFilesDirectory)]" +
		"\n\t[-scans     <ScansDirectory>]" +
		"\n\t[-result    <UploadedResultDirectory>]" +
		"\n\t-output     <OutputFile>" +
		"\n\t[-threshold <InvalidPSMPercentageToFail: 0-100>]" +
		"\n\t[-count_only]";
	private static final String TEMPORARY_MZTAB_FILE =
		"temp_modified_result.mzTab";
	private static final Pattern FILE_REFERENCE_PATTERN =
		Pattern.compile("ms_run\\[(\\d+)\\]");
	private static final Pattern FILE_PATTERN = Pattern.compile("file=(.+)");
	private static final Double DEFAULT_FAILURE_THRESHOLD = 10.0;
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public static void main(String[] args) {
		MzTabValidationOperation validation = extractArguments(args);
		if (validation == null)
			die(USAGE);
		PrintWriter writer = null;
		try {
			// set up output file writer
			if (validation.outputFile.exists() == false &&
				validation.outputFile.createNewFile() == false)
				die(String.format("Could not create output file [%s]",
					validation.outputFile.getAbsolutePath()));
			// if no mzTab files were found, then this is a partial
			// submission and no validation needs to occur
			if (validation.context == null)
				return;
			// otherwise, prepare output file for validated mzTab statistics
			writer = new PrintWriter(new BufferedWriter(
				new FileWriter(validation.outputFile, false)));
			writer.println("MzTab_file\tUploaded_file\tFile_descriptor\t" +
				"PSM_rows\tInvalid_PSM_rows\tFound_PSMs\tPeptide_rows\t" +
				"Found_Peptides\tProtein_rows\tFound_Proteins");
			// read all scans files
			Map<String, ImmutablePair<Collection<Integer>, Collection<Integer>>>
				scans = new LinkedHashMap<String,
					ImmutablePair<Collection<Integer>, Collection<Integer>>>();
			Collection<File> scansFiles = null;
			if (validation.scansDirectory != null)
				scansFiles =
					Arrays.asList(validation.scansDirectory.listFiles());
			if (scansFiles != null) {
				for (File scansFile : scansFiles) {
					if (scansFile != null) {
						ImmutablePair<Collection<Integer>, Collection<Integer>>
							spectra = readScansFile(scansFile);
						scans.put(scansFile.getName(), spectra);
					}
				}
			}
			// read all mzTab files, ensure that all referenced spectra
			// are present in the provided peak list files
			Map<String, Document> parsedMzidFileCache =
				new LinkedHashMap<String, Document>();
			Map<Document, Map<String, Map<String, Collection<String>>>>
				mzidSpectrumIDCache =
					new LinkedHashMap<Document,
						Map<String, Map<String, Collection<String>>>>();
			Collection<MzTabFile> mzTabFiles =
				validation.context.getMzTabFiles();
			if (mzTabFiles == null || mzTabFiles.isEmpty()) {
				System.out.println("No files were submitted in the " +
					"\"RESULT\" category for this dataset, so it will be " +
					"marked as an unsupported (i.e. partial) submission.");
			} else for (MzTabFile mzTabFile : mzTabFiles) {
				// extract counts for PSMs, invalid PSMs, proteins and peptides
				int[] counts = validateMzTabFile(mzTabFile, validation.context,
					scans, validation.uploadedResultDirectory,
					parsedMzidFileCache, mzidSpectrumIDCache,
					validation.countOnly);
				// get original uploaded result filename for this mzTab file,
				// if present; it may not be, if this is a count-only TSV
				// conversion
				String uploadedResultFilename =
					mzTabFile.getUploadedResultPath();
				if (uploadedResultFilename == null)
					uploadedResultFilename = mzTabFile.getMzTabFilename();
				if (counts == null || counts.length != 7)
					die(String.format(
						"Result file [%s] could not be parsed for validation.",
						uploadedResultFilename));
				int psmRows = counts[0];
				int invalidRows = counts[1];
				// if this is a regular validation operation (i.e. not
				// count-only), and the mzTab file has more than the
				// indicated percentage of invalid PSMs, then fail
				double percentage =
					(double)invalidRows / (double)psmRows * 100.0;
				if (validation.countOnly == false &&
					percentage > validation.failureThreshold) {
					// log the filename map values, since that's
					// the most likely reason for this failure
					//System.err.println(validation.context.toString());
					die(String.format("Result file [%s] contains %s%% " +
						"invalid PSM rows. Please correct the file and " +
						"ensure that its referenced spectra are accessible " +
						"within linked peak list files, and then re-submit.",
						uploadedResultFilename, percentage));
				}
				int foundPSMs = counts[2];
				// only show found peptide and protein counts if the
				// corresponding sections were not found in the file
				int peptideRows = counts[3];
				int foundPeptides = counts[4];
				if (peptideRows > 0)
					foundPeptides = 0;
				int proteinRows = counts[5];
				int foundProteins = counts[6];
				if (proteinRows > 0)
					foundProteins = 0;
				// if it's good, write this mzTab file's row counts to the file
				String mangledMzTabFilename =
					mzTabFile.getMangledMzTabFilename();
				if (mangledMzTabFilename == null)
					mangledMzTabFilename = mzTabFile.getMzTabFilename();
				writer.println(String.format(
					"%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d",
					mangledMzTabFilename, uploadedResultFilename,
					mzTabFile.getDescriptor(), psmRows, invalidRows, foundPSMs,
					peptideRows, foundPeptides, proteinRows, foundProteins));
			}
		} catch (Throwable error) {
			die(getRootCause(error).getMessage(), error);
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
		private File    scansDirectory;
		private File    uploadedResultDirectory;
		private File    outputFile;
		private boolean countOnly;
		private double  failureThreshold;
		
		/*====================================================================
		 * Constructors
		 *====================================================================*/
		public MzTabValidationOperation(
			File parameters, File mzTabDirectory, String mzTabRelativePath,
			File peakListDirectory, String peakListRelativePath,
			File scansDirectory, File resultDirectory, File outputFile,
			boolean countOnly, String failureThreshold
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
			this.uploadedResultDirectory = resultDirectory;
			// validate output file
			if (outputFile == null)
				throw new NullPointerException("Output file cannot be null.");
			else if (outputFile.isDirectory())
				throw new IllegalArgumentException(String.format(
					"Output file [%s] must be a normal (non-directory) file.",
					outputFile.getAbsolutePath()));
			this.outputFile = outputFile;
			// determine validation mode
			this.countOnly = countOnly;
			// determine failure threshold for mzTab validation
			if (failureThreshold != null) try {
				this.failureThreshold = Double.parseDouble(failureThreshold);
			} catch (NumberFormatException error) {
				throw new IllegalArgumentException(String.format(
					"Failure threshold [%s] must be a real number " +
					"between 0 and 100.", failureThreshold));
			}
			else this.failureThreshold = DEFAULT_FAILURE_THRESHOLD;
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
		File output = null;
		boolean countOnly = false;
		String failureThreshold = null;
		for (int i=0; i<args.length; i++) {
			String argument = args[i];
			if (argument == null)
				return null;
			else if (argument.equals("-count_only"))
				countOnly = true;
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
					output = new File(value);
				else if (argument.equals("-threshold"))
					failureThreshold = value;
				else return null;
			}
		}
		try {
			return new MzTabValidationOperation(params, mzTabDirectory,
				mzTabRelativePath, peakListDirectory, peakListRelativePath,
				scansDirectory, resultDirectory, output, countOnly,
				failureThreshold);
		} catch (Throwable error) {
			System.err.println(error.getMessage());
			return null;
		}
	}
	
	private static ImmutablePair<Collection<Integer>, Collection<Integer>>
	readScansFile(File scansFile) throws Exception {
		if (scansFile == null)
			throw new NullPointerException("Scans file is null.");
		else if (scansFile.isFile() == false || scansFile.canRead() == false)
			throw new IllegalArgumentException(String.format(
				"Argument scans file [%s] is not a readable file.",
				scansFile.getName()));
		// read all lines of the input scans file and store them
		Collection<Integer> scans = new LinkedHashSet<Integer>();
		Collection<Integer> indices = new LinkedHashSet<Integer>();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(scansFile));
			String line = null;
			int lineCount = 0;
			while (true) {
				line = reader.readLine();
				if (line == null)
					break;
				lineCount++;
				if (line.trim().equals(""))
					continue;
				String[] tokens = line.split("\\s+");
				if (tokens == null || tokens.length != 3)
					throw new IllegalArgumentException(String.format(
						"Line %d of scans file [%s] is invalid:\n" +
						"----------\n%s\n----------\nEach non-empty line is " +
						"expected to consist of three tokens separated by " +
						"whitespace.", lineCount, scansFile.getName(), line));
				// parse scan and index fields, which should be integers
				try {
					scans.add(Integer.parseInt(tokens[1]));
				} catch (NumberFormatException error) {
					throw new RuntimeException(String.format(
						"Line %d of scans file [%s] is invalid:\n" +
						"----------\n%s\n----------\nThe second token [%s] " +
						"is expected to be an integer representing this " +
						"spectrum's scan number.", lineCount,
						scansFile.getName(), line, tokens[1]), error);
				}
				try {
					indices.add(Integer.parseInt(tokens[2]));
				} catch (NumberFormatException error) {
					throw new RuntimeException(String.format(
						"Line %d of scans file [%s] is invalid:\n" +
						"----------\n%s\n----------\nThe third token [%s] " +
						"is expected to be an integer representing this " +
						"spectrum's index within the source peak list file.",
						lineCount, scansFile.getName(), line, tokens[2]),
						error);
				}
			}
		} catch (Throwable error) {
			throw new RuntimeException(error);
		} finally {
			try { reader.close(); }
			catch (Throwable error) {}
		}
		if (scans.isEmpty() && indices.isEmpty())
			return null;
		else return new ImmutablePair<Collection<Integer>, Collection<Integer>>(
			scans, indices);
	}
	
	private static int[] validateMzTabFile(
		MzTabFile mzTabFile, TaskMzTabContext context,
		Map<String, ImmutablePair<Collection<Integer>, Collection<Integer>>>
		spectra, File uploadedResults,
		Map<String, Document> parsedMzidFileCache,
		Map<Document, Map<String, Map<String, Collection<String>>>>
		mzidSpectrumIDCache, boolean countOnly
	) throws Exception {
		if (mzTabFile == null)
			throw new NullPointerException("MzTab file is null.");
		else if (context == null)
			throw new NullPointerException(
				"MzTab file mapping context is null.");
		else if (spectra == null)
			throw new NullPointerException(
				"Peak list file spectra ID collection is null.");
		String mzTabFilename = mzTabFile.getMzTabFilename();
		// validate mzTab file's PSMs
		Collection<PSMRecord> uniquePSMs = new LinkedHashSet<PSMRecord>();
		Collection<String> foundPeptides = new LinkedHashSet<String>();
		Collection<String> foundProteins = new LinkedHashSet<String>();
		int psmRowCount = 0;
		int invalidRowCount = 0;
		int peptideRowCount = 0;
		int proteinRowCount = 0;
		BufferedReader reader = null;
		PrintWriter writer = null;
		File output = new File(TEMPORARY_MZTAB_FILE);
		try {
			reader = new BufferedReader(new FileReader(mzTabFile.getFile()));
			writer = new PrintWriter(new BufferedWriter(
				new FileWriter(output, false)));
			String line = null;
			int lineCount = 0;
			int sequenceIndex = -1;
			int accessionIndex = -1;
			int modsIndex = -1;
			// read PSM rows, ensure that all "spectra_ref" elements correspond
			// to spectra that were actually found in the peak list files
			int spectraRefIndex = -1;
			int validIndex = -1;
			int invalidReasonIndex = -1;
			while (true) {
				line = reader.readLine();
				if (line == null)
					break;
				lineCount++;
				// if this is the PSH row, then determine
				// the "spectra_ref" column index
				if (line.startsWith("PSH")) {
					String[] headers = line.split("\\t");
					int headerCount = headers.length;
					if (headers == null || headerCount < 1)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:\n" +
							"----------\n%s\n----------\nNo tab-delimited " +
							"column header elements were found.",
							lineCount, mzTabFilename, line));
					else for (int i=0; i<headerCount; i++) {
						String header = headers[i];
						if (header == null)
							continue;
						else if (header.equalsIgnoreCase("sequence"))
							sequenceIndex = i;
						else if (header.equalsIgnoreCase("accession"))
							accessionIndex = i;
						else if (header.equalsIgnoreCase("modifications"))
							modsIndex = i;
						else if (header.equalsIgnoreCase("spectra_ref"))
							spectraRefIndex = i;
						else if (header.equalsIgnoreCase("opt_global_valid"))
							validIndex = i;
						else if (header.equalsIgnoreCase(
							"opt_global_invalid_reason"))
							invalidReasonIndex = i;
					}
					if (sequenceIndex < 0)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:\n" +
							"----------\n%s\n----------\nNo \"sequence\" " +
							"column header element was found.",
							lineCount, mzTabFilename, line));
					else if (accessionIndex < 0)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:\n" +
							"----------\n%s\n----------\nNo \"accession\" " +
							"column header element was found.",
							lineCount, mzTabFilename, line));
					else if (modsIndex < 0)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:\n" +
							"----------\n%s\n----------\nNo " +
							"\"modifications\" column header element was " +
							"found.", lineCount, mzTabFilename, line));
					else if (spectraRefIndex < 0)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:\n" +
							"----------\n%s\n----------\nNo \"spectra_ref\" " +
							"column header element was found.",
							lineCount, mzTabFilename, line));
					// add extra validity optional columns, if necessary
					if (validIndex < 0) {
						line = line.trim() + "\topt_global_valid";
						validIndex = headerCount;
						headerCount++;
					}
					if (invalidReasonIndex < 0) {
						line = line.trim() + "\topt_global_invalid_reason";
						invalidReasonIndex = headerCount;
						headerCount++;
					}
					writer.println(line);
					continue;
				}
				// only validate PSM rows, but count PRT and PEP rows
				else if (line.startsWith("PSM") == false) {
					if (line.startsWith("PRT"))
						proteinRowCount++;
					else if (line.startsWith("PEP"))
						peptideRowCount++;
					writer.println(line);
					continue;
				}
				// ensure that a "spectra_ref" column index was found
				if (sequenceIndex < 0 || accessionIndex < 0 || modsIndex < 0 ||
					spectraRefIndex < 0 || validIndex < 0 ||
					invalidReasonIndex < 0)
					throw new IllegalArgumentException(String.format(
						"A \"PSM\" row (line %d) was found before the " +
						"\"PSH\" row in mzTab file [%s].",
						lineCount, mzTabFilename));
				else psmRowCount++;
				// validate this PSM row
				String[] columns = line.split("\\t");
				try {
					if (columns == null || columns.length < 1 ||
						columns.length <= sequenceIndex)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:\n" +
							"----------\n%s\n----------\nNo \"sequence\" " +
							"column element was found (expected at index %d).",
							lineCount, mzTabFilename, line, sequenceIndex));
					else if (columns.length <= accessionIndex)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:\n" +
							"----------\n%s\n----------\nNo \"accession\" " +
							"column element was found (expected at index %d).",
							lineCount, mzTabFilename, line, accessionIndex));
					else if (columns.length <= modsIndex)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:\n" +
							"----------\n%s\n----------\nNo " +
							"\"modifications\" column element was found " +
							"(expected at index %d).",
							lineCount, mzTabFilename, line, modsIndex));
					else if (columns.length <= spectraRefIndex)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:\n" +
							"----------\n%s\n----------\nNo \"spectra_ref\" " +
							"column element was found (expected at index %d).",
							lineCount, mzTabFilename, line, spectraRefIndex));
					// check existing validity status, if any
					if (validIndex < columns.length) {
						String validity = columns[validIndex];
						if (validity.equalsIgnoreCase("INVALID")) {
							invalidRowCount++;
							// ensure that a reason was provided
							if (invalidReasonIndex >= columns.length)
								line = line.trim() + "\tThis PSM was marked " +
									"as invalid by its source.";
							writer.println(line);
							continue;
						}
					}
					// parse the spectra_ref into file reference and nativeID;
					// each "spectra_ref" column value should
					// be a string with the following format:
					// ms_run[<index>]:<nativeID-formatted identifier string>
					String spectraRef = columns[spectraRefIndex];
					String[] tokens = spectraRef.split(":");
					if (tokens == null || tokens.length != 2)
						throw new InvalidPSMException(String.format(
							"Invalid \"spectra_ref\" column value [%s]: this " +
							"value is expected to conform to string format " +
							"[%s].", spectraRef, "ms_run[1-n]:" +
							"<nativeID-formatted identifier string>"));
					PSMRecord psm = validatePSMRow(columns[sequenceIndex],
						columns[modsIndex], tokens[0], tokens[1], context,
						spectra, lineCount, mzTabFile, uploadedResults,
						parsedMzidFileCache, mzidSpectrumIDCache, countOnly);
					uniquePSMs.add(psm);
					// add this row's peptide and protein to found lists,
					// if present and valid
					String foundPeptide = columns[sequenceIndex];
					if (foundPeptide != null &&
						foundPeptide.trim().equalsIgnoreCase("null") == false)
						foundPeptides.add(foundPeptide);
					String foundProtein = columns[accessionIndex];
					if (foundProtein != null &&
						foundProtein.trim().equalsIgnoreCase("null") == false)
						foundProteins.add(foundProtein);
					// check if the PSM row validation changed the nativeID;
					// if so, rewrite the line with the new nativeID
					if (tokens[1].equals(psm.getNativeID()) == false) {
						StringBuffer newLine = new StringBuffer();
						for (int i=0; i<columns.length; i++) {
							if (i == spectraRefIndex) {
								newLine.append(tokens[0]).append(":");
								newLine.append(psm.getNativeID());
							} else newLine.append(columns[i]);
							newLine.append("\t");
						}
						// chomp trailing tab character
						if (newLine.charAt(newLine.length() - 1) == '\t')
							newLine.setLength(newLine.length() - 1);
						line = newLine.toString();
					}
					// if we got this far, then the row is valid,
					// so mark it as such if it isn't already
					if (validIndex >= columns.length)
						line = line.trim() + "\tVALID";
					if (invalidReasonIndex >= columns.length)
						line = line.trim() + "\tnull";
					writer.println(line);
				} catch (InvalidPSMException error) {
					invalidRowCount++;
					// mark the row as invalid
					if (validIndex < columns.length) {
						columns[validIndex] = "INVALID";
						// reconstruct the line with the updated validity
						line = "";
						for (String value : columns)
							line += value + "\t";
						// chomp trailing comma
						if (line.endsWith("\t"))
							line = line.substring(0, line.length() - 1);
					} else line = line.trim() + "\tINVALID";
					columns = line.split("\\t");
					// provide the reason why this row is invalid
					if (invalidReasonIndex < columns.length) {
						columns[invalidReasonIndex] =
							getRootCause(error).getMessage();
						// reconstruct the line with the updated reason
						line = "";
						for (String value : columns)
							line += value + "\t";
						// chomp trailing comma
						if (line.endsWith("\t"))
							line = line.substring(0, line.length() - 1);
					} else line = line.trim() + "\t" +
						getRootCause(error).getMessage();
					writer.println(line);
				} catch (Throwable error) {
					throw error;
				}
			}
		} catch (Throwable error) {
			throw new RuntimeException(error);
		} finally {
			try { reader.close(); }
			catch (Throwable error) {}
			try { writer.close(); }
			catch (Throwable error) {}
		}
		// overwrite input mzTab file with updated temporary file,
		// if appropriate
		if (countOnly == false)
			FileIOUtils.copyFile(output, mzTabFile.getFile());
		if (output.delete() == false)
			throw new IOException(String.format(
				"Could not delete temporary mzTab file [%s]",
				output.getAbsolutePath()));
		return new int[] {
			psmRowCount, invalidRowCount, uniquePSMs.size(),
			peptideRowCount, foundPeptides.size(),
			proteinRowCount, foundProteins.size()
		};
	}
	
	private static PSMRecord validatePSMRow(
		String sequence, String modifications, String msRunID, String nativeID,
		TaskMzTabContext context, Map<String,
			ImmutablePair<Collection<Integer>, Collection<Integer>>> spectra,
		int lineNumber, MzTabFile mzTabFile, File uploadedResults,
		Map<String, Document> parsedMzidFileCache,
		Map<Document, Map<String, Map<String, Collection<String>>>>
		mzidSpectrumIDCache, boolean countOnly
	) throws InvalidPSMException {
		if (sequence == null)
			throw new NullPointerException("\"sequence\" string is null.");
		else if (modifications == null)
			throw new NullPointerException("\"modifications\" string is null.");
		else if (msRunID == null)
			throw new NullPointerException(
				"\"ms_run-location\" string is null.");
		else if (nativeID == null)
			throw new NullPointerException("\"nativeID\" string is null.");
		else if (context == null)
			throw new NullPointerException(
				"MzTab file mapping context is null.");
		else if (spectra == null)
			throw new NullPointerException(
				"Peak list file spectra ID collection is null.");
		else if (mzTabFile == null)
			throw new NullPointerException("MzTab file is null.");
		Matcher matcher = FILE_REFERENCE_PATTERN.matcher(msRunID);
		if (matcher.matches() == false)
			throw new InvalidPSMException(String.format(
				"Invalid \"ms_run\" reference [%s]: this value is expected " +
				"to conform to string format [%s].", msRunID, "ms_run[1-n]"));
		int msRunIndex = Integer.parseInt(matcher.group(1));;
		String verifiedNativeID = nativeID;
		if (countOnly == false) {
			MzTabMsRun msRun = mzTabFile.getMsRun(msRunIndex);
			String mangledPeakListFilename = msRun.getMangledPeakListFilename();
			if (msRun == null || mangledPeakListFilename == null)
				throw new InvalidPSMException(String.format(
					"Invalid \"ms_run\" reference [%s]: a file mapping " +
					"for \"ms_run\" index %d could not be resolved.",
					msRunID, msRunIndex));
			String scanFilename = String.format(
				"%s.scans", FilenameUtils.getBaseName(mangledPeakListFilename));
			ImmutablePair<Collection<Integer>, Collection<Integer>> scans =
				spectra.get(scanFilename);
			if (scans == null)
				throw new InvalidPSMException(String.format(
					"No spectra were found for \"ms_run\" reference " +
					"[%s], corresponding to peak list file [%s] (parsed " +
					"into spectra summary file [%s]).",
					msRunID, msRun.getPeakListFilename(), scanFilename));
			verifiedNativeID = validateSpectraRef(nativeID, scans, lineNumber,
				mzTabFile, CommonUtils.cleanFileURL(msRun.getMsRunLocation()),
				context, sequence, uploadedResults, parsedMzidFileCache,
				mzidSpectrumIDCache);
		}
		return new PSMRecord(
			msRunIndex, verifiedNativeID, sequence, modifications);
	}
	
	private static String validateSpectraRef(String nativeID,
		ImmutablePair<Collection<Integer>, Collection<Integer>> scans,
		int lineNumber, MzTabFile mzTabFile, String peakListFilename,
		TaskMzTabContext context, String sequence, File uploadedResults,
		Map<String, Document> parsedMzidFileCache,
		Map<Document, Map<String, Map<String, Collection<String>>>>
		mzidSpectrumIDCache
	) throws InvalidPSMException {
		if (nativeID == null)
			throw new NullPointerException(
				"NativeID-formatted identifier string is null.");
		else if (scans == null)
			throw new NullPointerException(
				"Spectra summary collection is null.");
		else if (mzTabFile == null)
			throw new NullPointerException("MzTab file is null.");
		else if (peakListFilename == null)
			throw new NullPointerException("Peak list filename is null.");
		else if (context == null)
			throw new NullPointerException(
				"MzTab file mapping context is null.");
		else if (sequence == null)
			throw new NullPointerException("\"sequence\" string is null.");
		else if (uploadedResults == null)
			throw new NullPointerException(
				"Uploaded result directory is null.");
		// extract the spectrum identifier from the nativeID string
		boolean scan = true;
		Integer value = null;
		// first try to extract a scan number
		Matcher matcher = MzTabConstants.SCAN_PATTERN.matcher(nativeID);
		if (matcher.find())
			value = Integer.parseInt(matcher.group(1));
		else {
			// then try to extract an index
			matcher = MzTabConstants.INDEX_PATTERN.matcher(nativeID);
			if (matcher.find()) {
				value = Integer.parseInt(matcher.group(1));
				scan = false;
			} else {
				// if it's a file specifier, and no index was also specified,
				// then assume that it's just a 1-spectrum file
				matcher = FILE_PATTERN.matcher(nativeID);
				if (matcher.find()) {
					value = Integer.parseInt(matcher.group(1));
					scan = false;
				}
				// if it's just an integer, we don't know if it's a scan or an
				// index, so look it up in the source mzid file, if there is one
				else try {
					value = Integer.parseInt(nativeID);
					if (isScan(mzTabFile, sequence, value, uploadedResults,
						context, parsedMzidFileCache, mzidSpectrumIDCache)
						== false) {
						nativeID = String.format("index=%d", value);
						scan = false;
					} else nativeID = String.format("scan=%d", value);
				} catch (NumberFormatException error) {}
			}
		}
		// if nothing was found, then the nativeID
		// string was of an unrecognized type
		if (value == null)
			throw new InvalidPSMException(String.format(
				"Invalid NativeID-formatted spectrum identifier [%s]: either " +
				"an index or a scan number must be provided in order to find " +
				"the referenced spectrum within the submitted peak list file.",
				nativeID));
		// once the type of ID has been determined,
		// check against the proper set
		Collection<Integer> ids = null;
		if (scan)
			ids = scans.getLeft();
		else ids = scans.getRight();
		boolean found = false;
		if (ids != null) {
			if (ids.contains(value))
				found = true;
			// apparently, indices might be 0-based or 1-based,
			// so we need to accept this ID if either is present
			else if (scan == false &&
				(ids.contains(value - 1) || ids.contains(value + 1)))
				found = true;
		}
		if (found == false)
			throw new InvalidPSMException(String.format(
				"Invalid NativeID-formatted spectrum identifier " +
				"[%s]: spectrum %s %d could not be found within " +
				"the submitted peak list file.",
				nativeID, scan ? "scan number" : "index", value));
		else return nativeID;
	}
	
	private static boolean isScan(
		MzTabFile mzTabFile, String sequence, int id,
		File uploadedResultDirectory, TaskMzTabContext context,
		Map<String, Document> parsedMzidFileCache,
		Map<Document, Map<String, Map<String, Collection<String>>>>
		mzidSpectrumIDCache
	) throws InvalidPSMException {
		if (mzTabFile == null)
			throw new NullPointerException("MzTab file is null.");
		else if (sequence == null)
			throw new NullPointerException("\"sequence\" string is null.");
		else if (context == null)
			throw new NullPointerException(
				"MzTab file mapping context is null.");
		else if (uploadedResultDirectory == null ||
			uploadedResultDirectory.canRead() == false)
			throw new InvalidPSMException(String.format(
				"Invalid NativeID-formatted spectrum identifier [%d]: no " +
				"submitted mzIdentML file could be found to verify whether " +
				"this identifier represents an index or scan number.", id));
		// get mangled result filename
		String mangledResultFilename = mzTabFile.getMangledResultFilename();
		// get original mzid file that this mzTab was converted from, if any
		File mzidFile = getUploadedMzIdentMLFile(
			mangledResultFilename, uploadedResultDirectory);
		// if this mzTab file was not converted from an mzid file, then there's
		// no source to look up; its nativeIDs are just inherently bad
		if (mzidFile == null || mzidFile.canRead() == false)
			throw new InvalidPSMException(String.format(
				"Invalid NativeID-formatted spectrum identifier [%d]: no " +
				"submitted mzIdentML file could be found to verify whether " +
				"this identifier represents an index or scan number.", id));
		// check cache for parsed mzid document, to avoid redundant parsing
		Document mzidDocument = null;
		if (parsedMzidFileCache != null)
			mzidDocument = parsedMzidFileCache.get(mzidFile.getName());
		// if no cached copy was found, parse the file
		if (mzidDocument == null) try {
			mzidDocument = FileIOUtils.parseXML(mzidFile);
			if (parsedMzidFileCache != null)
				parsedMzidFileCache.put(mzidFile.getName(), mzidDocument);
		} catch (Throwable error) {
			throw new InvalidPSMException(String.format(
				"Invalid NativeID-formatted spectrum identifier [%d]: " +
				"submitted mzIdentML file [%s] could not be parsed to verify " +
				"whether this identifier represents an index or scan number.",
				id, mzidFile.getName()));
		}
		// check cache for processed mzid document,
		// to avoid redundant XML processing
		Map<String, Map<String, Collection<String>>> mzidMap =
			mzidSpectrumIDCache.get(mzidDocument);
		if (mzidMap == null) {
			mzidMap =
				new LinkedHashMap<String, Map<String, Collection<String>>>();
			mzidSpectrumIDCache.put(mzidDocument, mzidMap);
		}
		// look for the nativeID in the processed map
		String nativeID = getNativeIDFromMap(sequence, id, mzidMap);
		// if not found in the map, look it up in the document
		if (nativeID == null) {
			boolean found = false;
			NodeList peptides = getPeptideNodes(mzidDocument, sequence);
			if (peptides == null)
				throw new InvalidPSMException(String.format(
					"Invalid NativeID-formatted spectrum identifier [%d]: " +
					"could not find an entry for peptide sequence [%s] in " +
					"submitted mzIdentML file [%s] to verify whether this " +
					"identifier represents an index or scan number.",
					id, sequence, mzidFile.getName()));
			for (int i=0; i<peptides.getLength(); i++) {
				Node peptide = peptides.item(i);
				NodeList spectrumIDs = null;
				String peptideRef = null;
				try {
					peptideRef = peptide.getAttributes()
						.getNamedItem("id").getNodeValue();
				} catch (Throwable error) {}
				spectrumIDs = getSpectrumIDNodes(mzidDocument, peptideRef);
				if (spectrumIDs == null || peptideRef == null)
					continue;
				for (int j=0; j<spectrumIDs.getLength(); j++) {
					Node spectrumID = spectrumIDs.item(j);
					try {
						nativeID = spectrumID.getAttributes()
							.getNamedItem("spectrumID").getNodeValue();
					} catch (Throwable error) {}
					if (nativeID == null)
						continue;
					else {
						// add found nativeID to cache
						addNativeIDToMap(
							sequence, peptideRef, nativeID, mzidMap);
//
System.out.print("?");
//
						// if this nativeID matches, stop looking
						if (nativeID.equals(String.format("scan=%d", id)) ||
							nativeID.equals(
								String.format("index=%d", id - 1))) {
							found = true;
							break;
						}
					}
				}
				if (found)
					break;
			}
		}
//
else System.out.print("!");
//
		// once all steps have been taken to find a matching nativeID,
		// process whatever was found appropriately
		if (nativeID == null)
			throw new InvalidPSMException(String.format(
				"Invalid NativeID-formatted spectrum identifier [%d]: " +
				"no evidence could be found in submitted mzIdentML file [%s] " +
				"to verify whether this identifier represents an index or " +
				"scan number.", id, mzidFile.getName()));
		else if (nativeID.equals(String.format("scan=%d", id)))
			return true;
		// need to decrement the ID printed in the mzTab file by
		// 1 when comparing against mzid index IDs, since
		// apparently jmzTab increments it during the conversion
		else if (nativeID.equals(String.format("index=%d", id - 1)))
			return false;
		else throw new InvalidPSMException(String.format(
			"Invalid NativeID-formatted spectrum identifier [%d]: " +
			"found spectrum ID [%s] in submitted mzIdentML file [%s], " +
			"but the NativeID format is not recognized.",
			id, nativeID, mzidFile.getName()));
	}
	
	@SuppressWarnings("unused")
	private static Map<String, Map<String, Collection<String>>> mapMzidDocument(
		Document mzidDocument
	) {
		if (mzidDocument == null)
			return null;
		// build map of sequence -> peptide_ref
		Map<String, Collection<String>> peptideMap =
			new LinkedHashMap<String, Collection<String>>();
		// build map of peptide_ref -> nativeID
		Map<String, Collection<String>> spectrumMap =
			new LinkedHashMap<String, Collection<String>>();
		// first look at peptide_refs and map them to their sequences
		NodeList peptides = null;
		try {
			peptides = XPathAPI.selectNodeList(mzidDocument, "//Peptide");
		} catch (Throwable error) {}
		if (peptides == null || peptides.getLength() < 1)
			return null;
		for (int i=0; i<peptides.getLength(); i++) {
			Node peptide = peptides.item(i);
			String peptideRef = null;
			try {
				peptideRef =
					peptide.getAttributes().getNamedItem("id").getNodeValue();
			} catch (Throwable error) {}
			if (peptideRef == null)
				continue;
			NodeList peptideSequences = null;
			try {
				peptideSequences =
					XPathAPI.selectNodeList(peptide, "PeptideSequence");
			} catch (Throwable error) {}
			if (peptideSequences == null || peptideSequences.getLength() < 1)
				continue;
			for (int j=0; j<peptideSequences.getLength(); j++) {
				Node peptideSequence = peptideSequences.item(j);
				String sequence = null;
				try {
					sequence = peptideSequence.getTextContent().trim();
				} catch (Throwable error) {}
				if (sequence == null)
					continue;
				Collection<String> peptideRefs = peptideMap.get(sequence);
				if (peptideRefs == null) {
					peptideRefs = new LinkedHashSet<String>();
					peptideMap.put(sequence, peptideRefs);
				}
				peptideRefs.add(peptideRef);
			}
		}
		// then look at spectrum IDs and map their nativeIDs to peptide_refs
		NodeList spectrumIDs = null;
		try {
			spectrumIDs = XPathAPI.selectNodeList(
				mzidDocument, "//SpectrumIdentificationResult");
		} catch (Throwable error) {}
		if (spectrumIDs == null || spectrumIDs.getLength() < 1)
			return null;
		for (int i=0; i<spectrumIDs.getLength(); i++) {
			Node spectrumID = spectrumIDs.item(i);
			String nativeID = null;
			try {
				nativeID = spectrumID.getAttributes()
					.getNamedItem("spectrumID").getNodeValue();
			} catch (Throwable error) {}
			if (nativeID == null)
				continue;
			NodeList spectrumIDItems = null;
			try {
				spectrumIDItems = XPathAPI.selectNodeList(
					spectrumID, "SpectrumIdentificationItem");
			} catch (Throwable error) {}
			if (spectrumIDItems == null || spectrumIDItems.getLength() < 1)
				continue;
			for (int j=0; j<spectrumIDItems.getLength(); j++) {
				Node spectrumIDItem = spectrumIDItems.item(j);
				String peptideRef = null;
				try {
					peptideRef = spectrumIDItem.getAttributes()
						.getNamedItem("peptide_ref").getNodeValue();
				} catch (Throwable error) {}
				if (peptideRef == null)
					continue;
				Collection<String> nativeIDs = spectrumMap.get(peptideRef);
				if (nativeIDs == null) {
					nativeIDs = new LinkedHashSet<String>();
					spectrumMap.put(peptideRef, nativeIDs);
				}
				nativeIDs.add(nativeID);
			}
		}
		// finally, combine the maps
		Map<String, Map<String, Collection<String>>> outerMap =
			new LinkedHashMap<String, Map<String, Collection<String>>>();
		for (String sequence : peptideMap.keySet()) {
			// make sure an inner map exists for this sequence
			Map<String, Collection<String>> innerMap = outerMap.get(sequence);
			if (innerMap == null) {
				innerMap = new LinkedHashMap<String, Collection<String>>();
				outerMap.put(sequence, innerMap);
			}
			// gather all peptide_refs for this sequence and
			// move their nativeIDs into the inner map
			Collection<String> peptideRefs = peptideMap.get(sequence);
			for (String peptideRef : peptideRefs) {
				Collection<String> nativeIDs = spectrumMap.get(peptideRef);
				if (nativeIDs == null)
					continue;
				// make sure an inner collection of nativeIDs
				// exists for this peptide_ref
				Collection<String> innerNativeIDs = innerMap.get(peptideRef);
				if (innerNativeIDs == null) {
					innerNativeIDs = new LinkedHashSet<String>();
					innerMap.put(peptideRef, innerNativeIDs);
				}
				innerNativeIDs.addAll(nativeIDs);
			}
		}
		return outerMap;
	}
	
	private static NodeList getPeptideNodes(
		Document mzidDocument, String sequence
	) {
		if (mzidDocument == null || sequence == null)
			return null;
		try {
			return XPathAPI.selectNodeList(mzidDocument, String.format(
				"//Peptide[PeptideSequence[text()='%s']]", sequence));
		} catch (Throwable error) {
			return null;
		}
//		CustomNodeList peptides = new CustomNodeList();
//		NodeList allPeptides = mzidDocument.getElementsByTagName("Peptide");
//		for (int i=0; i<allPeptides.getLength(); i++) {
//			Node peptide = allPeptides.item(i);
//			NodeList children = peptide.getChildNodes();
//			Node peptideSequence = null;
//			for (int j=0; j<children.getLength(); j++) {
//				Node child = children.item(j);
//				if (child.getNodeName().equalsIgnoreCase("PeptideSequence")) {
//					peptideSequence = child;
//					break;
//				}
//			}
//			if (peptideSequence == null)
//				continue;
//			else if (peptideSequence.getTextContent().trim().equals(sequence))
//				peptides.add(peptide);
//		}
//		return peptides;
	}
	
	private static NodeList getSpectrumIDNodes(
		Document mzidDocument, String peptideRef
	) {
		if (mzidDocument == null || peptideRef == null)
			return null;
		try {
			return XPathAPI.selectNodeList(mzidDocument,
				String.format("//SpectrumIdentificationResult[" +
				"SpectrumIdentificationItem[@peptide_ref='%s']]",
				peptideRef));
		} catch (Throwable error) {
			return null;
		}
//		CustomNodeList spectrumIDs = new CustomNodeList();
//		NodeList allSpectrumIDs =
//			mzidDocument.getElementsByTagName("SpectrumIdentificationResult");
//		for (int i=0; i<allSpectrumIDs.getLength(); i++) {
//			Node spectrumID = allSpectrumIDs.item(i);
//			NodeList children = spectrumID.getChildNodes();
//			for (int j=0; j<children.getLength(); j++) {
//				Node child = children.item(j);
//				if (child.getNodeName().equalsIgnoreCase(
//					"SpectrumIdentificationItem")) try {
//					if (child.getAttributes().getNamedItem("peptide_ref")
//						.getNodeValue().equals(peptideRef)) {
//						spectrumIDs.add(spectrumID);
//						break;
//					}
//				} catch (Throwable error) {
//					continue;
//				}
//			}
//		}
//		return spectrumIDs;
	}
	
	private static String getNativeIDFromMap(
		String sequence, int id,
		Map<String, Map<String, Collection<String>>> mzidMap
	) {
		if (sequence == null || mzidMap == null || mzidMap.isEmpty())
			return null;
		Map<String, Collection<String>> peptideRefs = mzidMap.get(sequence);
		if (peptideRefs == null || peptideRefs.isEmpty())
			return null;
		for (String peptideRef : peptideRefs.keySet()) {
			Collection<String> nativeIDs = peptideRefs.get(peptideRef);
			if (nativeIDs == null || nativeIDs.isEmpty())
				continue;
			for (String nativeID : nativeIDs)
				if (nativeID.equals(String.format("scan=%d", id)) ||
					nativeID.equals(String.format("index=%d", id - 1)))
					return nativeID;
		}
		return null;
	}
	
	private static void addNativeIDToMap(
		String sequence, String peptideRef, String nativeID,
		Map<String, Map<String, Collection<String>>> mzidMap
	) {
		if (sequence == null || peptideRef == null || nativeID == null ||
			mzidMap == null)
			return;
		Map<String, Collection<String>> peptideRefs = mzidMap.get(sequence);
		if (peptideRefs == null) {
			peptideRefs = new LinkedHashMap<String, Collection<String>>();
			mzidMap.put(sequence, peptideRefs);
		}
		Collection<String> nativeIDs = peptideRefs.get(peptideRef);
		if (nativeIDs == null) {
			nativeIDs = new LinkedHashSet<String>();
			peptideRefs.put(peptideRef, nativeIDs);
		}
		nativeIDs.add(nativeID);
	}
	
	private static File getUploadedMzIdentMLFile(
		String mzTabFilename, File uploadedResultDirectory
	) {
		if (mzTabFilename == null || uploadedResultDirectory == null ||
			uploadedResultDirectory.canRead() == false)
			return null;
		File mzidFile = new File(uploadedResultDirectory, String.format(
			"%s.%s", FilenameUtils.getBaseName(mzTabFilename), "mzid"));
		if (mzidFile.canRead() == false)
			return null;
		else return mzidFile;
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
	
	private static Throwable getRootCause(Throwable error) {
		if (error == null)
			return null;
		Throwable cause = error.getCause();
		if (cause == null)
			return error;
		else return getRootCause(cause);
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	@SuppressWarnings("unused")
	private static class CustomNodeList implements NodeList {
		/*====================================================================
		 * Properties
		 *====================================================================*/
		private List<Node> nodes;
		
		/*====================================================================
		 * Constructor
		 *====================================================================*/
		public CustomNodeList() {
			this(null);
		}
		
		public CustomNodeList(List<Node> nodes) {
			if (nodes == null)
				nodes = new ArrayList<Node>();
			this.nodes = nodes;
		}
		
		/*====================================================================
		 * NodeList methods
		 *====================================================================*/
		public Node item(int index) {
			if (index >= nodes.size())
				return null;
			else return nodes.get(index);
		}
		
		public int getLength() {
			return nodes.size();
		}
		
		/*====================================================================
		 * List methods
		 *====================================================================*/
		public void add(Node node) {
			nodes.add(node);
		}
	}
}
