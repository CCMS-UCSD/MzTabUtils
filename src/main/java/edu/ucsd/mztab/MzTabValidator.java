package edu.ucsd.mztab;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.ImmutablePair;

import edu.ucsd.util.CommonUtils;
import edu.ucsd.util.FileIOUtils;

public class MzTabValidator
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String USAGE =
		"java -cp MassIVEUtils.jar edu.ucsd.mztab.MzTabValidator" +
		"\n\t<ParameterFile>" +
		"\n\t<MzTabDirectory>" +
		"\n\t<ScansDirectory>" +
		"\n\t<OutputFile>";
	private static final String TEMPORARY_MZTAB_FILE =
		"temp_modified_result.mzTab";
	private static final Pattern FILE_REFERENCE_PATTERN =
		Pattern.compile("ms_run\\[(\\d+)\\]");
	private static final Pattern FILE_LINE_PATTERN =
		Pattern.compile("^MTD\\s+ms_run\\[(\\d+)\\]-location\\s+(.+)$");
	private static final Pattern SCAN_PATTERN = Pattern.compile("scan=(\\d+)");
	private static final Pattern INDEX_PATTERN =
		Pattern.compile("index=(\\d+)");
	private static final Pattern FILE_PATTERN = Pattern.compile("file=(.+)");
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public static void main(String[] args) {
		if (args == null || args.length < 4)
			die(USAGE);
		PrintWriter writer = null;
		try {
			// build mzTab file context
			MassIVEMzTabContext context = new MassIVEMzTabContext(
				new File(args[0]), new File(args[1]), new File(args[2]));
			// verify output file
			// validate output file
			File output = new File(args[3]);
			if (output.isDirectory())
				throw new IllegalArgumentException(
					String.format("Output file [%s] " +
						"must be a normal (non-directory) file.",
						output.getAbsolutePath()));
			// set up output file writer
			if (output.exists() == false && output.createNewFile() == false)
				die(String.format("Could not create output file [%s]",
					output.getAbsolutePath()));
			writer = new PrintWriter(
				new BufferedWriter(new FileWriter(output, false)));
			writer.println("MzTab_file\tUploaded_file\tPSM_rows\t" +
				"Invalid_PSM_rows\tFound_PSMs\tPeptide_rows\t" +
				"Found_Peptides\tProtein_rows\tFound_Proteins");
			// read all scans files
			Map<String, ImmutablePair<Collection<Integer>, Collection<Integer>>>
				scans = new LinkedHashMap<String,
					ImmutablePair<Collection<Integer>, Collection<Integer>>>();
			Collection<File> scansFiles = context.getScansFiles();
			if (scansFiles != null) {
				for (File scansFile : scansFiles) {
					ImmutablePair<Collection<Integer>, Collection<Integer>>
						spectra = readScansFile(scansFile);
					if (scansFile != null)
						scans.put(scansFile.getName(), spectra);
				}
			}
			// read all mzTab files, ensure that all referenced spectra
			// are present in the provided peak list files
			Collection<File> mzTabFiles = context.getMzTabFiles();
			if (mzTabFiles == null || mzTabFiles.isEmpty()) {
				System.out.println("No files were submitted in the " +
					"\"RESULT\" category for this dataset, so it will be " +
					"marked as an unsupported (i.e. partial) submission.");
			} else for (File mzTabFile : mzTabFiles) {
				// extract counts for PSMs, invalid PSMs, proteins and peptides
				int[] counts = validateMzTabFile(mzTabFile, context, scans);
				String mzTabFilename = mzTabFile.getName();
				String uploadedMzTabFilename =
					context.getUploadedMzTabFilename(mzTabFilename);
				if (counts == null || counts.length != 7)
					die(String.format(
						"MzTab file [%s] could not be parsed for validation.",
						uploadedMzTabFilename));
				// if this mzTab file has more than 10% invalid PSMs, it's bad
				int psmRows = counts[0];
				int invalidRows = counts[1];
				double percentage = (double)invalidRows / (double)psmRows * 100.0;
				if (percentage > 10)
					die(String.format("MzTab file [%s] contains %s%% invalid " +
						"PSM rows.  Please correct the file and ensure that " +
						"its referenced spectra are accessible within linked " +
						"peak list files, and then re-submit.",
						uploadedMzTabFilename, percentage));
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
				writer.println(String.format(
					"%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d",
					mzTabFilename, uploadedMzTabFilename, psmRows, invalidRows,
					foundPSMs, peptideRows, foundPeptides, proteinRows, foundProteins));
			}
		} catch (Throwable error) {
			die(getRootCause(error).getMessage());
		} finally {
			try { writer.close(); }
			catch (Throwable error) {}
		}
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
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
		File mzTabFile, MassIVEMzTabContext context,
		Map<String, ImmutablePair<Collection<Integer>, Collection<Integer>>>
		spectra
	) throws Exception {
		if (mzTabFile == null)
			throw new NullPointerException("MzTab file is null.");
		else if (mzTabFile.isFile() == false || mzTabFile.canRead() == false)
			throw new IllegalArgumentException(String.format(
				"Argument mzTab file [%s] is not a readable file.",
				mzTabFile.getName()));
		else if (context == null)
			throw new NullPointerException(
				"MzTab file mapping context is null.");
		else if (spectra == null)
			throw new NullPointerException(
				"Peak list file spectra ID collection is null.");
		String mzTabFilename =
			context.getUploadedMzTabFilename(mzTabFile.getName());
		// extract all peak list file references from mzTab file
		Map<Integer, String> peakListFiles = null;
		try {
			peakListFiles = extractMsRunFilenames(mzTabFile);
		} catch (Throwable error) {
			throw new RuntimeException(error);
		}
		if (peakListFiles == null || peakListFiles.isEmpty())
			throw new IllegalArgumentException(String.format(
				"No valid \"ms_run[1-n]-location\" lines were found " +
				"in mzTab file [%s].", mzTabFilename));
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
			reader = new BufferedReader(new FileReader(mzTabFile));
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
					uniquePSMs.add(validatePSMRow(columns[sequenceIndex],
						columns[modsIndex], columns[spectraRefIndex], context,
						spectra, peakListFiles, lineCount, mzTabFilename));
					foundPeptides.add(columns[sequenceIndex]);
					foundProteins.add(columns[accessionIndex]);
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
		// overwrite input mzTab file with updated temporary file
		FileIOUtils.copyFile(output, mzTabFile);
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
	
	private static Map<Integer, String> extractMsRunFilenames(File mzTabFile)
	throws Exception {
		if (mzTabFile == null)
			throw new NullPointerException("MzTab file is null.");
		else if (mzTabFile.isFile() == false || mzTabFile.canRead() == false)
			throw new IllegalArgumentException(String.format(
				"Argument mzTab file [%s] is not a readable file.",
				mzTabFile.getName()));
		// collect file references from the mzTab file's metadata section
		Map<Integer, String> peakListFiles =
			new LinkedHashMap<Integer, String>();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(mzTabFile));
			String line = null;
			while (true) {
				line = reader.readLine();
				if (line == null)
					break;
				// don't keep reading if we're past the metadata section
				else if (line.startsWith("MTD") == false &&
					line.startsWith("COM") == false &&
					line.trim().equals("") == false)
					break;
				// get spectrum file data, if this is a file location line
				Matcher matcher = FILE_LINE_PATTERN.matcher(line);
				if (matcher.matches())
					peakListFiles.put(
						Integer.parseInt(matcher.group(1)), matcher.group(2));
			}
		} catch (Throwable error) {
			throw new RuntimeException(error);
		} finally {
			try { reader.close(); }
			catch (Throwable error) {}
		}
		if (peakListFiles == null || peakListFiles.isEmpty())
			return null;
		else return peakListFiles;
	}
	
	private static PSMRecord validatePSMRow(
		String sequence, String modifications, String spectraRef,
		MassIVEMzTabContext context, Map<String,
			ImmutablePair<Collection<Integer>, Collection<Integer>>> spectra,
		Map<Integer, String> peakListFiles, int lineNumber, String mzTabFilename
	) throws InvalidPSMException {
		if (sequence == null)
			throw new NullPointerException("\"sequence\" string is null.");
		else if (modifications == null)
			throw new NullPointerException("\"modifications\" string is null.");
		else if (spectraRef == null)
			throw new NullPointerException("\"spectra_ref\" string is null.");
		else if (context == null)
			throw new NullPointerException(
				"MzTab file mapping context is null.");
		else if (spectra == null)
			throw new NullPointerException(
				"Peak list file spectra ID collection is null.");
		else if (peakListFiles == null)
			throw new NullPointerException(
				"\"ms_run[1-n]-location\" map is null.");
		else if (mzTabFilename == null)
			throw new NullPointerException("MzTab filename is null.");
		// each "spectra_ref" column value should
		// be a string with the following format:
		// ms_run[<index>]:<nativeID-formatted identifier string>
		String[] tokens = spectraRef.split(":");
		if (tokens == null || tokens.length != 2)
			throw new InvalidPSMException(String.format(
				"Invalid \"spectra_ref\" column value [%s]: this value " +
				"is expected to conform to string format [%s].", spectraRef,
				"ms_run[1-n]:<nativeID-formatted identifier string>"));
		Matcher matcher = FILE_REFERENCE_PATTERN.matcher(tokens[0]);
		if (matcher.matches() == false)
			throw new InvalidPSMException(String.format(
				"Invalid \"ms_run\" reference [%s]: this value is expected " +
				"to conform to string format [%s].", tokens[0], "ms_run[1-n]"));
		int msRun = Integer.parseInt(matcher.group(1));
		String msRunLocation = peakListFiles.get(msRun);
		if (msRunLocation == null)
			throw new InvalidPSMException(String.format(
				"Invalid \"ms_run\" reference [%s]: a file location for " +
				"\"ms_run\" index %d was not found in the metadata section " +
				"of this file.", tokens[0], msRun));
		String scanFilename =
			context.getScanFilename(mzTabFilename, msRunLocation);
		if (scanFilename == null)
			throw new InvalidPSMException(String.format(
				"\"ms_run\" reference [%s], corresponding to file " +
				"location [%s], could not be mapped back to any " +
				"submitted peak list file that was parsed for " +
				"validation against its spectra contents.",
				tokens[0], msRunLocation));
		ImmutablePair<Collection<Integer>, Collection<Integer>> scans =
			spectra.get(scanFilename);
		if (scans == null)
			throw new InvalidPSMException(String.format(
				"No spectra were found for \"ms_run\" reference " +
				"[%s], corresponding to file location [%s] (parsed " +
				"into spectra summary file [%s]).",
				tokens[0], msRunLocation, scanFilename));
		else validateSpectraRef(tokens[1], scans, lineNumber,
			mzTabFilename, CommonUtils.cleanFileURL(msRunLocation));
		return new PSMRecord(msRun, tokens[1], sequence, modifications);
	}
	
	private static void validateSpectraRef(String nativeID,
		ImmutablePair<Collection<Integer>, Collection<Integer>> scans,
		int lineNumber, String mzTabFilename, String peakListFilename
	) throws InvalidPSMException {
		if (nativeID == null)
			throw new NullPointerException(
				"NativeID-formatted identifier string is null.");
		else if (scans == null)
			throw new NullPointerException(
				"Spectra summary collection is null.");
		else if (mzTabFilename == null)
			throw new NullPointerException("MzTab filename is null.");
		else if (peakListFilename == null)
			throw new NullPointerException("Peak list filename is null.");
		// extract the spectrum identifier from the nativeID string
		boolean scan = true;
		Integer value = null;
		// first try to extract a scan number
		Matcher matcher = SCAN_PATTERN.matcher(nativeID);
		if (matcher.find())
			value = Integer.parseInt(matcher.group(1));
		else {
			// then try to extract an index
			matcher = INDEX_PATTERN.matcher(nativeID);
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
				// if it's just an integer, then by default
				// we process it as a spectrum index
				else try {
					value = Integer.parseInt(nativeID);
					scan = false;
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
		Collection<Integer> ids = null;
		if (scan)
			ids = scans.getLeft();
		else ids = scans.getRight();
		if (ids == null || ids.contains(value) == false)
			throw new InvalidPSMException(String.format(
				"Invalid NativeID-formatted spectrum identifier " +
				"[%s]: spectrum %s %d could not be found within " +
				"the submitted peak list file.",
				nativeID, scan ? "scan number" : "index", value));
	}
	
	private static void die(String message) {
		die(message, null);
	}
	
	private static void die(String message, Throwable error) {
		if (message == null)
			message = "There was an error validating " +
				"the mzTab files of a MassIVE dataset";
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
}
