package edu.ucsd.mztab.ui;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import org.apache.commons.lang3.tuple.ImmutablePair;

import edu.ucsd.mztab.MzTabReader;
import edu.ucsd.mztab.model.MzTabConstants;
import edu.ucsd.mztab.model.MzTabFile;
import edu.ucsd.mztab.model.MzTabSectionHeader;
import edu.ucsd.mztab.model.MzTabConstants.MzTabSection;
import edu.ucsd.mztab.processors.FDRCalculationProcessor;
import edu.ucsd.util.CommonUtils;

public class MzTabFDRCleaner
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String USAGE =
		"java -cp MzTabUtils.jar edu.ucsd.mztab.ui.MzTabFDRCleaner" +
		"\n\t-mztab          <MzTabDirectory>" +
		"\n\t-output         <CleanedMzTabDirectory>" +
		"\n\t[-passThreshold <PassThresholdColumn>]" +
		"\n\t[-decoy         <IsDecoyColumn>]" +
		"\n\t[-decoyPattern  <SubstringIndicatingDecoy>]" +
		"\n\t[-qvalue        <QValueColumn>]" +
		"\n\t[-filter        true/false (default false; " +
			"if specified, PSM rows not meeting the FDR " +
			"threshold will be removed from the file)]" +
		"\n\t[-filterFDR     0-1 (if not specified, and filter=true, then " +
			"only PSMs marked as decoy or passThreshold=false will be removed]";
	private static final String[] RELEVANT_PSM_COLUMNS = new String[]{
		MzTabConstants.PASS_THRESHOLD_COLUMN,
		MzTabConstants.IS_DECOY_COLUMN,
		MzTabConstants.Q_VALUE_COLUMN
	};
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public static void main(String[] args) {
		MzTabFDRCleanupOperation cleanup = extractArguments(args);
		if (cleanup == null)
			die(USAGE);
		// read through all mzTab files, ensure that expected FDR
		// fields are present, and calculate whatever we can
		File[] files = cleanup.mzTabDirectory.listFiles();
		// sort files alphabetically
		Arrays.sort(files);
		for (File file : files) {
			// get this input mzTab file
			MzTabFile inputFile = new MzTabFile(file);
			// set up intermediate output file
			File tempFile = new File(String.format("%s.temp", file.getName()));
			// set up reader
			MzTabReader reader = new MzTabReader(inputFile, tempFile);
			// calculate FDR and ensure that each PSM row has the needed columns
			Map<String, Integer> counts = new HashMap<String, Integer>(6);
			Map<String, ImmutablePair<Boolean, Boolean>> peptides =
				new HashMap<String, ImmutablePair<Boolean, Boolean>>();
			Map<String, ImmutablePair<Boolean, Boolean>> proteins =
				new HashMap<String, ImmutablePair<Boolean, Boolean>>();
			reader.addProcessor(new FDRCalculationProcessor(
				counts, peptides, proteins, cleanup.passThresholdColumn,
				cleanup.decoyColumn, cleanup.decoyPattern,
				cleanup.qValueColumn));
			// clean file
			reader.read();
			// calculate global FDR values from returned count maps
			Double psmFDR = calculateFDR(
				counts.get("targetPSM"), counts.get("decoyPSM"));
			// peptide-level FDR
			Double peptideFDR = calculateFDR(
				counts.get("targetPeptide"), counts.get("decoyPeptide"));
			// protein-level FDR
			Double proteinFDR = calculateFDR(
				counts.get("targetProtein"), counts.get("decoyProtein"));
			// set up output file
			File outputFile = new File(cleanup.outputDirectory, file.getName());
			// add global FDR values to output file's metadata section
			doSecondFDRPass(tempFile, outputFile, inputFile.getMzTabFilename(),
				cleanup.filter, cleanup.filterFDR,
				psmFDR, peptideFDR, proteinFDR);
			// remove temporary file
			tempFile.delete();
		}
	}
	
	/*========================================================================
	 * Convenience classes
	 *========================================================================*/
	/**
	 * Struct to maintain context data for each mzTab FDR cleanup operation.
	 */
	private static class MzTabFDRCleanupOperation {
		/*====================================================================
		 * Properties
		 *====================================================================*/
		private File    mzTabDirectory;
		private File    outputDirectory;
		private String  passThresholdColumn;
		private String  decoyColumn;
		private String  decoyPattern;
		private String  qValueColumn;
		private boolean filter;
		private Double  filterFDR;
		
		/*====================================================================
		 * Constructors
		 *====================================================================*/
		public MzTabFDRCleanupOperation(
			File mzTabDirectory, File outputDirectory,
			String passThresholdColumn, String decoyColumn, String decoyPattern,
			String qValueColumn, boolean filter, Double filterFDR
		) {
			// validate mzTab directory
			if (mzTabDirectory == null)
				throw new NullPointerException(
					"MzTab directory cannot be null.");
			else if (mzTabDirectory.isDirectory() == false)
				throw new IllegalArgumentException(
					String.format("MzTab directory [%s] must be a directory.",
						mzTabDirectory.getAbsolutePath()));
			else if (mzTabDirectory.canRead() == false)
				throw new IllegalArgumentException(
					String.format("MzTab directory [%s] must be readable.",
						mzTabDirectory.getAbsolutePath()));
			else this.mzTabDirectory = mzTabDirectory;
			// validate cleaned mzTab output directory
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
			// initialize FDR columns (any or all may be null)
			this.passThresholdColumn = passThresholdColumn;
			this.decoyColumn = decoyColumn;
			this.decoyPattern = decoyPattern;
			this.qValueColumn = qValueColumn;
			// initialize filter settings
			this.filter = filter;
			this.filterFDR = filterFDR;
		}
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private static MzTabFDRCleanupOperation extractArguments(String[] args) {
		if (args == null || args.length < 1)
			return null;
		File mzTabDirectory = null;
		File outputDirectory = null;
		String passThresholdColumn = null;
		String decoyColumn = null;
		String decoyPattern = null;
		String qValueColumn = null;
		Boolean filter = false;
		Double filterFDR = null;
		for (int i=0; i<args.length; i++) {
			String argument = args[i];
			if (argument == null)
				return null;
			else {
				i++;
				if (i >= args.length)
					return null;
				String value = args[i];
				if (argument.equals("-mztab"))
					mzTabDirectory = new File(value);
				else if (argument.equals("-output"))
					outputDirectory = new File(value);
				else if (argument.equals("-passThreshold"))
					passThresholdColumn = value;
				else if (argument.equals("-decoy"))
					decoyColumn = value;
				else if (argument.equals("-decoyPattern"))
					decoyPattern = value;
				else if (argument.equals("-qvalue"))
					qValueColumn = value;
				else if (argument.equals("-filter")) {
					filter = CommonUtils.parseBooleanColumn(value);
					if (filter == null)
						throw new IllegalArgumentException(String.format(
							"Unrecognized value for \"-filter\": [%s]", value));
				} else if (argument.equals("-filterFDR")) try {
					filterFDR = Double.parseDouble(value);
					// enforce the FDR filter range of 0-1
					if (filterFDR < 0.0 || filterFDR > 1.0)
						throw new IllegalArgumentException("The argument to " +
							"\"-filterFDR\" must be a number in the range 0-1");
				} catch (Throwable error) {
					throw new IllegalArgumentException(String.format(
						"Illegal value for \"-filterFDR\": [%s]", value),
						error);
				} else return null;
			}
		}
		try {
			return new MzTabFDRCleanupOperation(
				mzTabDirectory, outputDirectory,
				passThresholdColumn, decoyColumn, decoyPattern, qValueColumn,
				filter, filterFDR);
		} catch (Throwable error) {
			die("There was an error reading command line parameters " +
				"to set up mzTab FDR cleanup operation.", error);
			return null;
		}
	}
	
	private static Double calculateFDR(Integer target, Integer decoy) {
		if (target == null || target == 0 || decoy == null)
			return null;
		else if (decoy == 0)
			return 0.0;
		else return (double)decoy / (double)target;
	}
	
	private static void doSecondFDRPass(
		File input, File output, String mzTabFilename,
		boolean filter, Double filterFDR,
		Double psmFDR, Double peptideFDR, Double proteinFDR
	) {
		if (input == null || output == null)
			return;
		BufferedReader reader = null;
		PrintWriter writer = null;
		try {
			reader = new BufferedReader(new FileReader(input));
			writer = new PrintWriter(
				new BufferedWriter(new FileWriter(output, false)));
			// determine if anything new needs to be written
			boolean doneWritingFDR = false;
			if (psmFDR == null && peptideFDR == null && proteinFDR == null)
				doneWritingFDR = true;
			// initialize PSH column variables
			MzTabSectionHeader psmHeader = null;
			Integer psmPassThresholdIndex = null;
			Integer psmIsDecoyIndex = null;
			Integer psmQValueIndex = null;
			// iterate over all lines in file, edit FDR line if
			// found or otherwise splice it in at the correct
			// location, and pass through all other lines
			String line = null;
			int lineNumber = 0;
			while (true) {
				line = reader.readLine();
				if (line == null)
					break;
				lineNumber++;
				if (line.startsWith("MTD") && doneWritingFDR == false) {
					// if this is the FDR line, replace it
					Matcher matcher =
						MzTabConstants.FDR_LINE_PATTERN.matcher(line);
					if (matcher.matches()) {
						// TODO: extract previous FDR values,
						// preserve them somehow
						String calculatedFDRList = getCalculatedFDRList(
							psmFDR, peptideFDR, proteinFDR);
						if (calculatedFDRList == null)
							calculatedFDRList = matcher.group(1);
						line = String.format("MTD\t%s\t%s",
							MzTabConstants.FDR_MTD_FIELD, calculatedFDRList);
						doneWritingFDR = true;
					}
					// otherwise, look for the correct place
					// to splice in the calculated FDR values
					else {
						String[] row = line.split("\\t");
						if (row != null && row.length > 1 &&
							isMTDFieldAfterFDR(row[1])) {
							// splice in the FDR line before the first
							// MTD field that should come after it
							String calculatedFDRList = getCalculatedFDRList(
								psmFDR, peptideFDR, proteinFDR);
							if (calculatedFDRList != null)
								writer.println(String.format("MTD\t%s\t%s",
									MzTabConstants.FDR_MTD_FIELD,
									calculatedFDRList));
							doneWritingFDR = true;
						}
					}
				}
				// set up PSM section header
				else if (line.startsWith("PSH")) {
					if (psmHeader != null)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:" +
							"\n----------\n%s\n----------\n" +
							"A \"PSH\" row was already seen previously in this file.",
							lineNumber, mzTabFilename, line));
					psmHeader = new MzTabSectionHeader(line);
					psmHeader.validateHeaderExpectations(
						MzTabSection.PSM, Arrays.asList(RELEVANT_PSM_COLUMNS));
					// record PSM Q-Value column index
					List<String> headers = psmHeader.getColumns();
					for (int i=0; i<headers.size(); i++) {
						String header = headers.get(i);
						if (header == null)
							continue;
						else if (header.equalsIgnoreCase(
							MzTabConstants.PASS_THRESHOLD_COLUMN)) {
							psmPassThresholdIndex = i;
						} else if (header.equalsIgnoreCase(
							MzTabConstants.IS_DECOY_COLUMN)) {
							psmIsDecoyIndex = i;
						} else if (header.equalsIgnoreCase(
							MzTabConstants.Q_VALUE_COLUMN)) {
							psmQValueIndex = i;
						}
					}
					// ensure that controlled FDR columns were all found
					if (psmPassThresholdIndex == null)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:" +
							"\n----------\n%s\n----------\n" +
							"No \"%s\" column was found.",
							lineNumber, mzTabFilename, line,
							MzTabConstants.PASS_THRESHOLD_COLUMN));
					else if (psmIsDecoyIndex == null)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:" +
							"\n----------\n%s\n----------\n" +
							"No \"%s\" column was found.",
							lineNumber, mzTabFilename, line,
							MzTabConstants.IS_DECOY_COLUMN));
					else if (psmQValueIndex == null)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:" +
							"\n----------\n%s\n----------\n" +
							"No \"%s\" column was found.",
							lineNumber, mzTabFilename, line,
							MzTabConstants.Q_VALUE_COLUMN));
				}
				// filter this PSM row if appropriate
				else if (line.startsWith("PSM") && filter) {
					if (psmHeader == null)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:" +
							"\n----------\n%s\n----------\n" +
							"A \"PSM\" row was found before any \"PSH\" row.",
							lineNumber, mzTabFilename, line));
					else psmHeader.validateMzTabRow(line);
					String[] row = line.split("\\t");
					// get passThreshold column (must be present for all rows)
					if (psmPassThresholdIndex >= row.length)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:" +
							"\n----------\n%s\n----------\n" +
							"Expected a \"%s\" column value at index %d, but " +
							"this line only contains %d elements.",
							lineNumber, mzTabFilename, line,
							MzTabConstants.PASS_THRESHOLD_COLUMN,
							psmPassThresholdIndex, row.length));
					// if passThreshold is false, then filter out this row
					Boolean passThreshold = CommonUtils.parseBooleanColumn(
						row[psmPassThresholdIndex]);
					if (passThreshold != null && passThreshold == false)
						continue;
					// get isDecoy column (must be present for all rows)
					if (psmIsDecoyIndex >= row.length)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:" +
							"\n----------\n%s\n----------\n" +
							"Expected a \"%s\" column value at index %d, but " +
							"this line only contains %d elements.",
							lineNumber, mzTabFilename, line,
							MzTabConstants.IS_DECOY_COLUMN,
							psmIsDecoyIndex, row.length));
					// if isDecoy is true, then filter out this row
					Boolean isDecoy = CommonUtils.parseBooleanColumn(
						row[psmIsDecoyIndex]);
					if (isDecoy != null && isDecoy == true)
						continue;
					// if a threshold filter FDR was specified,
					// determine if this row makes the cut
					if (filterFDR != null) {
						// get Q-value column (must be present for all rows)
						if (psmQValueIndex >= row.length)
							throw new IllegalArgumentException(String.format(
								"Line %d of mzTab file [%s] is invalid:" +
								"\n----------\n%s\n----------\n" +
								"Expected a \"%s\" column value at index %d, " +
								"but this line only contains %d elements.",
								lineNumber, mzTabFilename, line,
								MzTabConstants.Q_VALUE_COLUMN,
								psmQValueIndex, row.length));
						Double qValue = null;
						try {
							qValue = Double.parseDouble(row[psmQValueIndex]);
						} catch (NumberFormatException error) {}
						// if a non-null Q-value is present
						// for this row, then filter by that
						if (qValue != null) {
							if (qValue > filterFDR)
								continue;
						}
						// otherwise, the user asked for filtering by FDR
						// threshold, so check the calculated global FDR
						// since it's the only thing left to do
						else if (psmFDR == null || psmFDR > filterFDR)
							continue;
					}
				}
				writer.println(line);
			}
		} catch (RuntimeException error) {
			throw error;
		} catch (Throwable error) {
			throw new RuntimeException(error);
		} finally {
			try { reader.close(); } catch (Throwable error) {}
			try { writer.close(); } catch (Throwable error) {}
		}
	}
	
	private static String getCalculatedFDRList(
		Double psmFDR, Double peptideFDR, Double proteinFDR
	) {
		if (psmFDR == null && peptideFDR == null && proteinFDR == null)
			return null;
		StringBuilder fdr = new StringBuilder();
		if (psmFDR != null)
			fdr.append(String.format(MzTabConstants.GLOBAL_PSM_FDR_TERM,
				formatFDR(psmFDR))).append("|");
		if (peptideFDR != null)
			fdr.append(String.format(MzTabConstants.GLOBAL_PEPTIDE_FDR_TERM,
				formatFDR(peptideFDR))).append("|");
		if (proteinFDR != null)
			fdr.append(String.format(MzTabConstants.GLOBAL_PROTEIN_FDR_TERM,
				formatFDR(proteinFDR))).append("|");
		// chomp trailing pipe ("|")
		if (fdr.charAt(fdr.length() - 1) == '|')
			fdr.setLength(fdr.length() - 1);
		return fdr.toString();
	}
	
	private static String formatFDR(Double fdr) {
		if (fdr == null)
			return null;
		else return String.format("%.4f", fdr)
			.replaceFirst("(\\..?\\d*?)0+$", "$1");	// trim trailing zeros
	}
	
	private static boolean isMTDFieldAfterFDR(String field) {
		if (field == null)
			return false;
		// test against all fields that come before FDR, since there are fewer
		else if (field.equalsIgnoreCase("mzTab-version") ||
			field.equalsIgnoreCase("mzTab-mode") ||
			field.equalsIgnoreCase("mzTab-type") ||
			field.equalsIgnoreCase("mzTab-ID") ||
			field.equalsIgnoreCase("title") ||
			field.equalsIgnoreCase("description") ||
			field.toLowerCase().startsWith("sample_processing") ||
			field.toLowerCase().startsWith("instrument") ||
			field.toLowerCase().startsWith("software") ||
			field.toLowerCase().contains("search_engine_score"))
			return false;
		else return true;
	}
	
	private static void die(String message) {
		die(message, null);
	}
	
	private static void die(String message, Throwable error) {
		if (message == null)
			message = "There was an error cleaning up FDR compliance " +
				"within this set of mzTab files";
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
