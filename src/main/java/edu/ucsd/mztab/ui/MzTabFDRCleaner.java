package edu.ucsd.mztab.ui;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;

import edu.ucsd.mztab.MzTabReader;
import edu.ucsd.mztab.model.MzTabConstants;
import edu.ucsd.mztab.model.MzTabFDRStatistics;
import edu.ucsd.mztab.model.MzTabFile;
import edu.ucsd.mztab.model.MzTabSectionHeader;
import edu.ucsd.mztab.model.MzTabConstants.FDRType;
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
		"\n\t[-psmQValue     <PSMQValueColumn>]" +
		"\n\t[-peptideQValue <PSMQValueColumn>]" +
		"\n\t[-proteinQValue <PSMQValueColumn>]" +
		"\n\t[-filter        true/false (default false; " +
			"if specified, PSM rows not meeting the FDR " +
			"threshold will be removed from the file)]" +
		"\n\t[-filterType    psm/peptide/protein (default psm; " +
			"determines which Q-Value column to use when filtering, " +
			"if filter=true and filterFDR is specified)]" +
		"\n\t[-filterFDR     0-1 (if not specified, and filter=true, then " +
			"only PSMs marked as decoy or passThreshold=false will be removed]";
	private static final String[] RELEVANT_PSM_COLUMNS = new String[]{
		MzTabConstants.PSH_PEPTIDE_COLUMN,
		MzTabConstants.PSH_PROTEIN_COLUMN,
		MzTabConstants.PASS_THRESHOLD_COLUMN,
		MzTabConstants.IS_DECOY_COLUMN,
		MzTabConstants.Q_VALUE_COLUMN
	};
	private static final String[] RELEVANT_PRT_COLUMNS = new String[]{
		MzTabConstants.PRH_PROTEIN_COLUMN
	};
	private static final String[] RELEVANT_PEP_COLUMNS = new String[]{
		MzTabConstants.PEH_PEPTIDE_COLUMN
	};
	public static enum FDRFilterType { PSM, PEPTIDE, PROTEIN }
	
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
			// set up first intermediate output file
			File tempFile1 =
				new File(String.format("%s.1.temp", file.getName()));
			// set up reader
			MzTabReader reader = new MzTabReader(inputFile, tempFile1);
			// calculate FDR and ensure that each PSM row has the needed columns
			MzTabFDRStatistics statistics = new MzTabFDRStatistics();
			reader.addProcessor(new FDRCalculationProcessor(
				statistics, cleanup.passThresholdColumn,
				cleanup.decoyColumn, cleanup.decoyPattern,
				cleanup.psmQValueColumn));
			// clean file
			reader.read();
			// calculate global FDR values from returned count maps
			Double psmFDR = calculateFDR(
				statistics.getElementCount("targetPSM"),
				statistics.getElementCount("decoyPSM"));
			// if no FDR could be calculated, use highest found Q-Value, if any
			if (psmFDR == null)
				psmFDR = statistics.getMaxQValue(FDRType.PSM);
			// peptide-level FDR
			Double peptideFDR = calculateFDR(
				statistics.getElementCount("targetPeptide"),
				statistics.getElementCount("decoyPeptide"));
			if (peptideFDR == null)
				peptideFDR = statistics.getMaxQValue(FDRType.PEPTIDE);
			// protein-level FDR
			Double proteinFDR = calculateFDR(
				statistics.getElementCount("targetProtein"),
				statistics.getElementCount("decoyProtein"));
			if (proteinFDR == null)
				proteinFDR = statistics.getMaxQValue(FDRType.PROTEIN);
			// set up second intermediate output file
			File tempFile2 =
				new File(String.format("%s.2.temp", file.getName()));
			// add global FDR values to output file's metadata section,
			// filter out all PSM rows that do not meet the FDR cutoff,
			// and propagate calculated global FDR to any empty Q-Values
			doSecondFDRPass(tempFile1, tempFile2, inputFile.getMzTabFilename(),
				cleanup.filter, cleanup.filterType, cleanup.filterFDR,
				cleanup.peptideQValueColumn, cleanup.proteinQValueColumn,
				psmFDR, peptideFDR, proteinFDR, statistics);
			// set up final output file
			File outputFile = new File(cleanup.outputDirectory, file.getName());
			// filter out all protein and peptide rows no
			// longer supported by remaining PSM rows
			doThirdFDRPass(tempFile2, outputFile, inputFile.getMzTabFilename(),
				cleanup.filter, statistics);
			// remove temporary files
			tempFile1.delete();
			tempFile2.delete();
		}
	}
	
	public static Double calculateFDR(Integer target, Integer decoy) {
		if (target == null || target == 0 || decoy == null || decoy == 0)
			return null;
		// we can only calculate FDR if the count of
		// both targets and decoys is above zero
		else return (double)decoy / (double)target;
	}
	
	/**
	 * Steps of the second FDR pass:
	 * 
	 * 1. Write global FDR values calculated in step 1 to the metadata section
	 * of the mzTab file (if not already present).
	 * 
	 * 2. If filter=true, then filter out all PSM rows with passThreshold=false
	 * or isDecoy=true.
	 * 
	 * 3. For any PSM rows whose Q-Value is not known, propagate calculated
	 * global PSM-level FDR to that column.
	 * 
	 * 4. If filter=true and filterFDR is not null, then also filter out all
	 * rows whose Q-Value (of the type specified by filterType) is greater than
	 * the argument filterFDR. If any row has no such Q-Value, then filter it
	 * out if the calculated global FDR (of the type specified by filterType)
	 * is null or greater than the argument filterFDR.
	 * 
	 * 5. Remove from the argument peptides and proteins maps all elements
	 * that are no longer supported by any remaining PSM rows, after filtering,
	 * to set up the third pass.
	 * 
	 * @param input
	 * @param output
	 * @param mzTabFilename
	 * @param filter
	 * @param filterType
	 * @param filterFDR
	 * @param peptideQValueColumn
	 * @param proteinQValueColumn
	 * @param psmFDR
	 * @param peptideFDR
	 * @param proteinFDR
	 * @param peptides
	 * @param proteins
	 */
	public static void doSecondFDRPass(
		File input, File output, String mzTabFilename,
		boolean filter, FDRFilterType filterType, Double filterFDR,
		String peptideQValueColumn, String proteinQValueColumn,
		Double psmFDR, Double peptideFDR, Double proteinFDR,
		MzTabFDRStatistics statistics
	) {
		if (input == null || output == null)
			return;
		// keep track of all peptides and proteins to keep after filtering
		Set<String> keptPeptides = new HashSet<String>();
		Set<String> keptProteins = new HashSet<String>();
		// read through input mzTab file, update it for FDR
		// purposes, write updated rows to output file
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
			// initialize score column map
			Map<Integer, String> scoreColumns = new HashMap<Integer, String>();
			// initialize PSH column variables
			MzTabSectionHeader psmHeader = null;
			Integer sequenceIndex = null;
			Integer accessionIndex = null;
			Integer psmPassThresholdIndex = null;
			Integer psmIsDecoyIndex = null;
			Integer psmQValueIndex = null;
			Integer peptideQValueIndex = null;
			Integer proteinQValueIndex = null;
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
				if (line.startsWith("MTD")) {
					// write global FDR metadata line in the correct place
					if (doneWritingFDR == false) {
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
								MzTabConstants.FDR_MTD_FIELD,
								calculatedFDRList);
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
								String calculatedFDRList =
									getCalculatedFDRList(
										psmFDR, peptideFDR, proteinFDR);
								if (calculatedFDRList != null)
									writer.println(String.format("MTD\t%s\t%s",
										MzTabConstants.FDR_MTD_FIELD,
										calculatedFDRList));
								doneWritingFDR = true;
							}
							// while we're combing the MTD section,
							// map any score columns we find
							matcher = MzTabConstants
								.PSM_SEARCH_ENGINE_SCORE_LINE_PATTERN.matcher(
								line);
							if (matcher.matches()) {
								String score = matcher.group(1);
								int index;
								try { index = Integer.parseInt(score); }
								catch (NumberFormatException error) {
									throw new IllegalArgumentException(
										String.format("Line %d of mzTab " +
										"file [%s] is invalid:" +
										"\n----------\n%s\n----------\n" +
										"The index [%s] of this declared " +
										"PSM search engine score could not " +
										"be parsed as an integer.",
										lineNumber, mzTabFilename, line,
										score));
								}
								String cvTerm = matcher.group(2);
								matcher =
									MzTabConstants.CV_TERM_PATTERN.matcher(
										cvTerm);
								if (matcher.matches() == false)
									throw new IllegalArgumentException(
										String.format("Line %d of mzTab " +
										"file [%s] is invalid:" +
										"\n----------\n%s\n----------\n" +
										"The value [%s] of this declared " +
										"PSM search engine score could not " +
										"be parsed as a valid CV term.",
										lineNumber, mzTabFilename, line,
										cvTerm));
								// if both the index and name of this
								// search engine score could be
								// determined, then map them
								scoreColumns.put(index, matcher.group(3));
							}
						}
					}
				}
				// set up PSM section header
				else if (line.startsWith("PSH")) {
					if (psmHeader != null)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:" +
							"\n----------\n%s\n----------\n" +
							"A \"PSH\" row was already seen previously in " +
							"this file.", lineNumber, mzTabFilename, line));
					psmHeader = new MzTabSectionHeader(line);
					psmHeader.validateHeaderExpectations(
						MzTabSection.PSM, Arrays.asList(RELEVANT_PSM_COLUMNS));
					// record FDR-relevant column indices
					List<String> headers = psmHeader.getColumns();
					for (int i=0; i<headers.size(); i++) {
						String header = headers.get(i);
						if (header == null)
							continue;
						else if (header.equalsIgnoreCase(
							MzTabConstants.PSH_PEPTIDE_COLUMN))
							sequenceIndex = i;
						else if (header.equalsIgnoreCase(
							MzTabConstants.PSH_PROTEIN_COLUMN))
							accessionIndex = i;
						else if (header.equalsIgnoreCase(
							MzTabConstants.PASS_THRESHOLD_COLUMN))
							psmPassThresholdIndex = i;
						else if (header.equalsIgnoreCase(
							MzTabConstants.IS_DECOY_COLUMN))
							psmIsDecoyIndex = i;
						else if (header.equalsIgnoreCase(
							MzTabConstants.Q_VALUE_COLUMN))
							psmQValueIndex = i;
						else if (CommonUtils.headerCorrespondsToColumn(
							header, peptideQValueColumn, scoreColumns))
							peptideQValueIndex = i;
						else if (CommonUtils.headerCorrespondsToColumn(
							header, proteinQValueColumn, scoreColumns))
							proteinQValueIndex = i;
					}
					// ensure that controlled FDR columns were all found
					if (sequenceIndex == null)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:" +
							"\n----------\n%s\n----------\n" +
							"No \"%s\" column was found.",
							lineNumber, mzTabFilename, line,
							MzTabConstants.PSH_PEPTIDE_COLUMN));
					else if (accessionIndex == null)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:" +
							"\n----------\n%s\n----------\n" +
							"No \"%s\" column was found.",
							lineNumber, mzTabFilename, line,
							MzTabConstants.PSH_PROTEIN_COLUMN));
					else if (psmPassThresholdIndex == null)
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
							"Expected a \"%s\" column value at index %d, " +
							"but this line only contains %d elements.",
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
							"Expected a \"%s\" column value at index %d, " +
							"but this line only contains %d elements.",
							lineNumber, mzTabFilename, line,
							MzTabConstants.IS_DECOY_COLUMN,
							psmIsDecoyIndex, row.length));
					// if isDecoy is true, then filter out this row
					Boolean isDecoy = CommonUtils.parseBooleanColumn(
						row[psmIsDecoyIndex]);
					if (isDecoy != null && isDecoy == true)
						continue;
					// if this row has no PSM-level Q-Value, then propagate
					// calculated global PSM-level FDR to this column
					Double psmQValue = null;
					try {
						psmQValue = Double.parseDouble(row[psmQValueIndex]);
					} catch (NumberFormatException error) {
						if (psmFDR != null) {
							psmQValue = psmFDR;
							row[psmQValueIndex] = formatFDR(psmQValue);
							line = getLine(row);
						}
					}
					// if a threshold filter FDR was specified,
					// determine if this row makes the cut
					if (filterFDR != null) {
						// determine Q-Value column based
						// on selected FDR filter type
						Integer qValueIndex = null;
						String qValueColumnName = null;
						if (filterType == null ||
							filterType.equals(FDRFilterType.PSM)) {
							qValueIndex = psmQValueIndex;
							qValueColumnName = MzTabConstants.Q_VALUE_COLUMN;
						} else if (filterType.equals(FDRFilterType.PEPTIDE)) {
							qValueIndex = peptideQValueIndex;
							qValueColumnName = peptideQValueColumn;
						} else if (filterType.equals(FDRFilterType.PROTEIN)) {
							qValueIndex = proteinQValueIndex;
							qValueColumnName = proteinQValueColumn;
						}
						// get relevant Q-Value, if present
						Double qValue = null;
						if (qValueIndex != null) {
							// if Q-Value column is present in header but not
							// this row, then that's a bad mzTab line
							if (qValueIndex >= row.length)
								throw new IllegalArgumentException(
									String.format(
									"Line %d of mzTab file [%s] is invalid:" +
									"\n----------\n%s\n----------\n" +
									"Expected a \"%s\" column value at index " +
									"%d, but this line only contains %d " +
									"elements.", lineNumber, mzTabFilename,
									line, qValueColumnName, qValueIndex,
									row.length));
							else try {
								qValue = Double.parseDouble(row[qValueIndex]);
							} catch (NumberFormatException error) {}
						}
						// if a non-null Q-value is present
						// for this row, then filter by that
						if (qValue != null) {
							if (qValue > filterFDR)
								continue;
						}
						// otherwise, the user asked for filtering by FDR
						// threshold, so check the calculated global FDR
						// since it's the only thing left to do
						else {
							// determine which global FDR value to use
							// based on selected FDR filter type
							Double globalFDR = null;
							if (filterType == null ||
								filterType.equals(FDRFilterType.PSM))
								globalFDR = psmFDR;
							else if (filterType.equals(FDRFilterType.PEPTIDE))
								globalFDR = peptideFDR;
							else if (filterType.equals(FDRFilterType.PROTEIN))
								globalFDR = proteinFDR;
							// if the global FDR of the appropriate type is
							// null or greater than the user-specified cutoff,
							// then this row must be cut (along with presumably
							// all other rows, since if there is no Q-Value of
							// the appropriate type for this row then there
							// probably isn't one for any other row)
							if (globalFDR == null || globalFDR > filterFDR)
								continue;
						}
					}
					// if we got this far, then the PSM row passed filtering
					// and therefore we should note its peptide and protein
					// as ones that should be kept in the final file
					keptPeptides.add(row[sequenceIndex]);
					keptProteins.add(row[accessionIndex]);
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
		// remove from the parent peptides and proteins maps all elements that
		// are no longer supported by PSM evidence in the file after filtering
		if (filter) {
			// clean peptides
			Set<String> originalPeptides = null;
			if (statistics != null)
				originalPeptides =
					new HashSet<String>(statistics.getPeptides());
			if (originalPeptides != null) {
				for (String peptide : originalPeptides)
					if (keptPeptides.contains(peptide) == false)
						statistics.removePeptide(peptide);
			}
			// clean proteins
			Set<String> originalProteins = null;
			if (statistics != null)
				originalProteins =
					new HashSet<String>(statistics.getProteins());
			if (originalProteins != null) {
				for (String protein : originalProteins)
					if (keptProteins.contains(protein) == false)
						statistics.removeProtein(protein);
			}
		}
	}
	
	/**
	 * Steps of the third FDR pass:
	 * 
	 * 1. Filter out all PRT and PEP rows not supported by any remaining PSM
	 * 
	 * @param input
	 * @param output
	 * @param mzTabFilename
	 * @param filter
	 * @param peptides
	 * @param proteins
	 */
	public static void doThirdFDRPass(
		File input, File output, String mzTabFilename, boolean filter,
		MzTabFDRStatistics statistics
	) {
		if (input == null || output == null)
			return;
		// read through input mzTab file, filter out unsupported PRT/PEP rows
		BufferedReader reader = null;
		PrintWriter writer = null;
		try {
			reader = new BufferedReader(new FileReader(input));
			writer = new PrintWriter(
				new BufferedWriter(new FileWriter(output, false)));
			// initialize protein section header variables
			MzTabSectionHeader prtHeader = null;
			Integer accessionIndex = null;
			// initialize peptide section header variables
			MzTabSectionHeader pepHeader = null;
			Integer sequenceIndex = null;
			// iterate over all lines in file, filter out any PRT or PEP line
			// if it is no longer found in the peptides or proteins maps
			String line = null;
			int lineNumber = 0;
			while (true) {
				line = reader.readLine();
				if (line == null)
					break;
				lineNumber++;
				// set up protein header
				if (line.startsWith("PRH")) {
					if (prtHeader != null)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:" +
							"\n----------\n%s\n----------\n" +
							"A \"PRH\" row was already seen previously in " +
							"this file.", lineNumber, mzTabFilename, line));
					prtHeader = new MzTabSectionHeader(line);
					prtHeader.validateHeaderExpectations(
						MzTabSection.PRT, Arrays.asList(RELEVANT_PRT_COLUMNS));
					// record accession column index
					List<String> headers = prtHeader.getColumns();
					for (int i=0; i<headers.size(); i++) {
						String header = headers.get(i);
						if (header == null)
							continue;
						else if (header.equalsIgnoreCase(
							MzTabConstants.PRH_PROTEIN_COLUMN)) {
							accessionIndex = i;
							break;
						}
					}
					// ensure that accession index was found
					if (accessionIndex == null)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:" +
							"\n----------\n%s\n----------\n" +
							"No \"%s\" column was found.",
							lineNumber, mzTabFilename, line,
							MzTabConstants.PRH_PROTEIN_COLUMN));
				}
				// filter this PRT row if appropriate
				else if (line.startsWith("PRT") && filter &&
					statistics != null) {
					if (prtHeader == null)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:" +
							"\n----------\n%s\n----------\n" +
							"A \"PRT\" row was found before any \"PRH\" row.",
							lineNumber, mzTabFilename, line));
					else prtHeader.validateMzTabRow(line);
					String[] row = line.split("\\t");
					// get accession column value (must be present for all rows)
					if (accessionIndex >= row.length)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:" +
							"\n----------\n%s\n----------\n" +
							"Expected a \"%s\" column value at index %d, " +
							"but this line only contains %d elements.",
							lineNumber, mzTabFilename, line,
							MzTabConstants.PRH_PROTEIN_COLUMN,
							accessionIndex, row.length));
					// if this protein's accession is not found in
					// the proteins map, then filter out this row
					String accession = row[accessionIndex];
					if (accession != null &&
						statistics.containsProtein(accession) == false)
						continue;
				}
				// set up peptide header
				else if (line.startsWith("PEH")) {
					if (pepHeader != null)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:" +
							"\n----------\n%s\n----------\n" +
							"A \"PEH\" row was already seen previously in " +
							"this file.", lineNumber, mzTabFilename, line));
					pepHeader = new MzTabSectionHeader(line);
					pepHeader.validateHeaderExpectations(
						MzTabSection.PEP, Arrays.asList(RELEVANT_PEP_COLUMNS));
					// record sequence column index
					List<String> headers = pepHeader.getColumns();
					for (int i=0; i<headers.size(); i++) {
						String header = headers.get(i);
						if (header == null)
							continue;
						else if (header.equalsIgnoreCase(
							MzTabConstants.PEH_PEPTIDE_COLUMN)) {
							sequenceIndex = i;
							break;
						}
					}
					// ensure that sequence index was found
					if (sequenceIndex == null)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:" +
							"\n----------\n%s\n----------\n" +
							"No \"%s\" column was found.",
							lineNumber, mzTabFilename, line,
							MzTabConstants.PEH_PEPTIDE_COLUMN));
				}
				// filter this PEP row if appropriate
				else if (line.startsWith("PEP") && filter &&
					statistics != null) {
					if (pepHeader == null)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:" +
							"\n----------\n%s\n----------\n" +
							"A \"PEP\" row was found before any \"PEH\" row.",
							lineNumber, mzTabFilename, line));
					else pepHeader.validateMzTabRow(line);
					String[] row = line.split("\\t");
					// get sequence column value (must be present for all rows)
					if (sequenceIndex >= row.length)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:" +
							"\n----------\n%s\n----------\n" +
							"Expected a \"%s\" column value at index %d, " +
							"but this line only contains %d elements.",
							lineNumber, mzTabFilename, line,
							MzTabConstants.PEH_PEPTIDE_COLUMN,
							sequenceIndex, row.length));
					// if this peptide's sequence is not found in
					// the peptides map, then filter out this row
					String sequence = row[sequenceIndex];
					if (sequence != null &&
						statistics.containsPeptide(sequence) == false)
						continue;
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
		private File           mzTabDirectory;
		private File           outputDirectory;
		private String         passThresholdColumn;
		private String         decoyColumn;
		private String         decoyPattern;
		private String         psmQValueColumn;
		private String         peptideQValueColumn;
		private String         proteinQValueColumn;
		private boolean        filter;
		private FDRFilterType  filterType;
		private Double         filterFDR;
		
		/*====================================================================
		 * Constructors
		 *====================================================================*/
		public MzTabFDRCleanupOperation(
			File mzTabDirectory, File outputDirectory,
			String passThresholdColumn, String decoyColumn, String decoyPattern,
			String psmQValueColumn, String peptideQValueColumn,
			String proteinQValueColumn,
			boolean filter, String filterType, Double filterFDR
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
			this.psmQValueColumn = psmQValueColumn;
			this.peptideQValueColumn = peptideQValueColumn;
			this.proteinQValueColumn = proteinQValueColumn;
			// initialize filter settings
			this.filter = filter;
			this.filterFDR = filterFDR;
			// validate filter type
			if (filterType == null || filterType.trim().equals(""))
				this.filterType = null;
			else try {
				this.filterType =
					FDRFilterType.valueOf(filterType.trim().toUpperCase());
			} catch (Throwable error) {
				throw new IllegalArgumentException(
					String.format("Unrecognized filter type [%s]: must be  " +
						"\"psm\", \"peptide\" or \"protein\".", filterType));
			}
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
		String psmQValueColumn = null;
		String peptideQValueColumn = null;
		String proteinQValueColumn = null;
		String filterType = null;
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
				else if (argument.equals("-psmQValue"))
					psmQValueColumn = value;
				else if (argument.equals("-peptideQValue"))
					peptideQValueColumn = value;
				else if (argument.equals("-proteinQValue"))
					proteinQValueColumn = value;
				else if (argument.equals("-filterType"))
					filterType = value;
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
				passThresholdColumn, decoyColumn, decoyPattern,
				psmQValueColumn, peptideQValueColumn, proteinQValueColumn,
				filter, filterType, filterFDR);
		} catch (Throwable error) {
			die("There was an error reading command line parameters " +
				"to set up mzTab FDR cleanup operation.", error);
			return null;
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
		else return Double.toString(fdr);
//		else return String.format("%.4f", fdr)
//			.replaceFirst("(\\..?\\d*?)0+$", "$1");	// trim trailing zeros
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
	
	private static String getLine(String[] tokens) {
		if (tokens == null || tokens.length < 1)
			return null;
		StringBuilder line = new StringBuilder();
		for (int i=0; i<tokens.length; i++)
			line.append(tokens[i]).append("\t");
		// chomp trailing tab ("\t")
		if (line.charAt(line.length() - 1) == '\t')
			line.setLength(line.length() - 1);
		return line.toString();
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
