package edu.ucsd.mztab.processors;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;

import org.apache.commons.lang3.tuple.ImmutablePair;

import edu.ucsd.mztab.model.MzTabConstants;
import edu.ucsd.mztab.model.MzTabConstants.FDRType;
import edu.ucsd.mztab.model.MzTabFDRStatistics;
import edu.ucsd.mztab.model.MzTabFile;
import edu.ucsd.mztab.model.MzTabProcessor;
import edu.ucsd.mztab.model.MzTabSectionHeader;
import edu.ucsd.mztab.model.MzTabConstants.MzTabSection;
import edu.ucsd.util.CommonUtils;

public class FDRCalculationProcessor implements MzTabProcessor
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String[] RELEVANT_PSM_COLUMNS = new String[]{
		MzTabConstants.PSH_PSM_ID_COLUMN, MzTabConstants.PSH_PEPTIDE_COLUMN,
		MzTabConstants.PSH_PROTEIN_COLUMN
	};
	
	/*========================================================================
	 * Properties
	 *========================================================================*/
	// mzTab file properties
	private String               mzTabFilename;
	private MzTabSectionHeader   psmHeader;
	private Map<String, Integer> columns;
	private Map<Integer, String> scoreColumns;
	// source file FDR properties
	private MzTabFDRStatistics   statistics;
	private String               passThresholdColumn;
	private String               decoyColumn;
	private String               decoyPattern;
	private String               qValueColumn;
	// FDR filter threshold
	private FDRType              filterType;
	private Double               filterFDR;
	
	/*========================================================================
	 * Constructor
	 *========================================================================*/
	public FDRCalculationProcessor(
		MzTabFDRStatistics statistics, String passThresholdColumn,
		String decoyColumn, String decoyPattern, String qValueColumn,
		FDRType filterType, Double filterFDR
	) {
		// initialize FDR statistics data structure
		if (statistics == null)
			throw new NullPointerException(
				"Argument FDR statistics object is null.");
		else this.statistics = statistics;
		// initialize mzTab file properties
		psmHeader = null;
		mzTabFilename = null;
		// initialize column index map
		columns = new HashMap<String, Integer>(7);
		// initialize score column map
		scoreColumns = new HashMap<Integer, String>();
		// initialize source columns
		this.passThresholdColumn = passThresholdColumn;
		this.decoyColumn = decoyColumn;
		this.decoyPattern = decoyPattern;
		this.qValueColumn = qValueColumn;
		// initialize FDR filter settings
		this.filterType = filterType;
		this.filterFDR = filterFDR;
	}
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public void setUp(MzTabFile mzTabFile) {
		if (mzTabFile == null)
			throw new NullPointerException("Argument mzTab file is null.");
		else mzTabFilename = mzTabFile.getMzTabFilename();
	}
	
	public String processMzTabLine(String line, int lineNumber) {
		if (line == null)
			throw new NullPointerException(
				"Processed mzTab line cannot be null.");
		// map score columns
		else if (line.startsWith("MTD")) {
			Matcher matcher =
				MzTabConstants.PSM_SEARCH_ENGINE_SCORE_LINE_PATTERN.matcher(
					line);
			if (matcher.matches()) {
				String score = matcher.group(1);
				int index;
				try { index = Integer.parseInt(score); }
				catch (NumberFormatException error) {
					throw new IllegalArgumentException(String.format(
						"Line %d of mzTab file [%s] is invalid:" +
						"\n----------\n%s\n----------\n" +
						"The index [%s] of this declared PSM search engine " +
						"score could not be parsed as an integer.",
						lineNumber, mzTabFilename, line, score));
				}
				String cvTerm = matcher.group(2);
				matcher = MzTabConstants.CV_TERM_PATTERN.matcher(cvTerm);
				if (matcher.matches() == false)
					throw new IllegalArgumentException(String.format(
						"Line %d of mzTab file [%s] is invalid:" +
						"\n----------\n%s\n----------\n" +
						"The value [%s] of this declared PSM search engine " +
						"score could not be parsed as a valid CV term.",
						lineNumber, mzTabFilename, line, cvTerm));
				// if both the index and name of this search engine
				// score could be determined, then map them
				scoreColumns.put(index, matcher.group(3));
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
			// get PSM section column headers
			List<String> headers = psmHeader.getColumns();
			// if the user did not specify a Q-value column, then
			// look for one that we know corresponds to Q-value
			if (qValueColumn == null) {
				for (String column : MzTabConstants.KNOWN_QVALUE_COLUMNS) {
					for (int i=0; i<headers.size(); i++) {
						String header = headers.get(i);
						if (header == null)
							continue;
						else if (CommonUtils.headerCorrespondsToColumn(
							header, column, scoreColumns)) {
							qValueColumn = header;
							break;
						}
					}
					if (qValueColumn != null)
						break;
				}
			}
			// record all relevant column indices
			for (int i=0; i<headers.size(); i++) {
				String header = headers.get(i);
				if (header == null)
					continue;
				else if (header.equalsIgnoreCase(
					MzTabConstants.PSH_PSM_ID_COLUMN))
					columns.put(MzTabConstants.PSH_PSM_ID_COLUMN, i);
				else if (CommonUtils.headerCorrespondsToColumn(
					header, passThresholdColumn, scoreColumns))
					columns.put(passThresholdColumn, i);
				else if (CommonUtils.headerCorrespondsToColumn(
					header, decoyColumn, scoreColumns))
					columns.put(decoyColumn, i);
				else if (CommonUtils.headerCorrespondsToColumn(
					header, qValueColumn, scoreColumns))
					columns.put(qValueColumn, i);
				else if (header.equalsIgnoreCase(
					MzTabConstants.PASS_THRESHOLD_COLUMN))
					columns.put(MzTabConstants.PASS_THRESHOLD_COLUMN, i);
				else if (header.equalsIgnoreCase(
					MzTabConstants.IS_DECOY_COLUMN))
					columns.put(MzTabConstants.IS_DECOY_COLUMN, i);
				else if (header.equalsIgnoreCase(MzTabConstants.Q_VALUE_COLUMN))
					columns.put(MzTabConstants.Q_VALUE_COLUMN, i);
			}
			// add CCMS-controlled FDR columns, if not already present
			if (columns.get(MzTabConstants.PASS_THRESHOLD_COLUMN) == null) {
				columns.put(
					MzTabConstants.PASS_THRESHOLD_COLUMN, headers.size());
				line = String.format("%s\t%s", line.trim(),
					MzTabConstants.PASS_THRESHOLD_COLUMN);
				headers.add(MzTabConstants.PASS_THRESHOLD_COLUMN);
			}
			if (columns.get(MzTabConstants.IS_DECOY_COLUMN) == null) {
				columns.put(MzTabConstants.IS_DECOY_COLUMN, headers.size());
				line = String.format("%s\t%s", line.trim(),
					MzTabConstants.IS_DECOY_COLUMN);
				headers.add(MzTabConstants.IS_DECOY_COLUMN);
			}
			if (columns.get(MzTabConstants.Q_VALUE_COLUMN) == null) {
				columns.put(MzTabConstants.Q_VALUE_COLUMN, headers.size());
				line = String.format("%s\t%s", line.trim(),
					MzTabConstants.Q_VALUE_COLUMN);
				headers.add(MzTabConstants.Q_VALUE_COLUMN);
			}
		}
		// record FDR attributes for this PSM row
		else if (line.startsWith("PSM")) {
			if (psmHeader == null)
				throw new IllegalArgumentException(String.format(
					"Line %d of mzTab file [%s] is invalid:" +
					"\n----------\n%s\n----------\n" +
					"A \"PSM\" row was found before any \"PSH\" row.",
					lineNumber, mzTabFilename, line));
			else psmHeader.validateMzTabRow(line);
			// determine controlled FDR values, if present
			String[] row = line.split("\\t");
			// get PSM_ID (should be present for all rows)
			String psmID = row[columns.get(MzTabConstants.PSH_PSM_ID_COLUMN)];
			// passThreshold; default true
			Boolean passThreshold = true;
			Integer passThresholdIndex =
				columns.get(MzTabConstants.PASS_THRESHOLD_COLUMN);
			// if the control column is already present, just read its value
			if (passThresholdIndex < row.length)
				passThreshold =
					CommonUtils.parseBooleanColumn(row[passThresholdIndex]);
			// add the control column if it's not already there
			else {
				String[] newRow = new String[passThresholdIndex + 1];
				for (int i=0; i<row.length; i++)
					newRow[i] = row[i];
				row = newRow;
			}
			// if the control column was not present, or if it is
			// present but its value could not be clearly interpreted
			// as either true or false, then try to determine the
			// correct value and write it to the control column
			if (passThreshold == null) {
				// if the source column is present, read it
				if (passThresholdColumn != null) {
					Integer index = columns.get(passThresholdColumn);
					if (index != null)
						passThreshold =
							CommonUtils.parseBooleanColumn(row[index]);
				}
				// enforce a default of true, even if something was present
				// but could not be interpreted as a valid boolean value
				if (passThreshold == null)
					passThreshold = true;
				row[passThresholdIndex] = Boolean.toString(passThreshold);
				line = getLine(row);
			}
			// isDecoy; default null
			Boolean isDecoy = null;
			Integer decoyIndex = columns.get(MzTabConstants.IS_DECOY_COLUMN);
			// if the control column is already present, just read its value
			if (decoyIndex < row.length)
				isDecoy = CommonUtils.parseBooleanColumn(row[decoyIndex]);
			// add the control column if it's not already there
			else {
				String[] newRow = new String[decoyIndex + 1];
				for (int i=0; i<row.length; i++)
					newRow[i] = row[i];
				row = newRow;
			}
			// if the control column was not present, or if it is
			// present but its value could not be clearly interpreted
			// as either true or false, then try to determine the
			// correct value and write it to the control column
			if (isDecoy == null) {
				// if the source column is present, read it
				if (decoyColumn != null) {
					Integer index = columns.get(decoyColumn);
					if (index != null) {
						String value = row[index];
						// if a decoy column was specified and is present,
						// and a decoy substring was provided, then the
						// row is a decoy hit iff the given column value
						// contains that substring
						if (decoyPattern != null) {
							if (value.contains(decoyPattern))
								isDecoy = true;
							else isDecoy = false;
						}
						// if a decoy column was specified and is present,
						// but no decoy substring was provided, then try
						// all known possible decoy substrings
						else {
							for (String knownDecoyPattern :
								MzTabConstants.KNOWN_DECOY_PATTERNS) {
								if (value.contains(knownDecoyPattern)) {
									isDecoy = true;
									break;
								}
							}
							// if no decoy substrings were found in this value,
							// interpret it as a standard boolean value
							if (isDecoy == null)
								isDecoy = CommonUtils.parseBooleanColumn(value);
						}
					}
				}
				// write isDecoy as 0/1 by convention
				row[decoyIndex] =
					isDecoy == null ? "null" : isDecoy ? "1" : "0";
				line = getLine(row);
			}
			// Q-value; default null
			String qValue = "null";
			Integer qValueIndex = columns.get(MzTabConstants.Q_VALUE_COLUMN);
			// if the control column is already present, just read its value
			if (qValueIndex < row.length)
				qValue = row[qValueIndex];
			// add the control column if it's not already there
			else {
				String[] newRow = new String[qValueIndex + 1];
				for (int i=0; i<row.length; i++)
					newRow[i] = row[i];
				row = newRow;
			}
			// if the control column was not present, or if it is
			// present but its value could not be clearly interpreted
			// as a valid Q-value, then try to determine the
			// correct value and write it to the control column
			if (qValue == null || qValue.trim().equalsIgnoreCase("null")) {
				// if the source column is present, read it
				if (qValueColumn != null) {
					Integer index = columns.get(qValueColumn);
					if (index != null)
						qValue = row[index];
				}
				row[qValueIndex] = qValue == null ? "null" : qValue;
				line = getLine(row);
			}
			// keep track of this PSM for FDR statistical
			// purposes only if it passes threshold
			if (passThreshold) {
				// if this PSM row is not a decoy, then note its Q-Value
				if (isDecoy == null || isDecoy == false) try {
					statistics.recordQValue(
						FDRType.PSM, Double.parseDouble(qValue),
						filterType, filterFDR);
				} catch (NumberFormatException error) {}
				// increment the proper count for this PSM if it passes
				// threshold and its "isDecoy" value is not null.
				if (isDecoy != null) {
					// a PSM is a target PSM if any of its rows
					// have isDecoy=false; any target PSM should
					// explicitly not be in the decoy set
					if (isDecoy == false) {
						statistics.addElement("targetPSM", psmID);
						statistics.removeElement("decoyPSM", psmID);
					}
					// a PSM should only be added to the deoy
					// set if it's not already in the target set
					else if (
						statistics.containsElement("targetPSM", psmID) == false)
						statistics.addElement("decoyPSM", psmID);
				}
			}
			// add this PSM row's peptide sequence to the proper maps
			Integer peptideIndex =
				psmHeader.getColumnIndex(MzTabConstants.PSH_PEPTIDE_COLUMN);
			Integer proteinIndex =
				psmHeader.getColumnIndex(MzTabConstants.PSH_PROTEIN_COLUMN);
			String peptide = row[peptideIndex];
			statistics.addPeptide(peptide, passThreshold, isDecoy);
			statistics.addProteinPeptide(row[proteinIndex], peptide);
		}
		return line;
	}
	
	public void tearDown() {
		// calculate FDR attributes for all proteins
		for (String accession : statistics.getPeptideMappedProteins()) {
			// ensure this protein is counted by adding a default record for it
			statistics.addProtein(accession, null, null);
			// determine the protein's attributes from those of its peptides
			Boolean passThreshold = null;
			Boolean isDecoy = null;
			boolean decoyPeptideFound = false;
			Set<String> sequences = statistics.getProteinPeptides(accession);
			if (sequences != null) {
				for (String sequence : sequences) {
					ImmutablePair<Boolean, Boolean> attributes =
						statistics.getPeptide(sequence);
					if (attributes != null) {
						// a protein passes threshold iff any one of
						// its peptides passes threshold; so only
						// bother to look at this peptide if no
						// passing peptides have been found so far
						if (passThreshold == null || passThreshold == false) {
							Boolean peptidePassThreshold = attributes.getLeft();
							// propagate either true or false; no false
							// will overwrite a true since this peptide
							// wouldn't even have been considered if a
							// passing peptide had been seen so far
							if (peptidePassThreshold != null)
								passThreshold = peptidePassThreshold;
						}
						// a protein is labeled target iff ALL of
						// its peptides are targets (or at least
						// one target with the rest null)
						Boolean peptideIsDecoy = attributes.getRight();
						if (peptideIsDecoy == null) {
							// if this peptide's decoy status is null,
							// then it has no effect on the overall
							// decoy status of the protein, so do nothing;
							// if isDecoy=null so far, then this doesn't
							// change it, and if it's true or false then
							// a null peptide doesn't change that either
						}
						// if this peptide is a decoy,
						// then the protein is as well
						else if (peptideIsDecoy == true) {
							decoyPeptideFound = true;
							isDecoy = true;
						}
						// if this peptide is a target, then the protein
						// is only marked as a target if no other
						// decoy peptides have been found
						else if (decoyPeptideFound) {
							isDecoy = true;
						} else isDecoy = false;
					}
				}
			}
			// ensure passThreshold is true by default if all peptides were null
			if (passThreshold == null)
				passThreshold = true;
			// update protein with determined attributes
			statistics.addProtein(accession, passThreshold, isDecoy);
		}
		// add peptide element counts
		for (String sequence : statistics.getPeptides()) {
			ImmutablePair<Boolean, Boolean> attributes =
				statistics.getPeptide(sequence);
			if (attributes != null) {
				// increment the proper count for this peptide if it passes
				// threshold and its "isDecoy" value is not null.
				if (attributes.getLeft() && attributes.getRight() != null) {
					if (attributes.getRight())
						statistics.addElement("decoyPeptide", sequence);
					else statistics.addElement("targetPeptide", sequence);
				}
			}
		}
		// add protein element counts
		for (String accession : statistics.getProteins()) {
			ImmutablePair<Boolean, Boolean> attributes =
				statistics.getProtein(accession);
			if (attributes != null) {
				// increment the proper count for this protein if it passes
				// threshold and its "isDecoy" value is not null.
				if (attributes.getLeft() && attributes.getRight() != null) {
					if (attributes.getRight())
						statistics.addElement("decoyProtein", accession);
					else statistics.addElement("targetProtein", accession);
				}
			}
		}
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private String getLine(String[] tokens) {
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
}
