package edu.ucsd.mztab.processors;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutablePair;

import edu.ucsd.mztab.model.MzTabConstants;
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
		MzTabConstants.PSH_PEPTIDE_COLUMN, MzTabConstants.PSH_PROTEIN_COLUMN
	};
	
	/*========================================================================
	 * Properties
	 *========================================================================*/
	// mzTab file properties
	private String               mzTabFilename;
	private MzTabSectionHeader   psmHeader;
	private Map<String, Integer> columns;
	// source file FDR properties
	private String               passThresholdColumn;
	private String               decoyColumn;
	private String               decoyPattern;
	private String               qValueColumn;
	// element type ("targetXYZ"/"decoyXYZ") -> count
	private Map<String, Integer>                         counts;
	// identifier -> passThreshold/isDecoy
	private Map<String, ImmutablePair<Boolean, Boolean>> peptides;
	private Map<String, ImmutablePair<Boolean, Boolean>> proteins;
	// protein accession -> matched peptides
	private Map<String, Set<String>>                     proteinPeptides;
	
	/*========================================================================
	 * Constructor
	 *========================================================================*/
	public FDRCalculationProcessor(
		Map<String, Integer> counts,
		Map<String, ImmutablePair<Boolean, Boolean>> peptides,
		Map<String, ImmutablePair<Boolean, Boolean>> proteins,
		String passThresholdColumn, String decoyColumn, String decoyPattern,
		String qValueColumn
	) {
		// initialize counter maps
		if (counts == null)
			throw new NullPointerException("Argument counts map is null.");
		else this.counts = counts;
		if (peptides == null)
			throw new NullPointerException(
				"Argument peptide FDR attributes map is null.");
		else this.peptides = peptides;
		if (proteins == null)
			throw new NullPointerException(
				"Argument protein FDR attributes map is null.");
		else this.proteins = proteins;
		proteinPeptides = new HashMap<String, Set<String>>();
		// initialize mzTab file properties
		psmHeader = null;
		mzTabFilename = null;
		// initialize column index map
		columns = new LinkedHashMap<String, Integer>(6);
		// initialize source columns
		this.passThresholdColumn = passThresholdColumn;
		this.decoyColumn = decoyColumn;
		this.decoyPattern = decoyPattern;
		this.qValueColumn = qValueColumn;
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
			// record all relevant column indices
			List<String> headers = psmHeader.getColumns();
			for (int i=0; i<headers.size(); i++) {
				String header = headers.get(i);
				if (header == null)
					continue;
				else if (CommonUtils.headerCorrespondsToColumn(
					header, passThresholdColumn))
					columns.put(passThresholdColumn, i);
				else if (CommonUtils.headerCorrespondsToColumn(
					header, decoyColumn))
					columns.put(decoyColumn, i);
				else if (CommonUtils.headerCorrespondsToColumn(
					header, qValueColumn))
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
				line = String.format( "%s\t%s", line.trim(),
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
						// but no decoy substring was provided, then we
						// can only interpret standard boolean values
						else isDecoy = CommonUtils.parseBooleanColumn(value);
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
			// increment the proper count for this PSM if it passes
			// threshold and its "isDecoy" value is not null.
			if (passThreshold && isDecoy != null) {
				if (isDecoy)
					incrementCount("decoyPSM");
				else incrementCount("targetPSM");
			}
			// add this PSM row's peptide sequence to the proper maps
			Integer peptideIndex =
				psmHeader.getColumnIndex(MzTabConstants.PSH_PEPTIDE_COLUMN);
			Integer proteinIndex =
				psmHeader.getColumnIndex(MzTabConstants.PSH_PROTEIN_COLUMN);
			String peptide = row[peptideIndex];
			addPeptide(peptide, passThreshold, isDecoy);
			addProteinPeptide(row[proteinIndex], peptide);
		}
		return line;
	}
	
	public void tearDown() {
		// ensure all element counts are present
		if (counts.get("decoyPSM") == null)
			counts.put("decoyPSM", 0);
		if (counts.get("targetPSM") == null)
			counts.put("targetPSM", 0);
		if (counts.get("decoyPeptide") == null)
			counts.put("decoyPeptide", 0);
		if (counts.get("targetPeptide") == null)
			counts.put("targetPeptide", 0);
		if (counts.get("decoyProtein") == null)
			counts.put("decoyProtein", 0);
		if (counts.get("targetProtein") == null)
			counts.put("targetProtein", 0);
		// calculate FDR attributes for all proteins
		for (String accession : proteinPeptides.keySet()) {
			// ensure this protein is counted by adding a default record for it
			addProtein(accession, false, null);
			// determine the protein's attributes from those of its peptides
			Boolean passThreshold = null;
			Boolean isDecoy = null;
			boolean nonDecoyPeptideFound = false;
			Set<String> sequences = proteinPeptides.get(accession);
			if (sequences != null) {
				for (String sequence : sequences) {
					ImmutablePair<Boolean, Boolean> attributes =
						peptides.get(sequence);
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
						// a protein is labeled decoy iff
						// ALL of its peptides are decoys
						Boolean peptideIsDecoy = attributes.getRight();
						// if this peptide's decoy status is null, then the
						// protein is not a decoy; it's either false or null
						if (peptideIsDecoy == null) {
							nonDecoyPeptideFound = true;
							// only leave the protein's decoy status null
							// if all peptides found so far have been null
							if (isDecoy != null)
								isDecoy = false;
						}
						// if this peptide is not a decoy,
						// then the protein isn't either
						else if (peptideIsDecoy == false) {
							nonDecoyPeptideFound = true;
							isDecoy = false;
						}
						// if this peptide is a decoy, then the protein
						// is only marked as a decoy if no other
						// non-decoy peptides have been found
						else if (nonDecoyPeptideFound)
							isDecoy = false;
						else isDecoy = true;
					}
				}
			}
			// ensure passThreshold is true by default if all peptides were null
			if (passThreshold == null)
				passThreshold = true;
			// update protein with determined attributes
			addProtein(accession, passThreshold, isDecoy);
		}
		// add peptide element counts
		for (String sequence : peptides.keySet()) {
			ImmutablePair<Boolean, Boolean> attributes = peptides.get(sequence);
			if (attributes != null) {
				// increment the proper count for this peptide if it passes
				// threshold and its "isDecoy" value is not null.
				if (attributes.getLeft() && attributes.getRight() != null) {
					if (attributes.getRight())
						incrementCount("decoyPeptide");
					else incrementCount("targetPeptide");
				}
			}
		}
		// add protein element counts
		for (String accession : proteins.keySet()) {
			ImmutablePair<Boolean, Boolean> attributes =
				proteins.get(accession);
			if (attributes != null) {
				// increment the proper count for this protein if it passes
				// threshold and its "isDecoy" value is not null.
				if (attributes.getLeft() && attributes.getRight() != null) {
					if (attributes.getRight())
						incrementCount("decoyProtein");
					else incrementCount("targetProtein");
				}
			}
		}
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private void incrementCount(String count) {
		if (count == null)
			return;
		Integer value = counts.get(count);
		if (value == null)
			value = 1;
		else value += 1;
		counts.put(count, value);
	}
	
	private void addPeptide(
		String sequence, Boolean passThreshold, Boolean isDecoy
	) {
		addFDRAttributes(true, sequence, passThreshold, isDecoy);
	}
	
	private void addProtein(
		String accession, Boolean passThreshold, Boolean isDecoy
	) {
		addFDRAttributes(false, accession, passThreshold, isDecoy);
	}
	
	private void addFDRAttributes(
		boolean type, String identifier, Boolean passThreshold, Boolean isDecoy
	) {
		if (identifier == null)
			return;
		Map<String, ImmutablePair<Boolean, Boolean>> map = null;
		if (type)
			map = peptides;
		else map = proteins;
		ImmutablePair<Boolean, Boolean> attributes = map.get(identifier);
		// if one of the saved attributes is already true, then keep it
		if (attributes != null) {
			Boolean savedPassThreshold = attributes.getLeft();
			Boolean savedIsDecoy = attributes.getRight();
			if (passThreshold == null ||
				(savedPassThreshold != null && savedPassThreshold == true))
				passThreshold = savedPassThreshold;
			if (isDecoy == null ||
				(savedIsDecoy != null && savedIsDecoy == true))
				isDecoy = savedIsDecoy;
		}
		// save these attributes for this identifier
		attributes =
			new ImmutablePair<Boolean, Boolean>(passThreshold, isDecoy);
		map.put(identifier, attributes);
	}
	
	private void addProteinPeptide(String accession, String sequence) {
		if (accession == null || sequence == null)
			return;
		Set<String> peptides = proteinPeptides.get(accession);
		if (peptides == null)
			peptides = new HashSet<String>();
		peptides.add(sequence);
		proteinPeptides.put(accession, peptides);
	}
	
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
