package edu.ucsd.mztab.processors;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;

import edu.ucsd.mztab.model.MzTabFile;
import edu.ucsd.mztab.model.MzTabProcessor;
import edu.ucsd.mztab.model.MzTabSectionHeader;
import edu.ucsd.mztab.model.MzTabConstants.MzTabSection;

public class FDRPropagationProcessor implements MzTabProcessor
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String[] RELEVANT_PSM_COLUMNS = new String[]{
		FDRCalculationProcessor.PASS_THRESHOLD_COLUMN,
		FDRCalculationProcessor.IS_DECOY_COLUMN,
		FDRCalculationProcessor.Q_VALUE_COLUMN
	};
	
	/*========================================================================
	 * Properties
	 *========================================================================*/
	// mzTab file properties
	private String               mzTabFilename;
	private MzTabSectionHeader   psmHeader;
	private Integer              psmQValueIndex;
	// identifier -> passThreshold/isDecoy
	private Map<String, ImmutablePair<Boolean, Boolean>> peptides;
	private Map<String, ImmutablePair<Boolean, Boolean>> proteins;
	// global FDR values
	private String psmFDR;
	private String peptideFDR;
	private String proteinFDR;
	
	/*========================================================================
	 * Constructor
	 *========================================================================*/
	public FDRPropagationProcessor(
		Map<String, Integer> counts,
		Map<String, ImmutablePair<Boolean, Boolean>> peptides,
		Map<String, ImmutablePair<Boolean, Boolean>> proteins
	) {
		// initialize counter maps
		if (peptides == null)
			throw new NullPointerException(
				"Argument peptide FDR attributes map is null.");
		else this.peptides = peptides;
		if (proteins == null)
			throw new NullPointerException(
				"Argument protein FDR attributes map is null.");
		else this.proteins = proteins;
		// calculate PSM-level FDR value
		if (counts == null)
			throw new NullPointerException("Argument counts map is null.");
		Integer target = counts.get("targetPSM");
		Integer decoy = counts.get("decoyPSM");
		if (target == null || target == 0 || decoy == null)
			psmFDR = "null";
		else if (decoy == 0)
			psmFDR = "0.0";
		else psmFDR = String.format("%.2f", (double)decoy / (double)target);
		// calculate peptide-level FDR value
		target = counts.get("targetPeptide");
		decoy = counts.get("decoyPeptide");
		if (target == null || target == 0 || decoy == null)
			peptideFDR = "null";
		else if (decoy == 0)
			peptideFDR = "0.0";
		else peptideFDR = String.format("%.2f", (double)decoy / (double)target);
		// calculate protein-level FDR value
		target = counts.get("targetProtein");
		decoy = counts.get("decoyProtein");
		if (target == null || target == 0 || decoy == null)
			proteinFDR = "null";
		else if (decoy == 0)
			proteinFDR = "0.0";
		else proteinFDR = String.format("%.2f", (double)decoy / (double)target);
		// initialize mzTab file properties
		psmHeader = null;
		mzTabFilename = null;
		psmQValueIndex = null;
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
		// TODO: propagate protein FDR
		else if (line.startsWith("PRH")) {
			
		} else if (line.startsWith("PRT")) {
			
		}
		// TODO: propagate peptide FDR
		else if (line.startsWith("PEH")) {
			
		} else if (line.startsWith("PEP")) {
			
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
					FDRCalculationProcessor.Q_VALUE_COLUMN)) {
					psmQValueIndex = i;
					break;
				}
			}
			// ensure that a PSM Q-Value column was found
			if (psmQValueIndex == null)
				throw new IllegalArgumentException(String.format(
					"Line %d of mzTab file [%s] is invalid:" +
					"\n----------\n%s\n----------\n" +
					"No \"%s\" column was found.",
					lineNumber, mzTabFilename, line,
					FDRCalculationProcessor.Q_VALUE_COLUMN));
		}
		// write global PSM-level FDR to this row
		else if (line.startsWith("PSM")) {
			if (psmHeader == null)
				throw new IllegalArgumentException(String.format(
					"Line %d of mzTab file [%s] is invalid:" +
					"\n----------\n%s\n----------\n" +
					"A \"PSM\" row was found before any \"PSH\" row.",
					lineNumber, mzTabFilename, line));
			else psmHeader.validateMzTabRow(line);
			// get Q-value column (must be present for all rows)
			String[] row = line.split("\\t");
			if (psmQValueIndex >= row.length)
				throw new IllegalArgumentException(String.format(
					"Line %d of mzTab file [%s] is invalid:" +
					"\n----------\n%s\n----------\n" +
					"Expected a \"%s\" column value at index %d, but " +
					"this line only contains %d elements.",
					lineNumber, mzTabFilename, line,
					FDRCalculationProcessor.Q_VALUE_COLUMN,
					psmQValueIndex, row.length));
			// if a valid value is not already present for this
			// column, then write in the calculated value
			String qValue = row[psmQValueIndex];
			if (qValue == null || qValue.trim().equals("") ||
				qValue.trim().equalsIgnoreCase("null")) {
				row[psmQValueIndex] = psmFDR;
				// rewrite line with updated value
				StringBuilder newLine = new StringBuilder();
				for (String value : row)
					newLine.append(value).append("\t");
				// chomp trailing tab character
				if (newLine.charAt(newLine.length() - 1) == '\t')
					newLine.setLength(newLine.length() - 1);
				line = newLine.toString();
			}
		}
		return line;
	}
	
	public void tearDown() {}
}
