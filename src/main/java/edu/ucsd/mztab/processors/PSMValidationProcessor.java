package edu.ucsd.mztab.processors;

import java.util.List;
import java.util.Map;

import edu.ucsd.mztab.model.MzTabConstants;
import edu.ucsd.mztab.model.MzTabFile;
import edu.ucsd.mztab.model.MzTabProcessor;
import edu.ucsd.mztab.model.MzTabSectionHeader;

public class PSMValidationProcessor
implements MzTabProcessor
{
	/*========================================================================
	 * Properties
	 *========================================================================*/
	// mzTab file properties
	private String               mzTabFilename;
	private MzTabSectionHeader   psmHeader;
	private int                  validIndex;
	private int                  invalidReasonIndex;
	// PSM row counter map ("psmRows", "invalidRows")
	private Map<String, Integer> counts;
	
	/*========================================================================
	 * Constructor
	 *========================================================================*/
	public PSMValidationProcessor(Map<String, Integer> counts) {
		// validate counts map
		if (counts == null)
			throw new NullPointerException("Argument counts map is null.");
		else this.counts = counts;
		// initialize mzTab validity column indices
		validIndex = -1;
		invalidReasonIndex = -1;
	}
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public void setUp(MzTabFile mzTabFile) {
		if (mzTabFile == null)
			throw new NullPointerException("Argument mzTab file is null.");
		else this.mzTabFilename = mzTabFile.getMzTabFilename();
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
			// add CCMS-controlled validity columns, if not already present
			if (validIndex < 0) {
				validIndex = headers.size();
				line = String.format("%s\t%s",
					line.trim(), MzTabConstants.VALID_COLUMN);
				headers.add(MzTabConstants.VALID_COLUMN);
			}
			if (invalidReasonIndex < 0) {
				invalidReasonIndex = headers.size();
				line = String.format("%s\t%s",
					line.trim(), MzTabConstants.INVALID_REASON_COLUMN);
				headers.add(MzTabConstants.INVALID_REASON_COLUMN);
			}
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
			// add this row to the overall count of PSM rows encountered so far
			incrementCount("psmRows");
			// ensure that the valid column is present in this row
			String[] row = line.split("\\t");
			if (validIndex >= row.length) {
				String[] newRow = new String[validIndex + 1];
				for (int i=0; i<row.length; i++)
					newRow[i] = row[i];
				row = newRow;
				// set the valid column's value to "VALID" by default
				row[validIndex] = "VALID";
				line = getLine(row);
			}
			// ensure that the invalid reason column is present in this row
			if (invalidReasonIndex >= row.length) {
				String[] newRow = new String[invalidReasonIndex + 1];
				for (int i=0; i<row.length; i++)
					newRow[i] = row[i];
				row = newRow;
				// set the invalid reason column's value to "null" by default
				row[invalidReasonIndex] = "null";
				line = getLine(row);
			}
			// if this PSM row has been marked as invalid, add it to the
			// count of invalid rows and ensure that a reason is given
			String valid = row[validIndex];
			if (valid != null && valid.trim().equalsIgnoreCase("INVALID")) {
				incrementCount("invalidRows");
				String reason = row[invalidReasonIndex];
				if (reason == null || reason.trim().equalsIgnoreCase("null")) {
					row[invalidReasonIndex] =
						"This PSM was marked as invalid by its source.";
					line = getLine(row);
				}
			}
		}
		return line;
	}
	
	public void tearDown() {}
	
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
