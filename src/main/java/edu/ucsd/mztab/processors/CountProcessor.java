package edu.ucsd.mztab.processors;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;

import edu.ucsd.mztab.model.MzTabConstants;
import edu.ucsd.mztab.model.MzTabConstants.MzTabSection;
import edu.ucsd.mztab.model.MzTabFile;
import edu.ucsd.mztab.model.MzTabProcessor;
import edu.ucsd.mztab.model.MzTabSectionHeader;

public class CountProcessor implements MzTabProcessor
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String[] RELEVANT_PRT_COLUMNS =
		new String[]{ "accession", "modifications" };
	private static final String[] RELEVANT_PEP_COLUMNS =
		new String[]{ "sequence", "accession", "modifications" };
	private static final String[] RELEVANT_PSM_COLUMNS =
		new String[]{ "PSM_ID", "sequence", "accession", "modifications" };
	
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private Map<String, Integer>     counts;
	private Map<String, Set<String>> uniqueElements;
	private String                   mzTabFilename;
	private MzTabSectionHeader       prtHeader;
	private MzTabSectionHeader       pepHeader;
	private MzTabSectionHeader       psmHeader;
	
	/*========================================================================
	 * Constructor
	 *========================================================================*/
	public CountProcessor(Map<String, Integer> counts) {
		// validate counts map
		if (counts == null)
			throw new NullPointerException("Argument counts map is null.");
		else this.counts = counts;
		// initialize unique elements map
		uniqueElements = new HashMap<String, Set<String>>();
		// initialize mzTab file parameters
		mzTabFilename = null;
		prtHeader = null;
		pepHeader = null;
		psmHeader = null;
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
		// protein section
		else if (line.startsWith("PRH")) {
			if (prtHeader != null)
				throw new IllegalArgumentException(String.format(
					"Line %d of mzTab file [%s] is invalid:\n" +
					"----------\n%s\n----------\n" +
					"A \"PRH\" row was already seen previously in this file.",
					lineNumber, mzTabFilename, line));
			prtHeader = new MzTabSectionHeader(line);
			prtHeader.validateHeaderExpectations(
				MzTabSection.PRT, Arrays.asList(RELEVANT_PRT_COLUMNS));
		} else if (line.startsWith("PRT")) {
			if (prtHeader == null)
				throw new IllegalArgumentException(String.format(
					"Line %d of mzTab file [%s] is invalid:\n" +
					"----------\n%s\n----------\n" +
					"A \"PRT\" row was found before any \"PRH\" row.",
					lineNumber, mzTabFilename, line));
			else prtHeader.validateMzTabRow(line);
			incrementCount("PRT");
			// extract count-worthy elements from this PRT row
			String[] columns = line.split("\\t");
			for (String column : RELEVANT_PRT_COLUMNS)
				addElement(column, columns[prtHeader.getColumnIndex(column)]);
		}
		// peptide section
		else if (line.startsWith("PEH")) {
			if (pepHeader != null)
				throw new IllegalArgumentException(String.format(
					"Line %d of mzTab file [%s] is invalid:\n" +
					"----------\n%s\n----------\n" +
					"A \"PEH\" row was already seen previously in this file.",
					lineNumber, mzTabFilename, line));
			pepHeader = new MzTabSectionHeader(line);
			pepHeader.validateHeaderExpectations(
				MzTabSection.PEP, Arrays.asList(RELEVANT_PEP_COLUMNS));
		} else if (line.startsWith("PEP")) {
			if (pepHeader == null)
				throw new IllegalArgumentException(String.format(
					"Line %d of mzTab file [%s] is invalid:\n" +
					"----------\n%s\n----------\n" +
					"A \"PEP\" row was found before any \"PEH\" row.",
					lineNumber, mzTabFilename, line));
			else pepHeader.validateMzTabRow(line);
			incrementCount("PEP");
			// extract count-worthy elements from this PEP row
			String[] columns = line.split("\\t");
			for (String column : RELEVANT_PEP_COLUMNS)
				addElement(column, columns[pepHeader.getColumnIndex(column)]);
		}
		// PSM section
		else if (line.startsWith("PSH")) {
			if (psmHeader != null)
				throw new IllegalArgumentException(String.format(
					"Line %d of mzTab file [%s] is invalid:\n" +
					"----------\n%s\n----------\n" +
					"A \"PSH\" row was already seen previously in this file.",
					lineNumber, mzTabFilename, line));
			psmHeader = new MzTabSectionHeader(line);
			psmHeader.validateHeaderExpectations(
				MzTabSection.PSM, Arrays.asList(RELEVANT_PSM_COLUMNS));
		} else if (line.startsWith("PSM")) {
			if (psmHeader == null)
				throw new IllegalArgumentException(String.format(
					"Line %d of mzTab file [%s] is invalid:\n" +
					"----------\n%s\n----------\n" +
					"A \"PSM\" row was found before any \"PSH\" row.",
					lineNumber, mzTabFilename, line));
			else psmHeader.validateMzTabRow(line);
			incrementCount("PSM");
			// extract count-worthy elements from this PSM row
			String[] columns = line.split("\\t");
			for (String column : RELEVANT_PSM_COLUMNS)
				addElement(column, columns[psmHeader.getColumnIndex(column)]);
		}
		return line;
	}
	
	public void tearDown() {
		// ensure all basic row counts are present
		if (counts.get("PSM") == null)
			counts.put("PSM", 0);
		if (counts.get("PEP") == null)
			counts.put("PEP", 0);
		if (counts.get("PRT") == null)
			counts.put("PRT", 0);
		// add count of found unique PSMs
		if (uniqueElements.containsKey("PSM_ID"))
			counts.put("PSM_ID", uniqueElements.get("PSM_ID").size());
		else counts.put("PSM_ID", 0);
		// add count of found unique peptides
		if (uniqueElements.containsKey("sequence"))
			counts.put("sequence", uniqueElements.get("sequence").size());
		else counts.put("sequence", 0);
		// add count of found unique proteins
		if (uniqueElements.containsKey("accession"))
			counts.put("accession", uniqueElements.get("accession").size());
		else counts.put("accession", 0);
		// add count of found unique modifications
		if (uniqueElements.containsKey("modification"))
			counts.put(
				"modification", uniqueElements.get("modification").size());
		else counts.put("modification", 0);
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
	
	private void addElement(String type, String value) {
		if (type == null || value == null ||
			value.trim().equalsIgnoreCase("null"))
			return;
		else if (type.equals("modifications"))
			addModifications(value);
		else {
			Set<String> values = uniqueElements.get(type);
			if (values == null)
				values = new HashSet<String>();
			values.add(value);
			uniqueElements.put(type, values);
		}
	}
	
	private void addModifications(String modifications) {
		if (modifications == null)
			return;
		Matcher matcher =
			MzTabConstants.MZTAB_MODIFICATION_PATTERN.matcher(modifications);
		if (matcher.find() == false)
			throw new IllegalArgumentException(String.format(
				"Argument modification string [%s] is invalid: mzTab " +
				"modification strings should be comma-delimited " +
				"lists of strings each conforming to the following " +
				"format:\n%s", modifications,
				MzTabConstants.MZTAB_MODIFICATION_STRING_FORMAT));
		else do {
			addElement("modification", matcher.group(2));
		} while (matcher.find());
	}
}
