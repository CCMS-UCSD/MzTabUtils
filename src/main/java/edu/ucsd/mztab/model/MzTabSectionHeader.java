package edu.ucsd.mztab.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.ucsd.mztab.model.MzTabConstants.MzTabSection;

public class MzTabSectionHeader
{
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private String               line;
	private String[]             tokens;
	private Map<String, Integer> columns;
	private MzTabSection         section;
	
	/*========================================================================
	 * Constructor
	 *========================================================================*/
	public MzTabSectionHeader(String line) {
		// validate input section header line
		if (line == null)
			throw new NullPointerException(
				"Argument section header line is null.");
		else this.line = line;
		// split header line into tab-delimited tokens
		tokens = line.split("\\t");
		if (tokens == null || tokens.length < 1)
			throw new IllegalArgumentException(String.format(
				"Argument section header line is invalid:" +
				"\n----------\n%s\n----------\n" +
				"No tab-delimited column header elements were found.", line));
		// the first token in the header line should indicate
		// which mzTab file section this header belongs to
		if (tokens[0].equals("PRH"))
			section = MzTabSection.PRT;
		else if (tokens[0].equals("PEH"))
			section = MzTabSection.PEP;
		else if (tokens[0].equals("PSH"))
			section = MzTabSection.PSM;
		else if (tokens[0].equals("SMH"))
			section = MzTabSection.SML;
		else throw new IllegalArgumentException(String.format(
			"Argument section header line is invalid:" +
			"\n----------\n%s\n----------\n",
			"The first token [%s] does not correspond to any recognized " +
			"mzTab section identifier.", line, tokens[0]));
		// store column indices in map for faster lookup
		columns = new HashMap<String, Integer>(tokens.length);
		for (int i=0; i<tokens.length; i++)
			columns.put(tokens[i], i);
	}
	
	/*========================================================================
	 * Property accessor methods
	 *========================================================================*/
	public String getHeaderLine() {
		return line;
	}
	
	public List<String> getColumns() {
		return new ArrayList<String>(Arrays.asList(tokens));
	}
	
	public int getColumnCount() {
		return tokens.length;
	}
	
	public Integer getColumnIndex(String column) {
		if (column == null)
			return null;
		else return columns.get(column);
	}
	
	public MzTabSection getSection() {
		return section;
	}
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public void validateHeaderExpectations(
		MzTabSection section, Collection<String> columns
	) {
		// validate expected header type
		String headerType = null;
		if (section == null)
			headerType = "mzTab";
		else headerType = section.name();
		if (section != null && section.equals(this.section) == false)
			throw new IllegalArgumentException(String.format(
				"%s section header is invalid:" +
				"\n----------\n%s\n----------\n" +
				"The first token [%s] does not correspond to a %s header row.",
				headerType, line, tokens[0], section.name()));
		// validate expected columns
		if (columns == null || columns.isEmpty())
			return;
		else for (String column : columns) {
			if (getColumnIndex(column) == null)
				throw new IllegalArgumentException(String.format(
					"%s section header is invalid:" +
					"\n----------\n%s\n----------\n" +
					"No \"%s\" column header element was found.",
					headerType, line, column));
		}
	}
	
	public void validateMzTabRow(String line) {
		if (line == null)
			throw new NullPointerException(
				"Argument mzTab row line is null.");
		// split line into tab-delimited columns and compare against header
		String[] columns = line.split("\\t", -1);
		if (columns == null || columns.length < 1)
			throw new IllegalArgumentException(String.format(
				"Argument mzTab row line is invalid:" +
				"\n----------\n%s\n----------\n" +
				"No tab-delimited column elements were found.", line));
		int expectedColumnCount = getColumnCount();
		if (columns.length != expectedColumnCount)
			throw new IllegalArgumentException(String.format(
				"Argument mzTab row line is invalid:\n" +
				"The number of columns (%d) does not match the number " +
				"previously parsed from the header row (%d)." +
				"\n----------\n%s",
				columns.length, expectedColumnCount,
				dumpHeaderVsRowComparison(columns)));
		// ensure that this row is of the same type as the header
		MzTabSection rowType = MzTabSection.valueOf(columns[0]);
		if (rowType == null || rowType.equals(section) == false)
			throw new IllegalArgumentException(String.format(
				"Argument mzTab row line is invalid:" +
				"\n----------\n%s\n----------\n" +
				"The first token [%s] does not correspond to the expected " +
				"mzTab file section as implied by the header row [%s].",
				line, columns[0], section.name()));
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private String dumpHeaderVsRowComparison(String[] row) {
		if (row == null)
			row = new String[]{};
		StringBuilder comparison =
			new StringBuilder("Header -> Row\n-------------");
		// print the row values aligning with each recorded column header
		for (int i=0; i<tokens.length; i++) {
			String header = tokens[i];
			if (header == null)
				header = "null";
			String value = null;
			if (row.length > i) {
				value = row[i];
				if (value == null)
					value = "null";
			} else value = "--";
			comparison.append("\n[").append(header).append("] -> ");
			comparison.append("[").append(value).append("]");
		}
		// print any row values beyond the boundaries of the recorded header
		if ((row.length - tokens.length) > 0) {
			for (int i=tokens.length; i<row.length; i++) {
				String value = row[i];
				if (value == null)
					value = "null";
				comparison.append("\n[--] -> ");
				comparison.append("[").append(value).append("]");
			}
		}
		return comparison.toString();
	}
}
