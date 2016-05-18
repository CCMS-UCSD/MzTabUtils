package edu.ucsd.mztab.processors;

import edu.ucsd.mztab.model.MzTabFile;
import edu.ucsd.mztab.model.MzTabProcessor;

public class ValidityProcessor implements MzTabProcessor
{
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private int    validIndex;
	private int    invalidReasonIndex;
	private String mzTabFilename;
	
	/*========================================================================
	 * Constructor
	 *========================================================================*/
	public ValidityProcessor() {
		validIndex = -1;
		invalidReasonIndex = -1;
		mzTabFilename = null;
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
		// if this is the PSH row, then ensure the
		// file has the special validity columns
		else if (line.startsWith("PSH")) {
			if (validIndex >= 0 || invalidReasonIndex >= 0)
				throw new IllegalArgumentException(String.format(
					"Line %d of mzTab file [%s] is invalid:" +
					"\n----------\n%s\n----------\n" +
					"A \"PSH\" row was already seen previously in this file.",
					lineNumber, mzTabFilename, line));
			String[] headers = line.split("\\t");
			if (headers == null || headers.length < 1)
				throw new IllegalArgumentException(String.format(
					"Line %d of mzTab file [%s] is invalid:" +
					"\n----------\n%s\n----------\n" +
					"No columns could be found for this \"PSH\" row.",
					lineNumber, mzTabFilename, line));
			else for (int i=0; i<headers.length; i++) {
				String header = headers[i];
				if (header == null)
					continue;
				else if (header.equalsIgnoreCase("opt_global_valid"))
					validIndex = i;
				else if (header.equalsIgnoreCase("opt_global_invalid_reason"))
					invalidReasonIndex = i;
			}
			// add extra validity optional columns, if necessary
			if (validIndex < 0) {
				validIndex = headers.length;
				line = line.trim() + "\topt_global_valid";
				headers = line.split("\\t");
			}
			if (invalidReasonIndex < 0) {
				invalidReasonIndex = headers.length;
				line = line.trim() + "\topt_global_invalid_reason";
			}
		} else if (line.startsWith("PSM")) {
			if (validIndex < 0 || invalidReasonIndex < 0)
				throw new IllegalArgumentException(String.format(
					"Line %d of mzTab file [%s] is invalid:" +
					"\n----------\n%s\n----------\n" +
					"A \"PSM\" row was found before any \"PSH\" row.",
					lineNumber, mzTabFilename, line));
			String[] columns = line.split("\\t");
			if (columns == null || columns.length < 1)
				throw new IllegalArgumentException(String.format(
					"Line %d of mzTab file [%s] is invalid:" +
					"\n----------\n%s\n----------\n" +
					"No columns could be found for this \"PSM\" row.",
					lineNumber, mzTabFilename, line));
			// mark the row as valid if it isn't already
			if (validIndex >= columns.length) {
				line = line.trim() + "\tVALID";
				columns = line.split("\\t");
			}
			if (invalidReasonIndex >= columns.length)
				line = line.trim() + "\tnull";
		}
		return line;
	}
	
	public void tearDown() {}
}
