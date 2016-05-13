package edu.ucsd.mztab.processors;

import java.util.regex.Matcher;

import edu.ucsd.mztab.model.MzTabConstants;
import edu.ucsd.mztab.model.MzTabFile;
import edu.ucsd.mztab.model.MzTabMsRun;
import edu.ucsd.mztab.model.MzTabProcessor;

public class MsRunCleanProcessor implements MzTabProcessor
{
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private MzTabFile mzTabFile;
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public void setUp(MzTabFile mzTabFile) {
		if (mzTabFile == null)
			throw new NullPointerException("Argument mzTab file is null.");
		else this.mzTabFile = mzTabFile;
	}
	
	public String processMzTabLine(String line, int lineNumber) {
		if (line == null)
			throw new NullPointerException(
				"Processed mzTab line cannot be null.");
		else if (line.startsWith("MTD")) {
			// if this is an ms_run-location line, update it
			Matcher matcher = MzTabConstants.FILE_LINE_PATTERN.matcher(line);
			if (matcher.matches()) {
				// get ProteoSAFe file descriptor for this ms_run
				int msRunIndex;
				try {
					msRunIndex = Integer.parseInt(matcher.group(1));
				} catch (NumberFormatException error) {
					throw new IllegalArgumentException(String.format(
						"Line %d of mzTab file [%s] is invalid:\n" +
						"----------\n%s\n----------\n" +
						"No ms_run index could be parsed from this " +
						"file reference.", lineNumber,
						mzTabFile.getMzTabFilename(), line));
				}
				MzTabMsRun msRun = mzTabFile.getMsRun(msRunIndex);
				if (msRun == null)
					throw new IllegalArgumentException(String.format(
						"Line %d of mzTab file [%s] is invalid:\n" +
						"----------\n%s\n----------\n" +
						"No file mapping could be found for ms_run[%d].",
						lineNumber, mzTabFile.getMzTabFilename(), line,
						msRunIndex));
				String path = msRun.getDescriptor();
				if (path == null)
					throw new IllegalArgumentException(String.format(
						"Line %d of mzTab file [%s] is invalid:\n" +
						"----------\n%s\n----------\n" +
						"No ProteoSAFe file location could be " +
						"determined for ms_run[%d].", lineNumber,
						mzTabFile.getMzTabFilename(), line, msRunIndex));
				// strip off descriptor prefix
				if (path.matches("^.{1}\\..*$"))
					path = path.substring(2);
				// update this ms_run-location to reference the descriptor
				line = String.format(
					"MTD\tms_run[%d]-location\tfile://%s", msRunIndex, path);
			}
		}
		return line;
	}
	
	public void tearDown() {}
}
