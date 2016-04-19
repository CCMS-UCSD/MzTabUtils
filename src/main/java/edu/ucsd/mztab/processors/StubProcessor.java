package edu.ucsd.mztab.processors;

import edu.ucsd.mztab.model.MzTabFile;
import edu.ucsd.mztab.model.MzTabProcessor;

public class StubProcessor implements MzTabProcessor
{
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public void setUp(MzTabFile mzTabFile) {}
	
	public String processMzTabLine(String line, int lineNumber) {
		return line;
	}
	
	public void tearDown() {}
}
