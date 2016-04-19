package edu.ucsd.mztab.model;

public interface MzTabProcessor
{
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public void setUp(MzTabFile mzTabFile);
	public String processMzTabLine(String line, int lineNumber);
	public void tearDown();
}
