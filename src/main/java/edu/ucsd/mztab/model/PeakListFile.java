package edu.ucsd.mztab.model;

import java.io.File;
import java.net.URI;

public class PeakListFile
{
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private String msRunLocation;
	private File   file;
	
	/*========================================================================
	 * Constructors
	 *========================================================================*/
	public PeakListFile(String msRunLocation) {
		// validate input ms_run location string
		if (msRunLocation == null)
			throw new NullPointerException(
				"Argument \"ms_run[1-n]-location\" value cannot be null.");
		else this.msRunLocation = msRunLocation;
	}
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public String getMsRunLocation() {
		return msRunLocation;
	}
}
