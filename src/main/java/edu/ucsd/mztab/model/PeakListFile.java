package edu.ucsd.mztab.model;

import java.io.File;
import java.net.URI;

import org.w3c.dom.Document;

public class PeakListFile
{
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private File   file;
	private String descriptor;
	private String msRunLocation;
	
	/*========================================================================
	 * Constructors
	 *========================================================================*/
	public PeakListFile(String msRunLocation, Document parameters) {
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
