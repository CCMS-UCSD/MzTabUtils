package edu.ucsd.mztab.model;

public class MzTabMsRun
{
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private int       msRunIndex;
	private String    msRunLocation;
	private String    mangledPeakListFilename;
	private String    uploadedPeakListPath;
	private String    mappedPeakListPath;
	
	/*========================================================================
	 * Constructors
	 *========================================================================*/
	public MzTabMsRun(Integer msRunIndex, String msRunLocation) {
		// validate ms_run index
		if (msRunIndex == null)
			throw new NullPointerException(
				"Argument \"ms_run[1-n]\" index cannot be null.");
		else if (msRunIndex < 1)
			throw new IllegalArgumentException(String.format(
				"Argument \"ms_run[1-n]\" index %d is less than 1.",
				msRunIndex));
		else this.msRunIndex = msRunIndex;
		// validate ms_run location string
		if (msRunLocation == null)
			throw new NullPointerException(
				"Argument \"ms_run[1-n]-location\" value cannot be null.");
		else this.msRunLocation = msRunLocation;
	}
	
	/*========================================================================
	 * Property accessor methods
	 *========================================================================*/
	public int getMsRunIndex() {
		return msRunIndex;
	}
	
	public String getMsRunLocation() {
		return msRunLocation;
	}
	
	public String getMangledPeakListFilename() {
		return mangledPeakListFilename;
	}
	
	public void setMangledPeakListFilename(String mangledPeakListFilename) {
		this.mangledPeakListFilename = mangledPeakListFilename;
	}
	
	public String getUploadedPeakListPath() {
		return uploadedPeakListPath;
	}
	
	public void setUploadedPeakListPath(String uploadedPeakListPath) {
		this.uploadedPeakListPath = uploadedPeakListPath;
	}
	
	public String getMappedPeakListPath() {
		return mappedPeakListPath;
	}
	
	public void setMappedPeakListPath(String mappedPeakListPath) {
		this.mappedPeakListPath = mappedPeakListPath;
	}
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	@Override
	public String toString() {
		StringBuilder msRun = new StringBuilder("{");
		msRun.append("\"index\":").append(getMsRunIndex()).append(",");
		msRun.append("\"location\":\"")
			.append(getMsRunLocation()).append("\",");
		msRun.append("\"mangled\":");
		String mangled = getMangledPeakListFilename();
		if (mangled == null)
			msRun.append("null");
		else msRun.append("\"").append(mangled).append("\"");
		msRun.append(",\"uploaded\":");
		String uploaded = getUploadedPeakListPath();
		if (uploaded == null)
			msRun.append("null");
		else msRun.append("\"").append(uploaded).append("\"");
		msRun.append(",\"mapped\":");
		String mapped = getMappedPeakListPath();
		if (mapped == null)
			msRun.append("null");
		else msRun.append("\"").append(mapped).append("\"");
		msRun.append("}");
		return msRun.toString();
	}
}
