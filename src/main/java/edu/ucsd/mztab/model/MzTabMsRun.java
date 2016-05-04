package edu.ucsd.mztab.model;

import edu.ucsd.util.CommonUtils;

public class MzTabMsRun
{
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private int    msRunIndex;
	private String msRunLocation;
	private String descriptor;
	private String mangledPeakListFilename;
	private String uploadedPeakListPath;
	private String mappedPeakListPath;
	
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
		// initialize all other properties to null
		descriptor = null;
		mangledPeakListFilename = null;
		uploadedPeakListPath = null;
		mappedPeakListPath = null;
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
	
	public String getCleanedMsRunLocation() {
		return CommonUtils.cleanFileURL(msRunLocation);
	}
	
	public String getDescriptor() {
		if (descriptor != null)
			return descriptor;
		// by default, the descriptor should consist of the uploaded path
		else if (uploadedPeakListPath != null) {
			descriptor = String.format("f.%s", uploadedPeakListPath);
			return descriptor;
		} else return null;
	}
	
	public void setDatasetDescriptor(
		String datasetID, String peakListRelativePath
	) {
		// dataset ID is required for dataset file descriptors
		if (datasetID == null) {
			this.descriptor = null;
			return;
		}
		// build descriptor appropriately based on parameters
		StringBuilder descriptor = new StringBuilder("f.").append(datasetID);
		// dataset files are never directly under the dataset directory; if no
		// peak list relative path is provided, then it defaults to "peak"
		if (peakListRelativePath == null ||
			peakListRelativePath.trim().isEmpty())
			peakListRelativePath = "peak";
		// append the relative path of the peak list directory
		descriptor.append("/").append(peakListRelativePath);
		// append the final file path under the mzTab directory
		descriptor.append("/");
		// if this is a dataset file, then it should have a mapped file path
		String mappedPath = getMappedPeakListPath();
		if (mappedPath != null)
			descriptor.append(mappedPath);
		else descriptor.append(getCleanedMsRunLocation());
		this.descriptor = descriptor.toString();
	}
	
	public void setTaskDescriptor(
		String username, String taskID, String peakListRelativePath
	) {
		// username and task ID are required for task file descriptors
		if (username == null || taskID == null) {
			this.descriptor = null;
			return;
		}
		// build descriptor appropriately based on parameters
		StringBuilder descriptor = new StringBuilder("u.");
		descriptor.append(username).append("/").append(taskID);
		// append the relative path of the peak list directory, if specified
		if (peakListRelativePath != null &&
			peakListRelativePath.trim().isEmpty() == false)
			descriptor.append("/").append(peakListRelativePath);
		// append the final file path under the mzTab directory
		descriptor.append("/");
		// if this is a task file, then it should either have a mangled filename
		// or the file itself should be present in the task peak list directory
		String mangledFilename = getMangledPeakListFilename();
		if (mangledFilename != null)
			descriptor.append(mangledFilename);
		else descriptor.append(getCleanedMsRunLocation());
		this.descriptor = descriptor.toString();
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
	
	public String getPeakListPath() {
		String path = getMappedPeakListPath();
		if (path == null)
			path = getUploadedPeakListPath();
		if (path == null)
			path = getMangledPeakListFilename();
		return path;
	}
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	@Override
	public String toString() {
		StringBuilder msRun = new StringBuilder("{");
		msRun.append("\"index\":").append(getMsRunIndex());
		msRun.append(",\n\t\t\"location\":");
		msRun.append("\"").append(getMsRunLocation()).append("\"");
		String descriptor = getDescriptor();
		msRun.append(",\n\t\t\"descriptor\":");
		if (descriptor == null)
			msRun.append("null");
		else msRun.append("\"").append(descriptor).append("\"");
		String mangledPeakListFilename = getMangledPeakListFilename();
		msRun.append(",\n\t\t\"mangled\":");
		if (mangledPeakListFilename == null)
			msRun.append("null");
		else msRun.append("\"").append(mangledPeakListFilename).append("\"");
		String uploadedPeakListPath = getUploadedPeakListPath();
		msRun.append(",\n\t\t\"uploaded\":");
		if (uploadedPeakListPath == null)
			msRun.append("null");
		else msRun.append("\"").append(uploadedPeakListPath).append("\"");
		String mappedPeakListPath = getMappedPeakListPath();
		msRun.append(",\n\t\t\"mapped\":");
		if (mappedPeakListPath == null)
			msRun.append("null");
		else msRun.append("\"").append(mappedPeakListPath).append("\"");
		msRun.append("}");
		return msRun.toString();
	}
}
