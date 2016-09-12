package edu.ucsd.mztab.model;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;

import org.apache.commons.io.FilenameUtils;

public class MzTabFile
{
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private File                     file;
	private String                   descriptor;
	private String                   mangledResultFilename;
	private String                   uploadedResultPath;
	private String                   mappedResultPath;
	private String                   relativePath;
	private Map<Integer, MzTabMsRun> msRuns;
	
	/*========================================================================
	 * Constructor
	 *========================================================================*/
	public MzTabFile(File mzTabFile) {
		// validate input mzTab file
		if (mzTabFile == null)
			throw new NullPointerException("Argument mzTab file is null.");
		else if (mzTabFile.isFile() == false || mzTabFile.canRead() == false)
			throw new IllegalArgumentException(String.format(
				"Argument mzTab file [%s] is not a valid readable file.",
				mzTabFile.getAbsolutePath()));
		else file = mzTabFile;
		// initialize all other properties to null
		descriptor = null;
		mangledResultFilename = null;
		uploadedResultPath = null;
		mappedResultPath = null;
		relativePath = null;
		msRuns = parseMsRuns(mzTabFile);
		if (msRuns == null)
			msRuns = new TreeMap<Integer, MzTabMsRun>();
	}
	
	/*========================================================================
	 * Property accessor methods
	 *========================================================================*/
	public File getFile() {
		return file;
	}
	
	public String getDescriptor() {
		if (descriptor != null)
			return descriptor;
		// by default, the descriptor should consist of the uploaded path
		else if (uploadedResultPath != null) {
			descriptor = String.format("f.%s", uploadedResultPath);
			return descriptor;
		} else return null;
	}
	
	public void setDatasetDescriptor(
		String datasetID, String mzTabRelativePath
	) {
		// dataset files are never directly under the dataset directory; if no
		// mzTab relative path is provided, then it defaults to "ccms_result"
		if (mzTabRelativePath == null || mzTabRelativePath.trim().isEmpty())
			mzTabRelativePath = "ccms_result";
		// build relative path of this mzTab file under its dataset
		StringBuilder relativePath = new StringBuilder(mzTabRelativePath);
		// append the final file path under the mzTab directory
		relativePath.append(File.separator);
		// if this is a dataset file, then it should have a mapped file path
		String filePath = getMappedMzTabPath();
		if (filePath == null)
			filePath = file.getName();
		// trim off leading slash, if present
		if (filePath.startsWith(File.separator))
			filePath = filePath.substring(1);
		relativePath.append(filePath);
		// save built relative path
		setRelativePath(
			FilenameUtils.separatorsToUnix(relativePath.toString()));
		// dataset ID is required for dataset file descriptors
		if (datasetID == null) {
			this.descriptor = null;
			return;
		}
		// build descriptor appropriately based on parameters
		StringBuilder descriptor = new StringBuilder("f.").append(datasetID);
		// append the built relative path of the mzTab file
		descriptor.append(File.separator).append(this.relativePath);
		this.descriptor = FilenameUtils.separatorsToUnix(descriptor.toString());
	}
	
	public void setTaskDescriptor(
		String username, String taskID, String mzTabRelativePath
	) {
		// build relative path of this mzTab file under its task
		StringBuilder relativePath = new StringBuilder();
		// append the relative path of the mzTab directory, if specified
		if (mzTabRelativePath != null &&
			mzTabRelativePath.trim().isEmpty() == false)
			relativePath.append(mzTabRelativePath);
		// append the final file path under the mzTab directory
		relativePath.append(File.separator);
		// if this is a task file, then it should either have a mangled filename
		// or the file itself should be present in the task mzTab directory
		String filePath = getMangledMzTabFilename();
		if (filePath == null)
			filePath = file.getName();
		// trim off leading slash, if present
		if (filePath.startsWith(File.separator))
			filePath = filePath.substring(1);
		relativePath.append(filePath);
		// save built relative path
		setRelativePath(
			FilenameUtils.separatorsToUnix(relativePath.toString()));
		// username and task ID are required for task file descriptors
		if (username == null || taskID == null) {
			this.descriptor = null;
			return;
		}
		// build descriptor appropriately based on parameters
		StringBuilder descriptor = new StringBuilder("u.");
		descriptor.append(username).append(File.separator).append(taskID);
		// append the built relative path of the mzTab file
		descriptor.append(File.separator).append(this.relativePath);
		this.descriptor = FilenameUtils.separatorsToUnix(descriptor.toString());
	}
	
	public String getMangledResultFilename() {
		return mangledResultFilename;
	}
	
	public String getMangledMzTabFilename() {
		if (mangledResultFilename == null)
			return null;
		else if (FilenameUtils.getExtension(mangledResultFilename)
			.equalsIgnoreCase("mztab"))
			return mangledResultFilename;
		else return String.format("%s.mzTab",
			FilenameUtils.getBaseName(mangledResultFilename));
	}
	
	public void setMangledResultFilename(String mangledResultFilename) {
		this.mangledResultFilename = mangledResultFilename;
	}
	
	public String getUploadedResultPath() {
		return uploadedResultPath;
	}
	
	public String getUploadedMzTabPath() {
		if (uploadedResultPath == null)
			return null;
		else if (FilenameUtils.getExtension(uploadedResultPath)
			.equalsIgnoreCase("mztab"))
			return uploadedResultPath;
		else return String.format("%s%s.mzTab",
			FilenameUtils.getPath(uploadedResultPath),
			FilenameUtils.getBaseName(uploadedResultPath));
	}
	
	public void setUploadedResultPath(String uploadedResultPath) {
		this.uploadedResultPath = uploadedResultPath;
	}
	
	public String getMappedResultPath() {
		return mappedResultPath;
	}
	
	public String getMappedMzTabPath() {
		if (mappedResultPath == null)
			return null;
		else if (FilenameUtils.getExtension(mappedResultPath)
			.equalsIgnoreCase("mztab"))
			return mappedResultPath;
		else return String.format("%s%s.mzTab",
			FilenameUtils.getPath(mappedResultPath),
			FilenameUtils.getBaseName(mappedResultPath));
	}
	
	public void setMappedResultPath(String mappedResultPath) {
		this.mappedResultPath = mappedResultPath;
	}
	
	public String getRelativePath() {
		return relativePath;
	}
	
	public void setRelativePath(String relativePath) {
		this.relativePath = relativePath;
	}
	
	public String getMzTabPath() {
		String path = getRelativePath();
		if (path == null)
			path = getMappedMzTabPath();
		if (path == null)
			path = getUploadedMzTabPath();
		if (path == null)
			path = getMangledMzTabFilename();
		if (path == null)
			path = file.getAbsolutePath();
		return path;
	}
	
	public String getMzTabFilename() {
		return FilenameUtils.getName(getMzTabPath());
	}
	
	public Map<Integer, MzTabMsRun> getMsRuns() {
		return new TreeMap<Integer, MzTabMsRun>(msRuns);
	}
	
	public MzTabMsRun getMsRun(int msRun) {
		return msRuns.get(msRun);
	}
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public static Map<Integer, MzTabMsRun> parseMsRuns(File mzTabFile) {
		if (mzTabFile == null)
			return null;
		Map<Integer, MzTabMsRun> msRuns = new TreeMap<Integer, MzTabMsRun>();
		BufferedReader reader = null;
		int highestIndex = 0;
		try {
			reader = new BufferedReader(new FileReader(mzTabFile));
			String line = null;
			int lineNumber = 0;
			while (true) {
				line = reader.readLine();
				if (line == null)
					break;
				lineNumber++;
				// don't keep reading if we're past the metadata section
				if (line.startsWith("MTD") == false &&
					line.startsWith("COM") == false &&
					line.trim().equals("") == false)
					break;
				// get spectrum file data, if this is a file location line
				Matcher matcher =
					MzTabConstants.FILE_LINE_PATTERN.matcher(line);
				if (matcher.matches()) {
					int index = Integer.parseInt(matcher.group(1));
					if (index > highestIndex)
						highestIndex = index;
					// make sure this mapping hasn't already been recorded
					if (msRuns.get(index) != null)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:\n" +
							"----------\n%s\n----------\n" +
							"\"ms_run[1-n]-location\" index (%d) was " +
							"already seen previously in this file.",
							lineNumber, mzTabFile.getAbsolutePath(), line,
							index));
					else msRuns.put(
						index, new MzTabMsRun(index, matcher.group(2)));
				}
			}
		} catch (RuntimeException error) {
			throw error;
		} catch (Throwable error) {
			throw new RuntimeException(error);
		} finally {
			try { reader.close(); }
			catch (Throwable error) {}
		}
		return msRuns;
	}
	
	@Override
	public String toString() {
		StringBuilder mzTab = new StringBuilder("{");
		mzTab.append("\n\t\"file\":");
		mzTab.append("\"").append(getFile().getAbsolutePath()).append("\"");
		String descriptor = getDescriptor();
		mzTab.append(",\n\t\"descriptor\":");
		if (descriptor == null)
			mzTab.append("null");
		else mzTab.append("\"").append(descriptor).append("\"");
		String mangledResultFilename = getMangledResultFilename();
		mzTab.append(",\n\t\"mangled\":");
		if (mangledResultFilename == null)
			mzTab.append("null");
		else mzTab.append("\"").append(mangledResultFilename).append("\"");
		String uploadedResultPath = getUploadedResultPath();
		mzTab.append(",\n\t\"uploaded\":");
		if (uploadedResultPath == null)
			mzTab.append("null");
		else mzTab.append("\"").append(uploadedResultPath).append("\"");
		String mappedResultPath = getMappedResultPath();
		mzTab.append(",\n\t\"mapped\":");
		if (mappedResultPath == null)
			mzTab.append("null");
		else mzTab.append("\"").append(mappedResultPath).append("\"");
		mzTab.append(",\n\t\"ms_runs\":[");
		for (Integer msRunIndex : msRuns.keySet()) {
			mzTab.append("\n\t\t").append(msRuns.get(msRunIndex).toString());
			mzTab.append(",");
		}
		// chomp trailing comma
		if (mzTab.charAt(mzTab.length() - 1) == ',')
			mzTab.setLength(mzTab.length() - 1);
		mzTab.append("\n\t]\n}");
		return mzTab.toString();
	}
}
