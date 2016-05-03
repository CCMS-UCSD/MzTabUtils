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
		msRuns = parseMsRuns(mzTabFile);
		if (msRuns == null)
			msRuns = new TreeMap<Integer, MzTabMsRun>();
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
	
	/*========================================================================
	 * Property accessor methods
	 *========================================================================*/
	public File getFile() {
		return file;
	}
	
	public String getDescriptor() {
		return descriptor;
	}
	
	public void setDescriptor(String descriptor) {
		this.descriptor = descriptor;
	}
	
	public String getMangledResultFilename() {
		return mangledResultFilename;
	}
	
	public void setMangledResultFilename(String mangledResultFilename) {
		this.mangledResultFilename = mangledResultFilename;
	}
	
	public String getUploadedResultPath() {
		return uploadedResultPath;
	}
	
	public void setUploadedResultPath(String uploadedResultPath) {
		this.uploadedResultPath = uploadedResultPath;
	}
	
	public String getUploadedResultFilename() {
		if (uploadedResultPath == null)
			return null;
		else return FilenameUtils.getName(uploadedResultPath);
	}
	
	public String getMzTabFilename() {
		String uploadedFilename = getUploadedResultFilename();
		if (uploadedFilename == null)
			return file.getName();
		else if (FilenameUtils.getExtension(uploadedFilename).equalsIgnoreCase(
			"mztab"))
			return uploadedFilename;
		else return String.format(
			"%s.mzTab", FilenameUtils.getBaseName(uploadedFilename));
	}
	
	public Map<Integer, MzTabMsRun> getMsRuns() {
		return new TreeMap<Integer, MzTabMsRun>(msRuns);
	}
	
	public MzTabMsRun getMsRun(int msRun) {
		return msRuns.get(msRun);
	}
}
