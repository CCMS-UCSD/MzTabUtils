package edu.ucsd.mztab.model;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;

public class MzTabFile
{
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private File                       file;
	private Map<Integer, PeakListFile> peakListFiles;
	
	/*========================================================================
	 * Constructor
	 *========================================================================*/
	public MzTabFile(File file) {
		// validate input mzTab file
		if (file == null)
			throw new NullPointerException("Argument mzTab file is null.");
		else if (file.isFile() == false || file.canRead() == false)
			throw new IllegalArgumentException(String.format(
				"Argument mzTab file [%s] is not a valid readable file.",
				file.getAbsolutePath()));
		else this.file = file;
		// extract peak list file references from mzTab file
		peakListFiles = new LinkedHashMap<Integer, PeakListFile>();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(this.file));
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
					if (peakListFiles.containsKey(index))
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:\n" +
							"----------\n%s\n----------\n" +
							"\"ms_run[1-n]-location\" index (%d) was already " +
							"seen previously in this file.",
							lineNumber, file.getAbsolutePath(), line, index));
					peakListFiles.put(
						index, new PeakListFile(matcher.group(2)));
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
		// validate peak list files map
		if (peakListFiles.isEmpty())
			throw new IllegalArgumentException(String.format(
				"No valid \"ms_run[1-n]-location\" lines were found " +
				"in mzTab file [%s].", file.getAbsolutePath()));
	}
	
	/*========================================================================
	 * Property accessor methods
	 *========================================================================*/
	public File getFile() {
		return file;
	}
	
	public PeakListFile getPeakListFile(int msRun) {
		return peakListFiles.get(msRun);
	}
}
