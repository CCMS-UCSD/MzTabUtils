package edu.ucsd.mztab.model;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.xpath.XPathAPI;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import edu.ucsd.util.FileIOUtils;

public class MzTabFile
{
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private File                       file;
	private String                     uploadedPath;
	private String                     mangledFilename;
	private Map<Integer, PeakListFile> peakListFiles;
	
	/*========================================================================
	 * Constructor
	 *========================================================================*/
	public MzTabFile(File mzTabFile, File paramsFile) {
		// validate input mzTab file
		if (mzTabFile == null)
			throw new NullPointerException("Argument mzTab file is null.");
		else if (mzTabFile.isFile() == false || mzTabFile.canRead() == false)
			throw new IllegalArgumentException(String.format(
				"Argument mzTab file [%s] is not a valid readable file.",
				mzTabFile.getAbsolutePath()));
		else file = mzTabFile;
		// validate argument params.xml file
		if (paramsFile == null)
			throw new NullPointerException(
				"Argument params.xml file cannot be null.");
		else if (paramsFile.isFile() == false || paramsFile.canRead() == false)
			throw new IllegalArgumentException(
				"Argument params.xml file must be a readable file.");
		// parse params.xml file into a DOM object
		Document parameters = null;
		try { parameters = FileIOUtils.parseXML(paramsFile); }
		catch (Throwable error) {}
		if (parameters == null)
			throw new NullPointerException(
				"Argument params.xml file could not be parsed.");
		// read parameters to extract the upload file mapping
		// for this mzTab file, if there is one
		ImmutablePair<String, String> mapping =
			getResultFileMapping(mzTabFile, parameters);
		if (mapping != null) {
			uploadedPath = mapping.getLeft();
			mangledFilename = mapping.getRight();
		} else {
			uploadedPath = null;
			mangledFilename = null;
		}
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
						index, new PeakListFile(matcher.group(2), parameters));
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
	
	public String getUploadedPath() {
		return uploadedPath;
	}
	
	public String getUploadedFilename() {
		if (uploadedPath == null)
			return null;
		else return FilenameUtils.getName(uploadedPath);
	}
	
	public String getMzTabFilename() {
		String uploadedFilename = getUploadedFilename();
		if (uploadedFilename == null)
			return file.getName();
		else if (FilenameUtils.getExtension(uploadedFilename).equalsIgnoreCase(
			"mztab"))
			return uploadedFilename;
		else return String.format(
			"%s.mzTab", FilenameUtils.getBaseName(uploadedFilename));
	}
	
	public String getMangledFilename() {
		return mangledFilename;
	}
	
	public Map<Integer, PeakListFile> getPeakListFiles() {
		return new LinkedHashMap<Integer, PeakListFile>(peakListFiles);
	}
	
	public PeakListFile getPeakListFile(int msRun) {
		return peakListFiles.get(msRun);
	}
	
	/*========================================================================
	 *Convenience methods
	 *========================================================================*/
	private ImmutablePair<String, String> getResultFileMapping(
		File file, Document parameters
	) {
		if (file == null || parameters == null)
			return null;
		String filename = file.getName();
		String base = FilenameUtils.getBaseName(filename);
		// traverse "upload_file_mapping" parameters to get
		// this result file's original upload and mangled filenames
		NodeList uploads = null;
		try {
			uploads = XPathAPI.selectNodeList(
				parameters, "//parameter[@name='upload_file_mapping']");
		} catch (Throwable error) {
			
		}
		if (uploads == null || uploads.getLength() < 1)
			return null;
		for (int i=0; i<uploads.getLength(); i++) {
			String value = uploads.item(i).getFirstChild().getNodeValue();
			// each "upload_file_mapping" parameter should have
			// as its value a string with the following format:
			// <mangled_filename>|<source_filename>
			String[] tokens = value.split("\\|");
			if (tokens == null || tokens.length != 2)
				throw new IllegalArgumentException(String.format(
					"\"upload_file_mapping\" parameter value [%s] " +
					"is invalid - it should contain two tokens " +
					"separated by a pipe (\"|\") character.", value));
			if (base.equals(FilenameUtils.getBaseName(tokens[0])))
				return new ImmutablePair<String, String>(tokens[1], tokens[0]);
		}
		return null;
	}
}
