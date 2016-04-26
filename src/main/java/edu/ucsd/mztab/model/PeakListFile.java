package edu.ucsd.mztab.model;

import java.io.File;
import java.net.URI;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.xpath.XPathAPI;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

public class PeakListFile
{
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private String msRunLocation;
	private String uploadedPath;
	private String mangledFilename;
	
	/*========================================================================
	 * Constructors
	 *========================================================================*/
	public PeakListFile(String msRunLocation, Document parameters) {
		// validate input ms_run location string
		if (msRunLocation == null)
			throw new NullPointerException(
				"Argument \"ms_run[1-n]-location\" value cannot be null.");
		else this.msRunLocation = msRunLocation;
		// validate argument params.xml document
		if (parameters == null)
			throw new NullPointerException(
				"Argument params.xml document cannot be null.");
		// read parameters to extract the upload file mapping
		// for this peak list file, if there is one
//		ImmutablePair<String, String> mapping =
//			getPeakListFileMapping(msRunLocation, parameters);
//		if (mapping != null) {
//			uploadedPath = mapping.getLeft();
//			mangledFilename = mapping.getRight();
//		} else {
//			uploadedPath = null;
//			mangledFilename = null;
//		}
	}
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public String getMsRunLocation() {
		return msRunLocation;
	}
	
	public String getUploadedPath() {
		return uploadedPath;
	}
	
	public String getUploadedFilename() {
		if (uploadedPath == null)
			return null;
		else return FilenameUtils.getName(uploadedPath);
	}
	
	public String getMangledFilename() {
		return mangledFilename;
	}
	
	/*========================================================================
	 *Convenience methods
	 *========================================================================*/
	private ImmutablePair<String, String> getPeakListFileMapping(
		String msRunLocation, Document parameters
	) {
		if (msRunLocation == null || parameters == null)
			return null;
		String filename = FilenameUtils.getName(msRunLocation);
		String base = FilenameUtils.getBaseName(filename);
		// traverse "upload_file_mapping" parameters to get
		// this peak list file's original upload and mangled filenames
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
