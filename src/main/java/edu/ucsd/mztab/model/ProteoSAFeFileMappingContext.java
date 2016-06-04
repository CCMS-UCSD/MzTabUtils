package edu.ucsd.mztab.model;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.xpath.XPathAPI;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

public class ProteoSAFeFileMappingContext
{
	/*========================================================================
	 * Properties
	 *========================================================================*/
	Map<String, String>              uploadFileMappings;
	Map<String, Map<String, String>> resultFileMappings;
	
	/*========================================================================
	 * Constructor
	 *========================================================================*/
	public ProteoSAFeFileMappingContext(Document parameters) {
		// validate parameters document
		if (parameters == null)
			throw new NullPointerException(
				"Argument params.xml document cannot be null.");
		// initialize file mapping data structures
		uploadFileMappings = new LinkedHashMap<String, String>();
		resultFileMappings = new LinkedHashMap<String, Map<String, String>>();
		try {
			// populate upload mappings
			NodeList mappings = XPathAPI.selectNodeList(
				parameters, "//parameter[@name='upload_file_mapping']");
			if (mappings != null && mappings.getLength() > 0) {
				for (int i=0; i<mappings.getLength(); i++) {
					String value =
						mappings.item(i).getFirstChild().getNodeValue();
					// each "upload_file_mapping" parameter should have
					// as its value a string with the following format:
					// <mangled_filename>|<source_filename>
					String[] tokens = value.split("\\|");
					if (tokens == null || tokens.length != 2)
						throw new IllegalArgumentException(String.format(
							"\"upload_file_mapping\" parameter value [%s] " +
							"is invalid - it should contain two tokens " +
							"separated by a pipe (\"|\") character.", value));
					// add this upload mapping to the context
					uploadFileMappings.put(tokens[0], tokens[1]);
				}
			}
			// populate result file mappings
			mappings = XPathAPI.selectNodeList(
				parameters, "//parameter[@name='result_file_mapping']");
			if (mappings != null && mappings.getLength() > 0) {
				for (int i=0; i<mappings.getLength(); i++) {
					String value =
						mappings.item(i).getFirstChild().getNodeValue();
					// each "result_file_mapping" parameter should have
					// as its value a string with the following format:
					// <result_file>#<referenced_filename>|<source_filename>
					String[] tokens = value.split("\\|");
					if (tokens == null || tokens.length != 2)
						throw new IllegalArgumentException(String.format(
							"\"result_file_mapping\" parameter value [%s] " +
							"is invalid - it should contain two tokens " +
							"separated by a pipe (\"|\") character.", value));
					// split the mapped value to extract the referenced filename
					String[] mapped = tokens[0].split("#");
					if (mapped == null || mapped.length != 2)
						throw new IllegalArgumentException(String.format(
							"\"result_file_mapping\" parameter value [%s] " +
							"is invalid - its first token ([%s]) should " +
							"consist of two values separated by a hash " +
							"(\"#\") character.", value, tokens[0]));
					// add this result file mapping to the context
					Map<String, String> msRunMappings =
						resultFileMappings.get(mapped[0]);
					if (msRunMappings == null)
						msRunMappings = new LinkedHashMap<String, String>();
					msRunMappings.put(mapped[1], tokens[1]);
					resultFileMappings.put(mapped[0], msRunMappings);
				}
			}
		} catch (Throwable error) {
			throw new IllegalArgumentException(
				"There was an error extracting file mappings from params.xml",
				error);
		}
	}
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public Map<String, String> getUploadFileMappings() {
		return new LinkedHashMap<String, String>(uploadFileMappings);
	}
	
	public Collection<String> getMangledFilenames() {
		return uploadFileMappings.keySet();
	}
	
	public String getUploadedFilePath(String mangledFilename) {
		if (mangledFilename == null)
			return null;
		else return uploadFileMappings.get(mangledFilename);
	}
	
	public Map<String, Map<String, String>> getResultFileMappings() {
		return new LinkedHashMap<String, Map<String, String>>(
			resultFileMappings);
	}
	
	public Collection<String> getResultFilenames() {
		return resultFileMappings.keySet();
	}
	
	public Map<String, String> getResultFileMsRunMappings(
		String resultFilename
	) {
		if (resultFilename == null)
			return null;
		Map<String, String> msRuns = resultFileMappings.get(resultFilename);
		if (msRuns == null)
			return null;
		else return new LinkedHashMap<String, String>(msRuns);
	}
	
	public Collection<String> getResultFileMsRunLocations(
		String resultFilename
	) {
		if (resultFilename == null)
			return null;
		Map<String, String> msRuns = resultFileMappings.get(resultFilename);
		if (msRuns == null)
			return null;
		else return msRuns.keySet();
	}
	
	public String getResultFileMsRunMapping(
		String resultFilename, String msRunLocation
	) {
		Map<String, String> msRuns = resultFileMappings.get(resultFilename);
		if (msRuns == null)
			return null;
		else return msRuns.get(msRunLocation);
	}
}
