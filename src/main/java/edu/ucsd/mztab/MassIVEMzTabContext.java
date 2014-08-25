package edu.ucsd.mztab;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.xpath.XPathAPI;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import edu.ucsd.util.FileIOUtils;

public class MassIVEMzTabContext
{
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private File params;
	private Collection<File> mzTabFiles;
	private Collection<File> scansFiles;
	private Map<String, Map<String, String>> filenameMap;
	
	/*========================================================================
	 * Constructors
	 *========================================================================*/
	public MassIVEMzTabContext(
		File params, File mzTabDirectory, File scansDirectory
	) {
		// validate argument params.xml file
		if (params == null)
			throw new NullPointerException(
				"Argument params.xml file cannot be null.");
		else if (params.isFile() == false || params.canRead() == false)
			throw new IllegalArgumentException(
				"Argument params.xml file must be a readable file.");
		else this.params = params;
		// validate argument mzTab directory
		if (mzTabDirectory == null)
			throw new NullPointerException(
				"Argument mzTab files directory cannot be null.");
		else if (mzTabDirectory.isDirectory() == false ||
			mzTabDirectory.canRead() == false)
			throw new IllegalArgumentException(
				"Argument mzTab files directory must be a readable directory.");
		// collect files in argument mzTab directory
		File[] files = mzTabDirectory.listFiles();
		if (files == null || files.length < 1)
			mzTabFiles = new LinkedHashSet<File>();
		else {
			mzTabFiles = new LinkedHashSet<File>(files.length);
			for (File file : files) {
				// validate mzTab file
				if (file == null)
					throw new NullPointerException(
						"Argument mzTab file cannot be null.");
				else if (file.isFile() == false || file.canRead() == false)
					throw new IllegalArgumentException(String.format(
						"Argument mzTab file [%s] is not a readable file.",
						file.getAbsolutePath()));
				else mzTabFiles.add(file);
			}
		}
		// validate argument scans directory
		if (scansDirectory == null)
			throw new NullPointerException(
				"Argument spectrum ID files directory cannot be null.");
		else if (scansDirectory.isDirectory() == false ||
			scansDirectory.canRead() == false)
			throw new IllegalArgumentException("Argument spectrum ID files " +
				"directory must be a readable directory.");
		// collect files in argument scans directory
		files = scansDirectory.listFiles();
		if (files == null || files.length < 1)
			scansFiles = new LinkedHashSet<File>();
		else {
			scansFiles = new LinkedHashSet<File>(files.length);
			for (File file : files) {
				// validate scans file
				if (file == null)
					throw new NullPointerException(
						"Argument scans file cannot be null.");
				else if (file.isFile() == false || file.canRead() == false)
					throw new IllegalArgumentException(String.format(
						"Argument scans file [%s] is not a readable file.",
						file.getAbsolutePath()));
				else scansFiles.add(file);
			}
		}
		// build filename map
		try {
			buildFilenameMap();
		} catch (Throwable error) {
			throw new RuntimeException(error);
		}
	}
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public String toJSON() {
		if (filenameMap == null)
			return null;
		StringBuffer output = new StringBuffer("{");
		for (String resultFilename : filenameMap.keySet()) {
			output.append("\n\t\"").append(resultFilename).append("\":{");
			Map<String, String> resultFileMapping =
				filenameMap.get(resultFilename);
			for (String referencedFilename : resultFileMapping.keySet()) {
				output.append("\"").append(referencedFilename).append("\":");
				output.append("\"").append(
					resultFileMapping.get(referencedFilename)).append("\",");
			}
			// chomp trailing comma
			if (output.charAt(output.length() - 1) == ',')
				output.setLength(output.length() - 1);
			output.append("},");
		}
		// chomp trailing comma
		if (output.charAt(output.length() - 1) == ',')
			output.setLength(output.length() - 1);
		// add a newline if any entries were printed
		if (output.length() > 1)
			output.append("\n");
		// close the JSON hash and return it
		output.append("}");
		return output.toString();
	}
	
	@Override
	public String toString() {
		return toJSON();
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private void buildFilenameMap()
	throws IOException {
		// parse params.xml file into a DOM object
		Document document = FileIOUtils.parseXML(params);
		if (document == null)
			throw new NullPointerException(
				"Argument params.xml file could not be parsed.");
		// traverse "result_file_mapping" parameters to build first-level map:
		// result filename ->
		// {referenced spectrum filename -> uploaded spectrum filename}
		try {
			NodeList mappings = XPathAPI.selectNodeList(
				document, "//parameter[@name='result_file_mapping']");
			if (mappings == null || mappings.getLength() < 1) {
				filenameMap = new LinkedHashMap<String, Map<String, String>>();
				return;
			} else filenameMap =
				new LinkedHashMap<String, Map<String, String>>(
					mappings.getLength());
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
				// built a one-element map for this mapping
				Map<String, String> mapping = new HashMap<String, String>(1);
				mapping.put(mapped[1], tokens[1]);
				filenameMap.put(mapped[0], mapping);
			}
			// then traverse "upload_file_mapping" parameters to mangle
			// result filenames, and build a peak list filename map:
			// uploaded spectrum filename -> mangled spectrum filename
			mappings = XPathAPI.selectNodeList(
				document, "//parameter[@name='upload_file_mapping']");
			if (mappings == null || mappings.getLength() < 1) {
				filenameMap = new LinkedHashMap<String, Map<String, String>>();
				return;
			}
			Map<String, String> peakListFileMap =
				new HashMap<String, String>(mappings.getLength());
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
				if (value.startsWith("PEAK-"))
					peakListFileMap.put(tokens[1], tokens[0]);
				else if (value.startsWith("RESULT-")) {
					// convert mangled filename to mzTab, if necessary,
					// since that's what the workflow did
					String mangledFilename = tokens[0];
					String extension =
						FilenameUtils.getExtension(mangledFilename);
					if (extension.equalsIgnoreCase("mztab") == false)
						mangledFilename =
							FilenameUtils.getBaseName(mangledFilename) +
							".mzTab";
					// find the mapping that best fits this source
					// filename from the first-level map
					int bestMatchLength = 0;
					String bestMatch = null;
					for (String resultFilename : filenameMap.keySet()) {
						if (tokens[1].endsWith(resultFilename) &&
							resultFilename.length() > bestMatchLength) {
							bestMatch = resultFilename;
							bestMatchLength = resultFilename.length();
						}
					}
					if (bestMatch == null) {
						System.out.println(String.format("Warning: no " +
							"referenced spectrum filename mappings were " +
							"found for result file [%s]", tokens[1]));
						continue;
					}
					// replace the mapping in the main map
					// with the mangled filename key
					Map<String, String> mapping = filenameMap.get(bestMatch);
					filenameMap.put(mangledFilename, mapping);
					filenameMap.remove(bestMatch);
				}
			}
			// finally, examine each leaf mapping and replace its value
			// with the best matching mangled peak list filename
			for (String resultFilename : filenameMap.keySet()) {
				Map<String, String> resultFileMapping =
					filenameMap.get(resultFilename);
				for (String referencedFilename : resultFileMapping.keySet()) {
					String leafMapping =
						resultFileMapping.get(referencedFilename);
					// find the mapping that best fits this source
					// filename from the peak list file map
					int bestMatchLength = 0;
					String bestMatch = null;
					for (String mangledFilename : peakListFileMap.keySet()) {
						String peakListFilename =
							peakListFileMap.get(mangledFilename);
						if (peakListFilename.endsWith(leafMapping) &&
							leafMapping.length() > bestMatchLength) {
							bestMatch = mangledFilename;
							bestMatchLength = leafMapping.length();
						}
					}
					if (bestMatch == null)
						throw new IllegalArgumentException(String.format(
							"\"result_file_mapping\" parameter value [%s] " +
							"is invalid - no associated " +
							"\"upload_file_mapping\" parameter could be " +
							"found for mapped peak list file [%s]",
							resultFilename + "#" + referencedFilename + "|" +
							leafMapping, leafMapping));
					// replace the leaf mapping with the
					// mangled peak list filename
					resultFileMapping.put(referencedFilename, bestMatch);
				}
				filenameMap.put(resultFilename, resultFileMapping);
			}
		} catch (Throwable error) {
			String errorMessage = "There was an error parsing params.xml to " +
				"build the user-specified filename map.";
			System.err.println(errorMessage);
			error.printStackTrace();
			throw new RuntimeException(errorMessage, error);
		}
	}
}
