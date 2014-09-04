package edu.ucsd.mztab;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.xpath.XPathAPI;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import edu.ucsd.util.FileIOUtils;
import edu.ucsd.util.FilePathComparator;

public class MassIVEMzTabContext
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final FilePathComparator FILE_PATH_COMPARATOR =
		new FilePathComparator();
	
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private File params;
	private Collection<File> mzTabFiles;
	private Collection<File> scansFiles;
	private Map<String, Collection<PeakListFileMapping>> filenameMap;
	
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
			output.append("\n\t\"").append(resultFilename).append("\":[");
			Collection<PeakListFileMapping> resultFileMap =
				filenameMap.get(resultFilename);
			for (PeakListFileMapping mapping : resultFileMap) {
				output.append("\n\t\t{\"uploadedMzTabFilename\":\"").append(
					mapping.uploadedMzTabFilename).append("\",");
				output.append("\"mangledMzTabFilename\":\"").append(
					mapping.mangledMzTabFilename).append("\",");
				output.append("\"uploadedPeakListFilename\":\"").append(
					mapping.uploadedPeakListFilename).append("\",");
				output.append("\"mangledPeakListFilename\":\"").append(
					mapping.mangledPeakListFilename).append("\",");
				output.append("\"mzTabPeakListFilename\":\"").append(
					mapping.mzTabPeakListFilename).append("\"},");
			}
			// chomp trailing comma
			if (output.charAt(output.length() - 1) == ',')
				output.setLength(output.length() - 1);
			output.append("],");
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
		// traverse "result_file_mapping" parameters to build mapping collection
		try {
			NodeList mappings = XPathAPI.selectNodeList(
				document, "//parameter[@name='result_file_mapping']");
			if (mappings == null || mappings.getLength() < 1) {
				filenameMap = new LinkedHashMap<String,
					Collection<PeakListFileMapping>>();
				return;
			} else filenameMap =
				new LinkedHashMap<String, Collection<PeakListFileMapping>>(
					mappings.getLength());
			// also keep track of all the original uploaded peak list
			// filenames, as recorded in "result_file_mapping" parameters
			Map<String, Collection<PeakListFileMapping>>
				uploadedPeakListFilenames =
					new LinkedHashMap<String, Collection<PeakListFileMapping>>(
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
				// build this mapping
				PeakListFileMapping mapping = new PeakListFileMapping(
					mapped[0], mapped[1], tokens[1]);
				// add it to the collection for this peak list file
				Collection<PeakListFileMapping> peakListMappings =
					uploadedPeakListFilenames.get(tokens[1]);
				if (peakListMappings == null)
					peakListMappings = new ArrayList<PeakListFileMapping>();
				peakListMappings.add(mapping);
				uploadedPeakListFilenames.put(tokens[1], peakListMappings);
				// add it to the collection for this result file
				Collection<PeakListFileMapping> resultFileMap =
					filenameMap.get(mapped[0]);
				if (resultFileMap == null)
					resultFileMap = new ArrayList<PeakListFileMapping>();
				resultFileMap.add(mapping);
				filenameMap.put(mapped[0], resultFileMap);
			}
			// then traverse "upload_file_mapping" parameters
			// to mangle result and peak list filenames
			mappings = XPathAPI.selectNodeList(
				document, "//parameter[@name='upload_file_mapping']");
			if (mappings == null || mappings.getLength() < 1)
				return;
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
				if (value.startsWith("PEAK-")) {
					// find the mapping that best fits this source
					// filename from the peak list source filename map
					String bestMatch = FILE_PATH_COMPARATOR.findBestFileMatch(
						tokens[1], uploadedPeakListFilenames.keySet());
					if (bestMatch == null)
						throw new NullPointerException(String.format(
							"No source peak list filename could be found " +
							"from among the \"result_file_mapping\" " +
							"parameters to match source filename [%s], " +
							"from \"upload_file_mapping\" parameter value [%s]",
							tokens[1], value));
					// update all of this peak list file's mapping
					// with the mangled filename
					Collection<PeakListFileMapping> peakListMappings =
						uploadedPeakListFilenames.get(bestMatch);
					for (PeakListFileMapping mapping : peakListMappings)
						mapping.mangledPeakListFilename = tokens[0];
				} else if (value.startsWith("RESULT-")) {
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
					String bestMatch = FILE_PATH_COMPARATOR.findBestFileMatch(
						tokens[1], filenameMap.keySet());
					if (bestMatch == null)
						throw new NullPointerException(String.format(
							"No source result filename could be found " +
							"from among the \"result_file_mapping\" " +
							"parameters to match source filename [%s], " +
							"from \"upload_file_mapping\" parameter value [%s]",
							tokens[1], value));
					// update all of this mzTab file's mappings
					// with the mangled filename
					Collection<PeakListFileMapping> mzTabMappings =
						filenameMap.get(bestMatch);
					for (PeakListFileMapping mapping : mzTabMappings)
						mapping.mangledMzTabFilename = mangledFilename;
				}
			}
		} catch (Throwable error) {
			String errorMessage = "There was an error parsing params.xml to " +
				"build the user-specified filename map.";
			System.err.println(errorMessage);
			error.printStackTrace();
			throw new RuntimeException(errorMessage, error);
		}
	}
	
	/*========================================================================
	 * Convenience classes
	 *========================================================================*/
	/**
	 * Struct to maintain context data for a particular mapping of peak list
	 * file referenced within an mzTab file -> uploaded dataset peak list file.
	 */
	private static class PeakListFileMapping {
		/*====================================================================
		 * Properties
		 *====================================================================*/
		private String uploadedMzTabFilename;
		private String mangledMzTabFilename;
		private String mzTabPeakListFilename;
		private String uploadedPeakListFilename;
		private String mangledPeakListFilename;
		
		/*====================================================================
		 * Constructor
		 *====================================================================*/
		public PeakListFileMapping(
			String uploadedMzTabFilename, String mzTabPeakListFilename,
			String uploadedPeakListFilename
		) {
			if (uploadedMzTabFilename == null)
				throw new NullPointerException(
					"Uploaded mzTab filename cannot be null.");
			else this.uploadedMzTabFilename = uploadedMzTabFilename;
			if (mzTabPeakListFilename == null)
				throw new NullPointerException(
					"MzTab peak list filename cannot be null.");
			else this.mzTabPeakListFilename = mzTabPeakListFilename;
			if (uploadedPeakListFilename == null)
				throw new NullPointerException(
					"Uploaded peak list filename cannot be null.");
			else this.uploadedPeakListFilename = uploadedPeakListFilename;
		}
	}
}
