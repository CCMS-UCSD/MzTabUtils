package edu.ucsd.mztab.model;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.xpath.XPathAPI;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

public class ProteoSAFeFileMappingContext
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final Pattern UPLOAD_COLLECTION_PATTERN =
		Pattern.compile("^((?:f\\.|d\\.).+;)+$");
	
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private Collection<UploadCollection>     uploadCollections;
	private Map<String, Map<String, String>> resultFileMappings;
	
	/*========================================================================
	 * Constructor
	 *========================================================================*/
	public ProteoSAFeFileMappingContext(Document parameters) {
		// validate parameters document
		if (parameters == null)
			throw new NullPointerException(
				"Argument params.xml document cannot be null.");
		// initialize file mapping data structures
		uploadCollections = new LinkedHashSet<UploadCollection>();
		resultFileMappings = new LinkedHashMap<String, Map<String, String>>();
		try {
			// populate all upload collection parameters
			NodeList params =
				XPathAPI.selectNodeList(parameters, "//parameter");
			if (params != null && params.getLength() > 0) {
				for (int i=0; i<params.getLength(); i++) {
					String value =
						params.item(i).getFirstChild().getNodeValue();
					if (UPLOAD_COLLECTION_PATTERN.matcher(value).matches()) {
						String name = params.item(i).getAttributes()
							.getNamedItem("name").getNodeValue();
						uploadCollections.add(
							new UploadCollection(name, value));
					}
				}
			}
			// populate upload mappings
			NodeList mappings = XPathAPI.selectNodeList(
				parameters, "//parameter[@name='upload_file_mapping']");
			if (mappings != null && mappings.getLength() > 0) {
				for (int i=0; i<mappings.getLength(); i++) {
					String value =
						mappings.item(i).getFirstChild().getNodeValue();
					// find proper collection to which to add this mapping
					for (UploadCollection collection : uploadCollections) {
						if (collection.isFromCollection(value)) {
							collection.addUploadMapping(value);
							break;
						}
					}
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
					String[] mapped = splitResultFileReference(tokens[0]);
					if (mapped == null || mapped.length != 2)
						throw new IllegalArgumentException(String.format(
							"\"result_file_mapping\" parameter value [%s] " +
							"is invalid - its first token ([%s]) should " +
							"consist of two values separated by a hash " +
							"(\"%s\") character.", value, tokens[0],
							MzTabConstants.EXTRACTED_FILE_DELIMITER));
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
	public Collection<UploadMapping> getUploadMappings() {
		Collection<UploadMapping> mappings = new LinkedHashSet<UploadMapping>();
		for (UploadCollection collection : uploadCollections)
			mappings.addAll(collection.getUploadMappings());
		return mappings;
	}
	
	public Collection<UploadMapping> getUploadMappings(
		String collectionParameter
	) {
		if (collectionParameter == null)
			return null;
		Collection<UploadMapping> mappings = new LinkedHashSet<UploadMapping>();
		for (UploadCollection collection : uploadCollections)
			if (collection.getParameterName().equals(collectionParameter))
				mappings.addAll(collection.getUploadMappings());
		return mappings;
	}
	
	public Collection<String> getMangledFilenames() {
		Collection<String> mangledFilenames = new LinkedHashSet<String>();
		Collection<UploadMapping> mappings = getUploadMappings();
		for (UploadMapping mapping : mappings)
			mangledFilenames.add(mapping.getMangledFilename());
		return mangledFilenames;
	}
	
	public String getMangledFilename(
		String filePathInCollection, String prefix
	) {
		if (filePathInCollection == null)
			return null;
		for (UploadCollection collection : uploadCollections) {
			for (UploadMapping mapping : collection.getUploadMappings()) {
				if (filePathInCollection.endsWith(
					mapping.getNormalizedUploadFilePath())) {
					String mangledFilename = mapping.getMangledFilename();
					// ensure this file is from the proper
					// collection, if specified
					if (prefix == null ||
						prefix.equals(collection.getMangledPrefix()))
						return mangledFilename;
				}
			}
		}
		return null;
	}
	
	public String getUploadFilePath(String mangledFilename) {
		if (mangledFilename == null)
			return null;
		Collection<UploadMapping> mappings = getUploadMappings();
		for (UploadMapping mapping : mappings)
			if (mangledFilename.equals(mapping.getMangledFilename()))
				return mapping.getUploadFilePath();
		return null;
	}
	
	public String getNormalizedUploadFilePath(String mangledFilename) {
		if (mangledFilename == null)
			return null;
		Collection<UploadMapping> mappings = getUploadMappings();
		for (UploadMapping mapping : mappings)
			if (mangledFilename.equals(mapping.getMangledFilename()))
				return mapping.getNormalizedUploadFilePath();
		return null;
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
	
	public static String[] splitResultFileReference(String reference) {
		if (reference == null)
			return null;
		Matcher matcher =
			MzTabConstants.EXTRACTED_FILE_DELIMITER_PATTERN.matcher(reference);
		if (matcher.find() == false)
			return null;
		String delimiter = matcher.group();
		int position = reference.indexOf(delimiter) + delimiter.length();
		String[] tokens = new String[2];
		tokens[0] = reference.substring(0, position - 1);
		tokens[1] = reference.substring(position);
		return tokens;
	}
	
	/*========================================================================
	 * Convenience classes
	 *========================================================================*/
	/**
	 * Struct to maintain context data for each "upload_file_mapping"
	 * parameter in params.xml.
	 */
	public static class UploadMapping {
		/*====================================================================
		 * Properties
		 *====================================================================*/
		private String mangledFilename;
		private String uploadFilePath;
		private String normalizedUploadFilePath;
		
		/*====================================================================
		 * Constructor
		 *====================================================================*/
		public UploadMapping(String mangledFilename, String uploadFilePath) {
			if (mangledFilename == null)
				throw new NullPointerException(
					"Mangled filename cannot be null.");
			else this.mangledFilename = mangledFilename;
			if (uploadFilePath == null)
				throw new NullPointerException(
					"Upload file path cannot be null.");
			else this.uploadFilePath = uploadFilePath;
			normalizedUploadFilePath = null;
		}
		
		/*====================================================================
		 * Property accessor methods
		 *====================================================================*/
		public String getMangledFilename() {
			return mangledFilename;
		}
		
		public String getUploadFilePath() {
			return uploadFilePath;
		}
		
		public String getNormalizedUploadFilePath() {
			return normalizedUploadFilePath;
		}
		
		public void setNormalizedUploadFilePath(
			String normalizedUploadFilePath
		) {
			this.normalizedUploadFilePath = normalizedUploadFilePath;
		}
	}
	
	/**
	 * Struct to maintain context data for each upload
	 * collection parameter in params.xml.
	 */
	public static class UploadCollection {
		/*====================================================================
		 * Properties
		 *====================================================================*/
		private String                    parameterName;
		private String                    parameterValue;
		private String                    mangledPrefix;
		private Collection<String>        files;
		private Collection<String>        folders;
		private Collection<UploadMapping> uploads;
		
		/*====================================================================
		 * Constructor
		 *====================================================================*/
		public UploadCollection(String parameterName, String parameterValue) {
			// validate parameter name and value
			if (parameterName == null)
				throw new NullPointerException(
					"Upload collection parameter name cannot be null.");
			else this.parameterName = parameterName;
			if (parameterValue == null)
				throw new NullPointerException(
					"Upload collection parameter value cannot be null.");
			else this.parameterValue = parameterValue;
			// parse parameter value to extract file and folder relative paths
			files = new LinkedHashSet<String>();
			folders = new LinkedHashSet<String>();
			String[] uploads = parameterValue.split(";");
			if (uploads != null && uploads.length > 0) {
				for (int i=0; i<uploads.length; i++) {
					if (uploads[i].startsWith("f."))
						files.add(uploads[i].substring(2));
					else if (uploads[i].startsWith("d."))
						folders.add(uploads[i].substring(2));
				}
			}
			// initialize other properties
			mangledPrefix = null;
			this.uploads = new LinkedHashSet<UploadMapping>();
		}
		
		/*====================================================================
		 * Property accessor methods
		 *====================================================================*/
		public String getParameterName() {
			return parameterName;
		}
		
		public String getParameterValue() {
			return parameterValue;
		}
		
		public String getMangledPrefix() {
			return mangledPrefix;
		}
		
		public Collection<String> getFiles() {
			return new LinkedHashSet<String>(files);
		}
		
		public Collection<String> getFolders() {
			return new LinkedHashSet<String>(folders);
		}

		public Collection<UploadMapping> getUploadMappings() {
			return new LinkedHashSet<UploadMapping>(uploads);
		}
		
		public void addUploadMapping(String uploadMapping) {
			if (uploadMapping == null)
				return;
			// each "upload_file_mapping" parameter should have
			// as its value a string with the following format:
			// <mangled_filename>|<source_filename>
			String[] uploadTokens = uploadMapping.split("\\|");
			if (uploadTokens == null || uploadTokens.length != 2)
				throw new IllegalArgumentException(String.format(
					"\"upload_file_mapping\" parameter value [%s] " +
					"is invalid - it should contain two tokens " +
					"separated by a pipe (\"|\") character.", uploadMapping));
			String mangledFilename = uploadTokens[0];
			String uploadFilePath = uploadTokens[1];
			// instantiate this upload mapping
			UploadMapping mapping =
				new UploadMapping(mangledFilename, uploadFilePath);
			// note the mangled filename prefix and ensure it's consistent
			String[] mangledTokens = mangledFilename.split("-");
			if (mangledTokens == null || mangledTokens.length < 2)
				throw new IllegalArgumentException(String.format(
					"\"upload_file_mapping\" parameter [%s] is invalid - " +
					"its mangled filename component [%s] should contain " +
					"two tokens separated by a dash (\"-\") character.",
					uploadMapping, mangledFilename));
			else if (mangledPrefix == null)
				mangledPrefix = mangledTokens[0];
			else if (mangledPrefix.equals(mangledTokens[0]) == false)
				throw new IllegalArgumentException(String.format(
					"\"upload_file_mapping\" parameter [%s] is being " +
					"associated with upload collection [%s], even though " +
					"this collection has already been associated with " +
					"upload mappings having expected prefix [%s].",
					uploadMapping, parameterName, mangledPrefix));
			// determine this upload mapping's normalized upload path,
			// if it's part of a folder selected for upload by the user
			for (String folder : folders) {
				if (isInFolder(uploadFilePath, folder)) {
					String normalizedPath =
						uploadFilePath.substring(folder.length());
					normalizedPath = String.format("%s%s",
						FilenameUtils.getBaseName(folder), normalizedPath);
					mapping.setNormalizedUploadFilePath(normalizedPath);
					break;
				}
			}
			// if it's not part of any user-selected folder, then this
			// mapping's normalized upload path is just the filename
			if (mapping.getNormalizedUploadFilePath() == null)
				mapping.setNormalizedUploadFilePath(
					FilenameUtils.getName(uploadFilePath));
			// finally, record the verified upload mapping
			uploads.add(mapping);
		}
		
		public boolean isFromCollection(String uploadMapping) {
			if (uploadMapping == null)
				return false;
			// each "upload_file_mapping" parameter should have
			// as its value a string with the following format:
			// <mangled_filename>|<source_filename>
			String[] uploadTokens = uploadMapping.split("\\|");
			if (uploadTokens == null || uploadTokens.length != 2)
				throw new IllegalArgumentException(String.format(
					"\"upload_file_mapping\" parameter value [%s] " +
					"is invalid - it should contain two tokens " +
					"separated by a pipe (\"|\") character.", uploadMapping));
			String mangledFilename = uploadTokens[0];
			String uploadFilePath = uploadTokens[1];
			// first check to see if the mangled prefix
			// is known, and matches this one
			String[] mangledTokens = mangledFilename.split("-");
			if (mangledTokens == null || mangledTokens.length < 2)
				throw new IllegalArgumentException(String.format(
					"\"upload_file_mapping\" parameter [%s] is invalid - " +
					"its mangled filename component [%s] should contain " +
					"two tokens separated by a dash (\"-\") character.",
					uploadMapping, mangledFilename));
			else if (mangledPrefix != null &&
				mangledPrefix.equals(mangledTokens[0]))
				return true;
			// otherwise, check to see if this upload is any of
			// the single files selected for this collection
			for (String file : files)
				if (uploadFilePath.equals(file))
					return true;
			// finally, check this collection's folders
			// to see if this upload is from any of them
			for (String folder : folders)
				if (isInFolder(uploadFilePath, folder))
					return true;
			return false;
		}
		
		/*====================================================================
		 * Convenience methods
		 *====================================================================*/
		private boolean isInFolder(String file, String folder) {
			if (file == null || folder == null)
				return false;
			// normalize file path separators to ensure good string matching
			file = FilenameUtils.separatorsToUnix(file);
			folder = FilenameUtils.separatorsToUnix(folder);
			// ensure folder path ends with a slash, so we won't
			// match wrong folders with a common prefix
			if (folder.endsWith("/") == false)
				folder = String.format("%s/", folder);
			// if file path starts with folder path,
			// then the file is under the folder
			return file.startsWith(folder);
		}
	}
}
