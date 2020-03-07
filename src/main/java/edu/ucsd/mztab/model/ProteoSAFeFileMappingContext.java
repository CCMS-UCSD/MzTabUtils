package edu.ucsd.mztab.model;

import java.io.File;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.xpath.XPathAPI;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import edu.ucsd.mztab.util.CommonUtils;
import edu.ucsd.mztab.util.FileIOUtils;
import edu.ucsd.mztab.util.ProteoSAFeUtils;

public class ProteoSAFeFileMappingContext
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final Pattern UPLOAD_COLLECTION_PATTERN =
		Pattern.compile("^((?:f\\.|d\\.|u\\.|t\\.).+;)+$");
	
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private Collection<UploadCollection>     uploadCollections;
	private Map<String, Map<String, String>> resultFileMappings;
	
	/*========================================================================
	 * Constructors
	 *========================================================================*/
	public ProteoSAFeFileMappingContext(File parametersFile)
	throws IOException {
		this(FileIOUtils.parseXML(parametersFile));
	}
	
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
			// NOTE: the logic implemented here assumes that the user
			// did not assign the same file to two different collections
			// OF THE SAME SIZE; in this case, the code has no way of
			// making the right call so it just picks arbitrarily
			NodeList mappings = XPathAPI.selectNodeList(
				parameters, "//parameter[@name='upload_file_mapping']");
			if (mappings != null && mappings.getLength() > 0) {
				// set up mapping parse data structures;
				// "upload_file_mapping" parameter value ->
				// mangled prefix/parsed upload file path pair
				Map<String, ImmutablePair<String, String>> uploadMappings =
					new LinkedHashMap<String, ImmutablePair<String, String>>(
						mappings.getLength());
				// mangled prefix -> number of files with that prefix
				Map<String, Integer> mangledPrefixCounts =
					new LinkedHashMap<String, Integer>(
						uploadCollections.size());
				// collection name -> upload file paths assigned to it
				Map<String, Collection<String>> collectionAssignments =
					new LinkedHashMap<String, Collection<String>>(
						uploadCollections.size());
				// upload file path -> collections it was assigned to
				Map<String, Collection<UploadCollection>> candidateCollections =
					new LinkedHashMap<String, Collection<UploadCollection>>();
				for (int i=0; i<mappings.getLength(); i++) {
					// parse this upload mapping into its parts
					String value = mappings.item(i).getFirstChild().getNodeValue();
					// each "upload_file_mapping" parameter should have
					// as its value a string with the following format:
					// <mangled_filename>|<source_filename>
					String[] uploadTokens = value.split("\\|");
					if (uploadTokens == null || uploadTokens.length != 2)
						throw new IllegalArgumentException(String.format(
							"\"upload_file_mapping\" parameter value [%s] " +
							"is invalid - it should contain two tokens " +
							"separated by a pipe (\"|\") character.", value));
					String mangledFilename = uploadTokens[0];
					String uploadFilePath = uploadTokens[1];
					// each mangled filename should be a
					// string with the following format:
					// <mangled_prefix>-<index>.<extension>
					String[] mangledTokens = mangledFilename.split("-");
					if (mangledTokens == null || mangledTokens.length < 2)
						throw new IllegalArgumentException(String.format(
							"\"upload_file_mapping\" parameter [%s] is invalid - " +
							"its mangled filename component [%s] should contain " +
							"two tokens separated by a dash (\"-\") character.",
							value, mangledFilename));
					String mangledPrefix = mangledTokens[0];
					// track parameter value -> mangled prefix/upload path pair
					uploadMappings.put(value, new ImmutablePair<String, String>(
						mangledPrefix, uploadFilePath));
					// we do not know which collection assignment this mapping refers to
					// (i.e. if the same file was added to multiple collections), so
					// increment this mangled prefix's collection size to make the call later
					Integer mangledPrefixCount = mangledPrefixCounts.get(mangledPrefix);
					if (mangledPrefixCount == null)
						mangledPrefixCount = 0;
					mangledPrefixCounts.put(mangledPrefix, ++mangledPrefixCount);
					// find candidate collections to which this mapping might belong
					for (UploadCollection collection : uploadCollections) {
						// if this file was uploaded to this collection, then this mapping
						// might represent that assignment, so associate the two
						if (collection.isFromCollection(mangledPrefix, uploadFilePath)) {
							// first associate this file with this collection
							String collectionName = collection.getParameterName();
							Collection<String> files =
								collectionAssignments.get(collectionName);
							if (files == null)
								files = new LinkedHashSet<String>();
							files.add(uploadFilePath);
							collectionAssignments.put(collectionName, files);
							// then associate this collection with this file
							Collection<UploadCollection> collections =
								candidateCollections.get(uploadFilePath);
							if (collections == null)
								collections = new LinkedHashSet<UploadCollection>();
							collections.add(collection);
							candidateCollections.put(uploadFilePath, collections);
						}
					}
				}
				// now try to reconcile each mapping to its proper collection
				// by comparing candidate collection sizes to mangled prefix counts
				for (String mapping : uploadMappings.keySet()) {
					ImmutablePair<String, String> values = uploadMappings.get(mapping);
					String mangledPrefix = values.getLeft();
					String uploadFilePath = values.getRight();
					// get this uploaded file's candidate collections
					Collection<UploadCollection> candidates =
						candidateCollections.get(uploadFilePath);
					// get the number of files in this mangled prefix's collection
					Integer collectionSize = mangledPrefixCounts.get(mangledPrefix);
					// this mapping should have both a non-empty set of candidate
					// collections and a non-zero mangled prefix collection size
					if (candidates == null)
						throw new IllegalStateException(String.format(
							"No candidate collections were found for uploaded file [%s].",
							uploadFilePath));
					else if (collectionSize == null)
						throw new IllegalStateException(String.format(
							"No file mappings were recorded for mangled prefix [%s] " +
							"associated with uploaded file [%s].",
							mangledPrefix, uploadFilePath));
					// find the first collection whose size matches the
					// number of file mappings with this mangled prefix
					boolean matchFound = false;
					for (UploadCollection collection : candidates) {
						String collectionName = collection.getParameterName();
						Collection<String> files =
							collectionAssignments.get(collectionName);
						// this collection should have a non-empty set of files
						if (files == null)
							throw new IllegalStateException(String.format(
								"No files were found associated with collection [%s].",
								collection));
						else if (files.size() == collectionSize) try {
							collection.addUploadMapping(mapping);
							matchFound = true;
							break;
						}
						// in the event that two candidate collections were found
						// whose size matches the set of files mapped to this mangled
						// prefix (i.e. the user assigned the same file to two
						// different collections of the same size), and one has
						// already been picked, then just move on to the next, and
						// hope that the arbitrary choice between the two (based
						// on the order in which collections were declared in
						// params.xml) was correct...
						catch (InvalidParameterException error) {
							continue;
						}
					}
					// if no match was found (candidate collection whose number
					// of assigned files matches the number of upload mappings
					// with this mapping's mangled prefix), then there's a problem
					if (matchFound == false)
						throw new IllegalStateException(String.format(
							"No candidate collection to which this file [%s] was " +
							"assigned could be found with the same number of files " +
							"as those mapped to this file's mangled prefix [%s].",
							uploadFilePath, mangledPrefix));
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
	
	public String toJSON() {
		// serialize this ProteoSAFeFileMappingContext object as a JSON object
		StringBuilder json = new StringBuilder("{");
		// upload collections
		json.append("\n\t\"uploadCollections\":[");
		if (uploadCollections == null || uploadCollections.isEmpty())
			json.append("],");
		else {
			for (UploadCollection collection : uploadCollections)
				json.append("\n")
					.append(CommonUtils.prettyPrint(collection.toJSON(), 2))
					.append(",");
			// chomp trailing comma
			if (json.charAt(json.length() - 1) == ',')
				json.setLength(json.length() - 1);
			json.append("\n\t],");
		}
		// result file mappings
		json.append("\n\t\"resultFileMappings\":{");
		if (resultFileMappings == null || resultFileMappings.isEmpty())
			json.append("}");
		else {
			for (String resultFilename : resultFileMappings.keySet()) {
				json.append("\n\t\t\"").append(resultFilename).append("\":[");
				Map<String, String> msRuns =
					resultFileMappings.get(resultFilename);
				if (msRuns == null || msRuns.isEmpty())
					json.append("],");
				else {
					for (String msRunLocation : msRuns.keySet())
						json.append("\n\t\t\t\"")
							.append(msRunLocation).append("\":\"")
							.append(msRuns.get(msRunLocation)).append("\",");
					// chomp trailing comma
					if (json.charAt(json.length() - 1) == ',')
						json.setLength(json.length() - 1);
					json.append("\n\t\t],");
				}
			}
			// chomp trailing comma
			if (json.charAt(json.length() - 1) == ',')
				json.setLength(json.length() - 1);
			json.append("\n\t}");
		}
		json.append("\n}");
		return json.toString();
	}
	
	@Override
	public String toString() {
		return toJSON();
	}
	
	/**
	 * Test main method to simply print out this file mapping context
	 */
	public static void main(String[] args) {
		String usage = "java -cp MassIVEUtils.jar " +
			"edu.ucsd.mztab.model.ProteoSAFeFileMappingContext " +
			"<ProteoSAFeParametersFile>";
		try {
			File paramsFile = new File(args[0]);
			ProteoSAFeFileMappingContext context =
				new ProteoSAFeFileMappingContext(paramsFile);
			System.out.println(context.toString());
		} catch (Throwable error) {
			error.printStackTrace();
			System.err.println("----------");
			System.err.println(usage);
		}
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
		
		/*====================================================================
		 * Public interface methods
		 *====================================================================*/
		public String toJSON() {
			// serialize this UploadMapping as a JSON object
			StringBuilder json = new StringBuilder("{");
			// mangled filename
			json.append("\n\t\"mangledFilename\":\"")
				.append(getMangledFilename()).append("\",");
			// upload file path
			json.append("\n\t\"uploadFilePath\":\"")
				.append(getUploadFilePath()).append("\",");
			// normalized upload file path
			json.append("\n\t\"normalizedUploadFilePath\":");
			String normalizedUploadFilePath = getNormalizedUploadFilePath();
			if (normalizedUploadFilePath == null)
				json.append("null");
			else json.append("\"").append(normalizedUploadFilePath).append("\"");
			json.append("\n}");
			return json.toString();
		}
		
		@Override
		public String toString() {
			return toJSON();
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
					String upload = uploads[i];
					String path = upload.substring(2);
					// get the file referenced by this file descriptor
					File file = null;
					if (upload.startsWith("f.") || upload.startsWith("d."))
						file = new File(ProteoSAFeUtils.USER_FILES_ROOT, path);
					else if (upload.startsWith("u.") || upload.startsWith("t."))
						file = new File(ProteoSAFeUtils.TASK_FILES_ROOT, path);
					// if no file could be found, use default logic;
					// may not work if file descriptor conventions
					// were not followed by the client
					if (file == null || file.exists() == false) {
						// user upload or task files
						if (upload.startsWith("f.") || upload.startsWith("u."))
							files.add(path);
						// user upload or task directories
						else if (upload.startsWith("d.") || upload.startsWith("t."))
							folders.add(path);
					}
					// otherwise check file to determine if it's a file or folder
					else if (file.isDirectory())
						folders.add(path);
					else files.add(path);
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
			addUploadMapping(mangledFilename, uploadFilePath);
		}
		
		public void addUploadMapping(
			String mangledFilename, String uploadFilePath
		) {
			if (mangledFilename == null || uploadFilePath == null)
				return;
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
					String.format("%s|%s", mangledFilename, uploadFilePath),
					mangledFilename));
			else if (mangledPrefix == null)
				mangledPrefix = mangledTokens[0];
			else if (mangledPrefix.equals(mangledTokens[0]) == false)
				throw new InvalidParameterException(String.format(
					"\"upload_file_mapping\" parameter [%s] is being " +
					"associated with upload collection [%s], even though " +
					"this collection has already been associated with " +
					"upload mappings having expected prefix [%s].",
					String.format("%s|%s", mangledFilename, uploadFilePath),
					parameterName, mangledPrefix));
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
			// each mangled filename should be a
			// string with the following format:
			// <mangled_prefix>-<index>.<extension>
			String[] mangledTokens = mangledFilename.split("-");
			if (mangledTokens == null || mangledTokens.length < 2)
				throw new IllegalArgumentException(String.format(
					"\"upload_file_mapping\" parameter [%s] is invalid - " +
					"its mangled filename component [%s] should contain " +
					"two tokens separated by a dash (\"-\") character.",
					uploadMapping, mangledFilename));
			return isFromCollection(mangledTokens[0], uploadFilePath);
		}
		
		public boolean isFromCollection(
			String mangledPrefix, String uploadFilePath
		) {
			if (mangledPrefix == null || uploadFilePath == null)
				return false;
			// first check to see if the mangled prefix
			// is known, and matches this one
			else if (this.mangledPrefix != null &&
				this.mangledPrefix.equals(mangledPrefix))
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
		 * Public interface methods
		 *====================================================================*/
		public String toJSON() {
			// serialize this UploadCollection as a JSON object
			StringBuilder json = new StringBuilder("{");
			// basic collection parameters
			json.append("\n\t\"parameterName\":\"")
				.append(getParameterName()).append("\",");
			json.append("\n\t\"parameterValue\":\"")
				.append(getParameterValue()).append("\",");
			json.append("\n\t\"mangledPrefix\":\"")
				.append(getMangledPrefix()).append("\",");
			// collection files
			json.append("\n\t\"files\":[");
			Collection<String> files = getFiles();
			if (files == null || files.isEmpty())
				json.append("],");
			else {
				for (String file : files)
					json.append("\n\t\t\"").append(file).append("\",");
				// chomp trailing comma
				if (json.charAt(json.length() - 1) == ',')
					json.setLength(json.length() - 1);
				json.append("\n\t],");
			}
			// collection folders
			json.append("\n\t\"folders\":[");
			Collection<String> folders = getFolders();
			if (folders == null || folders.isEmpty())
				json.append("],");
			else {
				for (String folder : folders)
					json.append("\n\t\t\"").append(folder).append("\",");
				// chomp trailing comma
				if (json.charAt(json.length() - 1) == ',')
					json.setLength(json.length() - 1);
				json.append("\n\t],");
			}
			// upload mappings
			json.append("\n\t\"uploadMappings\":[");
			Collection<UploadMapping> uploads = getUploadMappings();
			if (uploads == null || uploads.isEmpty())
				json.append("]");
			else {
				for (UploadMapping upload : uploads)
					json.append("\n")
						.append(CommonUtils.prettyPrint(upload.toJSON(), 2))
						.append(",");
				// chomp trailing comma
				if (json.charAt(json.length() - 1) == ',')
					json.setLength(json.length() - 1);
				json.append("\n\t]");
			}
			json.append("\n}");
			return json.toString();
		}
		
		@Override
		public String toString() {
			return toJSON();
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
