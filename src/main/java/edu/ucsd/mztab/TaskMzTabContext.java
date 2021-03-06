package edu.ucsd.mztab;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.xpath.XPathAPI;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import edu.ucsd.mztab.model.MzTabFile;
import edu.ucsd.mztab.model.MzTabMsRun;
import edu.ucsd.mztab.model.ProteoSAFeFileMappingContext;
import edu.ucsd.mztab.model.ProteoSAFeFileMappingContext.UploadMapping;
import edu.ucsd.mztab.util.FileIOUtils;
import edu.ucsd.mztab.util.ProteoSAFeUtils;

public class TaskMzTabContext
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String DEFAULT_PEAK_LIST_COLLECTION =
		"peak_list_files";
	
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private Collection<MzTabFile>        mzTabs;
	private ProteoSAFeFileMappingContext mappings;
	private String                       peakListCollection;
	
	/*========================================================================
	 * Constructors
	 *========================================================================*/
	public TaskMzTabContext(
		File mzTabDirectory, String mzTabRelativePath, File parametersFile
	) {
		this(mzTabDirectory, mzTabRelativePath,
			null, null, null, parametersFile);
	}
	
	public TaskMzTabContext(
		File mzTabDirectory, String mzTabRelativePath,
		File peakListDirectory, String peakListRelativePath,
		String peakListCollection, File parametersFile
	) {
		this(mzTabDirectory, mzTabRelativePath,
			peakListDirectory, peakListRelativePath, peakListCollection,
			parametersFile, null);
	}
	
	public TaskMzTabContext(
		File mzTabDirectory, String mzTabRelativePath,
		File peakListDirectory, String peakListRelativePath,
		String peakListCollection, File parametersFile, String datasetID
	) {
		// validate mzTab directory
		if (mzTabDirectory == null)
			throw new NullPointerException(
				"MzTab directory cannot be null.");
		else if (mzTabDirectory.isDirectory() == false)
			throw new IllegalArgumentException(
				String.format("MzTab directory [%s] must be a directory.",
					mzTabDirectory.getAbsolutePath()));
		else if (mzTabDirectory.canRead() == false)
			throw new IllegalArgumentException(
				String.format("MzTab directory [%s] must be readable.",
					mzTabDirectory.getAbsolutePath()));
		// validate peak list files directory (can be null)
		if (peakListDirectory != null) {
			if (peakListDirectory.isDirectory() == false)
				throw new IllegalArgumentException(String.format(
					"Peak list files directory [%s] must be a directory.",
					peakListDirectory.getAbsolutePath()));
			else if (peakListDirectory.canRead() == false)
				throw new IllegalArgumentException(String.format(
					"Peak list files directory [%s] must be readable.",
					peakListDirectory.getAbsolutePath()));
		}
		// determine peak list collection name (default "peak_list_files")
		if (peakListCollection == null)
			this.peakListCollection = DEFAULT_PEAK_LIST_COLLECTION;
		else this.peakListCollection = peakListCollection;
		// validate params.xml file
		if (parametersFile == null)
			throw new NullPointerException(
				"Argument params.xml file cannot be null.");
		else if (parametersFile.isFile() == false ||
				parametersFile.canRead() == false)
			throw new IllegalArgumentException(
				"Argument params.xml file must be a readable file.");
		// parse params.xml file into a DOM object
		Document parameters = null;
		try {
			parameters = FileIOUtils.parseXML(parametersFile);
		} catch (Throwable error) {}
		if (parameters == null)
			throw new NullPointerException(
				"Argument params.xml file could not be parsed.");
		// initialize basic parameters to null
		String username = null;
		String taskID = null;
		// if this is a dataset task, then the username for the purpose
		// of generating file descriptors will be the dataset ID
		if (datasetID != null)
			username = datasetID;
		// otherwise, extract username and task ID parameters from params.xml
		else try {
			Node parameter = XPathAPI.selectSingleNode(
				parameters, "//parameter[@name='user']");
			if (parameter == null)
				throw new NullPointerException(
					"A \"user\" parameter could not be found" +
					"in the parsed params.xml document.");
			else username = parameter.getFirstChild().getNodeValue();
			parameter = XPathAPI.selectSingleNode(
				parameters, "//parameter[@name='task']");
			if (parameter == null)
				throw new NullPointerException(
					"A \"task\" parameter could not be found" +
					"in the parsed params.xml document.");
			else taskID = parameter.getFirstChild().getNodeValue();
		} catch (Throwable error) {
			throw new IllegalArgumentException(
				"There was an error extracting a parameter from params.xml",
				error);
		}
		// if a dataset ID was specified, then the mzTab and peak list
		// directories had better be found under that dataset!
		String relativePrefix = null;
		if (datasetID != null)
			relativePrefix = datasetID;
		// otherwise, these directories had better be found under the
		// task indicated by the username/task ID from params.xml!
		else relativePrefix = String.format(
			"%s%s%s", username, File.separator, taskID);
		// if mzTab and peak list relative paths were not provided, determine
		// them based on whether this is a dataset or a regular ProteoSAFe task
		if (mzTabRelativePath == null)
			mzTabRelativePath = extractRelativePath(
				mzTabDirectory.getAbsolutePath(), relativePrefix);
		if (peakListRelativePath == null && peakListDirectory != null)
			peakListRelativePath = extractRelativePath(
				peakListDirectory.getAbsolutePath(), relativePrefix);
		// initialize mzTab file mapping collection
		mzTabs = findMzTabFiles(mzTabDirectory);
		if (mzTabs == null)
			mzTabs = new ArrayList<MzTabFile>();
		// cache all dataset files to speed up descriptor lookups
		Collection<File> datasetFiles = null;
		if (datasetID != null) {
			File datasetDirectory =
				new File(ProteoSAFeUtils.USER_FILES_ROOT, datasetID);
			// if this is a reanalysis, be sure to look only at its
			// own files, not everything under the entire container
			if (datasetID.startsWith(
				ProteoSAFeUtils.REANALYSIS_CONTAINER_ACCESSION_PREFIX) &&
				mzTabRelativePath != null)
				// the first directory in the mzTab relative path should
				// be the root directory for this reanalysis attachment
				datasetDirectory = new File(datasetDirectory,
					mzTabRelativePath.split(Pattern.quote("/"))[0]);
			datasetFiles = FileIOUtils.findFiles(datasetDirectory);
		}
		// extract file mapping context from params.xml
		mappings = new ProteoSAFeFileMappingContext(parameters);
		// iterate through all mzTab and ms_run mappings
		// and fill them out with params.xml knowledge
		for (MzTabFile mzTab : mzTabs) {
			// fill out this mzTab's file mappings
			mapMzTab(mzTab, mappings);
			// set descriptor appropriately based on parameters
			if (datasetID != null)
				mzTab.setDatasetDescriptor(
					datasetID, mzTabRelativePath, datasetFiles);
			else mzTab.setTaskDescriptor(username, taskID, mzTabRelativePath);
			// set descriptors appropriately for all ms_runs
			Map<Integer, MzTabMsRun> msRuns = mzTab.getMsRuns();
			for (Integer msRunIndex : msRuns.keySet()) {
				MzTabMsRun msRun = msRuns.get(msRunIndex);
				// fill out this ms_run's file mappings
				mapMsRun(msRun, mzTab.getMappedResultPath(), mappings);
				// handle repository submissions properly
				if (datasetID != null) {
					// if this is an ms_run for a dataset mzTab file, then
					// the peak list file had better be in the same dataset
					if (datasetID.startsWith("MSV"))
						msRun.setDatasetDescriptor(
							datasetID, peakListRelativePath, datasetFiles);
					// otherwise this is a reanalysis, so the peak list file
					// most likely will not be copied with this submission
					else {
						// if the cleaned ms_run-location string is already a
						// dataset relative path, just use that as the descriptor
						String msRunLocation = msRun.getCleanedMsRunLocation();
						if (msRunLocation != null &&
							msRunLocation.matches("^(R)?MSV[0-9]{9}/.*$"))
							msRun.setDescriptor(msRunLocation);
						// otherwise, only set the dataset descriptor with the given
						// relative path if this is not already a dataset file
						else {
							String uploaded = msRun.getUploadedPeakListPath();
							if (uploaded == null ||
								uploaded.matches("^(R)?MSV[0-9]{9}/.*$") == false)
								msRun.setDatasetDescriptor(
									datasetID, peakListRelativePath, datasetFiles);
						}
					}
				}
				// by default a peak list file's descriptor points
				// to its uploaded path; only try to set it to
				// something else if no uploaded path is present
				else if (msRun.getUploadedPeakListPath() == null) {
					// only try to use a task descriptor if a workflow-specific
					// peak list subdirectory relative path was provided
					if (peakListRelativePath != null)
						msRun.setTaskDescriptor(
							username, taskID, peakListRelativePath);
					// otherwise we must assume that the ms_run-location
					// does not correspond to any ProteoSAFe file, so
					// just retain it in the mzTab file exactly as-is
					else msRun.setDescriptor(msRun.getCleanedMsRunLocation());
				}
			}
		}
	}
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	@Override
	public String toString() {
		StringBuilder context = new StringBuilder("[");
		for (MzTabFile mzTab : mzTabs)
			context.append("\n").append(mzTab.toString()).append(",");
		// chomp trailing comma
		if (context.charAt(context.length() - 1) == ',')
			context.setLength(context.length() - 1);
		context.append("\n]");
		return context.toString();
	}
	
	/**
	 * Test main method to simply print out the context for a task
	 */
	public static void main(String[] args) {
		String usage =
			"java -cp MassIVEUtils.jar edu.ucsd.mztab.TaskMzTabContext " +
			"<MzTabDirectory> <MzTabRelativePath> " +
			"<PeakListDirectory> <PeakListRelativePath> " +
			"<ProteoSAFeParametersFile> [<DatasetID>]";
		try {
			File mzTabDirectory = new File(args[0]);
			String mzTabRelativePath = null;
			if (args[1].trim().isEmpty() == false)
				mzTabRelativePath = args[1];
			File peakListDirectory = null;
			if (args[2].trim().isEmpty() == false)
				peakListDirectory = new File(args[2]);
			String peakListRelativePath = null;
			if (args[3].trim().isEmpty() == false)
				peakListRelativePath = args[3];
			String peakListCollection = null;
			if (args[4].trim().isEmpty() == false)
				peakListCollection = args[4];
			File paramsFile = new File(args[5]);
			String datasetID = null;
			if (args.length >= 7)
				datasetID = args[6];
			TaskMzTabContext context = new TaskMzTabContext(
				mzTabDirectory, mzTabRelativePath,
				peakListDirectory, peakListRelativePath,
				peakListCollection, paramsFile, datasetID);
			System.out.println(context.toString());
		} catch (Throwable error) {
			error.printStackTrace();
			System.err.println("----------");
			System.err.println(usage);
		}
	}
	
	/*========================================================================
	 * Property accessor methods
	 *========================================================================*/
	public Collection<MzTabFile> getMzTabFiles() {
		if (mzTabs == null)
			return null;
		else return new ArrayList<MzTabFile>(mzTabs);
	}
	
	public MzTabFile getMzTabFile(File mzTabFile) {
		if (mzTabFile == null)
			return null;
		for (MzTabFile mzTab : mzTabs)
			if (mzTab.getFile().equals(mzTabFile))
				return mzTab;
		return null;
	}
	
	public MzTabMsRun getPeakListFile(String spectrumIDsFilename) {
		if (spectrumIDsFilename == null)
			return null;
		Collection<MzTabFile> mzTabFiles = getMzTabFiles();
		if (mzTabFiles == null || mzTabFiles.isEmpty())
			return null;
		else for (MzTabFile mzTabFile : getMzTabFiles()) {
			Map<Integer, MzTabMsRun> msRuns = mzTabFile.getMsRuns();
			if (msRuns == null || msRuns.isEmpty())
				continue;
			else for (MzTabMsRun msRun : msRuns.values()) {
				String mangledPeakListFilename =
					msRun.getMangledPeakListFilename();
				if (mangledPeakListFilename == null)
					continue;
				else if (spectrumIDsFilename.equals(String.format("%s.scans",
					FilenameUtils.getBaseName(mangledPeakListFilename))))
					return msRun;
			}
		}
		return null;
	}
	
	public ProteoSAFeFileMappingContext getMappingContext() {
		return mappings;
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private Collection<MzTabFile> findMzTabFiles(File mzTabDirectory) {
		if (mzTabDirectory == null || mzTabDirectory.canRead() == false ||
			mzTabDirectory.isDirectory() == false)
			return null;
		File[] files = mzTabDirectory.listFiles();
		if (files == null || files.length < 1)
			return null;
		// sort files alphabetically
		Arrays.sort(files);
		// add all found mzTabs to collection
		Collection<MzTabFile> mzTabs = new ArrayList<MzTabFile>();
		for (File file : files) {
			// recurse into subdirectories
			if (file.isDirectory()) {
				Collection<MzTabFile> descendantMzTabs = findMzTabFiles(file);
				if (descendantMzTabs != null &&
					descendantMzTabs.isEmpty() == false)
					mzTabs.addAll(descendantMzTabs);
			} else mzTabs.add(new MzTabFile(file));
		}
		return mzTabs;
	}
	
	private String extractRelativePath(
		String absolutePath, String relativePrefix
	) {
		if (absolutePath == null || relativePrefix == null)
			return null;
		int relativePathStart = absolutePath.indexOf(relativePrefix);
		if (relativePathStart < 0)
			return null;
		String relativePath = absolutePath.substring(relativePathStart);
		// trim off relative prefix
		relativePath = relativePath.substring(relativePrefix.length());
		// trim off leading slash, if present
		if (relativePath.startsWith(File.separator))
			relativePath = relativePath.substring(1);
		if (relativePath.trim().isEmpty())
			return null;
		else return relativePath;
	}
	
	private void mapMzTab(
		MzTabFile mzTab, ProteoSAFeFileMappingContext mappings
	) {
		if (mzTab == null || mappings == null)
			return;
		// get known mzTab file properties
		String mzTabFilename = mzTab.getFile().getName();
		String resultFilenameBase = FilenameUtils.getBaseName(mzTabFilename);
		String mzTabFilePath =
			FilenameUtils.separatorsToUnix(mzTab.getFile().getAbsolutePath());
		// if this mzTab file is based on an input collection file (that hasn't
		// yet been demangled e.g. for insertion into a dataset) then its name
		// should still be mangled and should therefore be mapped to an original
		// user-uploaded result file via some "upload_file_mapping" parameter
		for (UploadMapping mapping : mappings.getUploadMappings()) {
			// the mzTab file may be part of the original upload collection,
			// or it may be converted from a file of that collection, in
			// which case only the filename bases will match (e.g. it was
			// converted from mzIdentML to mzTab)
			String mangledFilename = mapping.getMangledFilename();
			String mangledFilenameBase =
				FilenameUtils.getBaseName(mangledFilename);
			if (mangledFilename.equals(mzTabFilename) ||
				mangledFilenameBase.equals(resultFilenameBase)) {
				mzTab.setMangledResultFilename(mangledFilename);
				mzTab.setUploadedResultPath(mapping.getUploadFilePath());
				break;
			}
		}
		// if no uploaded path was found for this mzTab file, then its name
		// may already have been demangled, e.g. to be copied into a dataset;
		// in which case, if it is based on an input collection file, then
		// its name should match up with an original user-uploaded result file
		// via some "upload_file_mapping" parameter
		String uploadedResultPath = mzTab.getUploadedResultPath();
		if (uploadedResultPath == null) {
			String mangledFilename = null;
			// look through upload mappings for best (i.e. longest) matching
			// "normalized" upload file path; that is, the part of the path
			// that is preserved within the actual (e.g. dataset) directory
			for (UploadMapping mapping : mappings.getUploadMappings()) {
				String normalizedPath = mapping.getNormalizedUploadFilePath();
				if (mzTabFilePath.endsWith(normalizedPath) &&
					(uploadedResultPath == null ||
					uploadedResultPath.length() < normalizedPath.length())) {
					mangledFilename = mapping.getMangledFilename();
					uploadedResultPath = mapping.getUploadFilePath();
				}
			}
			if (uploadedResultPath != null && mangledFilename != null) {
				mzTab.setMangledResultFilename(mangledFilename);
				mzTab.setUploadedResultPath(uploadedResultPath);
			}
		}
		// if an uploaded path was found for the mzTab file, then it is based
		// on an input collection file and therefore may have its mapped
		// relative path recorded in "result_file_mapping" parameters
		if (uploadedResultPath != null) {
			for (String resultFilename : mappings.getResultFilenames()) {
				if (uploadedResultPath.endsWith(resultFilename)) {
					mzTab.setMappedResultPath(resultFilename);
					break;
				}
			}
		}
	}
	
	private void mapMsRun(
		MzTabMsRun msRun, String mappedResultPath,
		ProteoSAFeFileMappingContext mappings
	) {
		if (msRun == null || mappings == null)
			return;
		// get known ms_run file properties
		String cleanedMsRun = msRun.getCleanedMsRunLocation();
		// if the parent mzTab file has a mapped path, then it is based
		// on an input collection file and therefore may have its peak list
		// mappings recorded in "result_file_mapping" parameters
		if (mappedResultPath != null) {
			boolean found = false;
			for (String resultFilename : mappings.getResultFilenames()) {
				if (mappedResultPath.endsWith(resultFilename)) {
					Collection<String> msRunLocations =
						mappings.getResultFileMsRunLocations(resultFilename);
					if (msRunLocations != null) {
						for (String msRunLocation : msRunLocations) {
							if (cleanedMsRun.equals(msRunLocation)) {
								msRun.setMappedPeakListPath(
									mappings.getResultFileMsRunMapping(
										resultFilename, msRunLocation));
								found = true;
								break;
							}
						}
					}
					if (found)
						break;
				}
			}
		}
		// now try to find the best match for this ms_run peak list file
		// from among this task's "upload_file_mapping" parameters
		String uploadedPeakListMatch = msRun.getMappedPeakListPath();
		// first try the main peak list collection
		UploadMapping mapping = getMsRunUploadMapping(
			cleanedMsRun, uploadedPeakListMatch,
			mappings.getUploadMappings(peakListCollection));
		// if not found there, then try all the collections
		if (mapping == null)
			mapping = getMsRunUploadMapping(cleanedMsRun, uploadedPeakListMatch,
				mappings.getUploadMappings());
		// if a match was found, use it
		if (mapping != null) {
			msRun.setMangledPeakListFilename(mapping.getMangledFilename());
			msRun.setUploadedPeakListPath(mapping.getUploadFilePath());
		}
	}
	
	private UploadMapping getMsRunUploadMapping(
		String cleanedMsRun, String uploadedPeakListMatch,
		Collection<UploadMapping> mappings
	) {
		if (cleanedMsRun == null || mappings == null || mappings.isEmpty())
			return null;
		// first try exact matches
		for (UploadMapping mapping : mappings)
			if (matchMsRunUploadMapping(cleanedMsRun, mapping, true))
				return mapping;
		// if no exact matches were found, try to match the mapped
		// value with the first uploaded path that matches it
		if (uploadedPeakListMatch != null) {
			for (UploadMapping mapping : mappings) {
				// if an upload mapping exists for this ms_run,
				// then the best match scenario we could hope for is that
				// a "result_file_mapping" exists, and its value is some
				// ending portion of the uploaded peak list file path, e.g.
				// MassIVE dataset submission or any workflow with file mapping
				if (mapping.getUploadFilePath().endsWith(uploadedPeakListMatch))
					return mapping;
			}
		}
		// if no uploaded peak list match was provided,
		// look for the best one in the mappings
		else {
			// collect all near matches, since we don't want any conflicts
			Collection<UploadMapping> matches =
				new LinkedHashSet<UploadMapping>();
			for (UploadMapping mapping : mappings)
				if (matchMsRunUploadMapping(cleanedMsRun, mapping, false))
					matches.add(mapping);
			// only return a near match if there are no conflicts
			if (matches.size() == 1)
				return matches.iterator().next();
		}
		// if no matches were found then return null
		return null;
	}
	
	private boolean matchMsRunUploadMapping(
		String cleanedMsRun, UploadMapping mapping, boolean exact
	) {
		if (cleanedMsRun == null || mapping == null)
			return false;
		String msRunBase = stripExtension(cleanedMsRun);
		String mangledFilename = mapping.getMangledFilename();
		String mangledFilenameBase = stripExtension(mangledFilename);
		String uploadFilePath = mapping.getUploadFilePath();
		// if an upload mapping exists for this ms_run,
		// then there are two possible exact match scenarios:
		// 1. the ms_run-location value ends with the mangled filename,
		// e.g. analysis workflows with mzTab conversion integrated
		if (cleanedMsRun.endsWith(mangledFilename) ||
		// 2. the ms_run-location value ends with some converted version
		// of the mangled filename, e.g. analysis workflows with spectrum
		// file conversion - in which case filename bases will match
			msRunBase.endsWith(mangledFilenameBase) ||
		// 3. the ms_run-location value is some ending portion of the
		// uploaded peak list file path, e.g. the convert-tsv workflow
			uploadFilePath.endsWith(cleanedMsRun))
			return true;
		// if we don't care about matching exactly,
		// then try matching just the leaf filename
		else if (exact == false) {
			String msRunFilename = FilenameUtils.getBaseName(cleanedMsRun);
			String uploadFilename = FilenameUtils.getBaseName(uploadFilePath);
			if (uploadFilename.equals(msRunFilename))
				return true;
//			// if we really want to get crazy, then
//			// try matching just the filename bases
//			String msRunFilenameBase = stripExtension(msRunFilename);
//			String uploadFilenameBase = stripExtension(uploadFilename);
//			if (uploadFilenameBase.equals(msRunFilenameBase))
//				return true;
		}
		// if nothing matched, then return false
		return false;
	}
	
	private String stripExtension(String path) {
		if (path == null)
			return null;
		String filename = FilenameUtils.getName(path);
		int dot = filename.indexOf(".");
		if (dot < 0)
			return path;
		else return path.substring(0, path.length() - filename.length() + dot);
	}
}
