package edu.ucsd.mztab;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.xpath.XPathAPI;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import edu.ucsd.mztab.model.MzTabFile;
import edu.ucsd.mztab.model.MzTabMsRun;
import edu.ucsd.mztab.model.ProteoSAFeFileMappingContext;
import edu.ucsd.mztab.model.ProteoSAFeFileMappingContext.UploadMapping;
import edu.ucsd.util.FileIOUtils;

public class TaskMzTabContext
{
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private Collection<MzTabFile> mzTabs;
	
	/*========================================================================
	 * Constructors
	 *========================================================================*/
	public TaskMzTabContext(
		File mzTabDirectory, String mzTabRelativePath, File parametersFile
	) {
		this(mzTabDirectory, mzTabRelativePath, null, null, parametersFile);
	}
	
	public TaskMzTabContext(
		File mzTabDirectory, String mzTabRelativePath,
		File peakListDirectory, String peakListRelativePath,
		File parametersFile
	) {
		this(mzTabDirectory, mzTabRelativePath,
			peakListDirectory, peakListRelativePath, parametersFile, null);
	}
	
	public TaskMzTabContext(
		File mzTabDirectory, String mzTabRelativePath,
		File peakListDirectory, String peakListRelativePath,
		File parametersFile, String datasetID
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
		// extract file mapping context from params.xml
		ProteoSAFeFileMappingContext mappings =
			new ProteoSAFeFileMappingContext(parameters);
		// iterate through all mzTab and ms_run mappings
		// and fill them out with params.xml knowledge
		for (MzTabFile mzTab : mzTabs) {
			// fill out this mzTab's file mappings
			mapMzTab(mzTab, mappings);
			// set descriptor appropriately based on parameters
			if (datasetID != null)
				mzTab.setDatasetDescriptor(datasetID, mzTabRelativePath);
			else mzTab.setTaskDescriptor(username, taskID, mzTabRelativePath);
			Map<Integer, MzTabMsRun> msRuns = mzTab.getMsRuns();
			for (Integer msRunIndex : msRuns.keySet()) {
				MzTabMsRun msRun = msRuns.get(msRunIndex);
				// fill out this ms_run's file mappings
				mapMsRun(msRun, mzTab.getMappedResultPath(), mappings);
				if (datasetID != null) {
					// if the cleaned ms_run-location string is already a
					// dataset relative path, just use that as the descriptor
					String msRunLocation = msRun.getCleanedMsRunLocation();
					if (msRunLocation != null &&
						msRunLocation.matches("^MSV[0-9]{9}/.*$"))
						msRun.setDescriptor(msRunLocation);
					// otherwise, only set the dataset descriptor with the given
					// relative path if this is not already a dataset file
					else {
						String uploaded = msRun.getUploadedPeakListPath();
						if (uploaded == null ||
							uploaded.matches("^MSV[0-9]{9}/.*$") == false)
							msRun.setDatasetDescriptor(
								datasetID, peakListRelativePath);
					}
				}
				// only try to use a task descriptor if there's no upload
				// mapping; peak list files should always have such a mapping
				else if (msRun.getUploadedPeakListPath() == null)
					msRun.setTaskDescriptor(
						username, taskID, peakListRelativePath);
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
			File paramsFile = new File(args[4]);
			String datasetID = null;
			if (args.length >= 6)
				datasetID = args[5];
			TaskMzTabContext context = new TaskMzTabContext(
				mzTabDirectory, mzTabRelativePath,
				peakListDirectory, peakListRelativePath,
				paramsFile, datasetID);
			System.out.println(context.toString());
		} catch (Throwable error) {
			System.out.println(usage);
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
		// first try exact matches
		for (UploadMapping mapping : mappings.getUploadMappings()) {
			String mangledFilename = mapping.getMangledFilename();
			String uploadedPeakListPath = mapping.getUploadFilePath();
			// if an upload mapping exists for this ms_run,
			// then there are two possible exact match scenarios:
			// 1. the ms_run-location value ends with the mangled filename,
			// e.g. analysis workflows with mzTab conversion integrated
			if (cleanedMsRun.endsWith(mangledFilename) ||
			// 2. the ms_run-location value is some ending portion of the
			// uploaded peak list file path, e.g. the convert-tsv workflow
				uploadedPeakListPath.endsWith(cleanedMsRun)) {
				msRun.setMangledPeakListFilename(mangledFilename);
				msRun.setUploadedPeakListPath(uploadedPeakListPath);
				return;
			}
		}
		// if no exact matches were found, try to match the mapped
		// value with the first uploaded path that matches it
		if (uploadedPeakListMatch != null) {
			for (UploadMapping mapping : mappings.getUploadMappings()) {
				String mangledFilename = mapping.getMangledFilename();
				String uploadedPeakListPath = mapping.getUploadFilePath();
				// if an upload mapping exists for this ms_run,
				// then the best match scenario we could hope for is that
				// a "result_file_mapping" exists, and its value is some
				// ending portion of the uploaded peak list file path, e.g.
				// MassIVE dataset submission or any workflow with file mapping
				if (uploadedPeakListPath.endsWith(uploadedPeakListMatch)) {
					msRun.setMangledPeakListFilename(mangledFilename);
					msRun.setUploadedPeakListPath(uploadedPeakListPath);
					return;
				}
			}
		}
	}
}
