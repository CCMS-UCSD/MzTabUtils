package edu.ucsd.mztab;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import javax.xml.transform.TransformerException;

import org.apache.commons.io.FilenameUtils;
import org.apache.xpath.XPathAPI;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import edu.ucsd.mztab.model.MzTabFile;
import edu.ucsd.mztab.model.MzTabMsRun;
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
	public TaskMzTabContext(File mzTabDirectory, File parametersFile) {
		this(mzTabDirectory, null, parametersFile);
	}
	
	public TaskMzTabContext(
		File mzTabDirectory, File peakListDirectory, File parametersFile
	) {
		this(mzTabDirectory, peakListDirectory, parametersFile, null);
	}
	
	public TaskMzTabContext(
		File mzTabDirectory, File peakListDirectory, File parametersFile,
		String datasetID
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
		// determine mzTab and peak list relative paths, based on
		// whether this is a dataset or a regular ProteoSAFe task
		String mzTabRelativePath = extractRelativePath(
			mzTabDirectory.getAbsolutePath(), relativePrefix);
		String peakListRelativePath = null;
		if (peakListDirectory != null)
			peakListRelativePath = extractRelativePath(
				peakListDirectory.getAbsolutePath(), relativePrefix);
		// initialize mzTab file mapping collection
		mzTabs = findMzTabFiles(mzTabDirectory);
		if (mzTabs == null)
			mzTabs = new ArrayList<MzTabFile>();
		// iterate through all mzTab and ms_run mappings
		// and fill them out with params.xml knowledge
		for (MzTabFile mzTab : mzTabs) try {
			// fill out this mzTab's file mappings
			mapMzTab(mzTab, parameters);
			// set descriptor appropriately based on parameters
			if (datasetID != null)
				mzTab.setDatasetDescriptor(datasetID, mzTabRelativePath);
			else mzTab.setTaskDescriptor(username, taskID, mzTabRelativePath);
			Map<Integer, MzTabMsRun> msRuns = mzTab.getMsRuns();
			for (Integer msRunIndex : msRuns.keySet()) {
				MzTabMsRun msRun = msRuns.get(msRunIndex);
				// fill out this ms_run's file mappings
				mapMsRun(msRun, mzTab.getMappedResultPath(), parameters);
				if (datasetID != null)
					msRun.setDatasetDescriptor(datasetID, peakListRelativePath);
				// only try to use a task descriptor if there's no upload
				// mapping; peak list files should always have such a mapping
				else if (msRun.getUploadedPeakListPath() == null)
					msRun.setTaskDescriptor(
						username, taskID, peakListRelativePath);
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
		File mzTabDirectory = new File(args[0]);
		File peakListDirectory = new File(args[1]);
		File paramsFile = new File(args[2]);
		String datasetID = null;
		if (args.length >= 4)
			datasetID = args[3];
		TaskMzTabContext context = new TaskMzTabContext(
			mzTabDirectory, peakListDirectory, paramsFile, datasetID);
		System.out.println(context.toString());
	}
	
	/*========================================================================
	 * Property accessor methods
	 *========================================================================*/
	public MzTabFile getMzTabFile(File mzTabFile) {
		if (mzTabFile == null)
			return null;
		for (MzTabFile mzTab : mzTabs)
			if (mzTab.getFile().equals(mzTabFile))
				return mzTab;
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
	
	private void mapMzTab(MzTabFile mzTab, Document parameters)
	throws TransformerException {
		if (mzTab == null || parameters == null)
			return;
		// get known mzTab file properties
		String mzTabFilename = mzTab.getFile().getName();
		String resultFilenameBase = FilenameUtils.getBaseName(mzTabFilename);
		// if this mzTab file is based on an input collection file, then its
		// name should be mangled, and should therefore be mapped to an original
		// user-uploaded result file via some "upload_file_mapping" parameter
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
				// the mzTab file may be part of the original upload collection,
				// or it may be converted from a file of that collection, in
				// which case only the filename bases will match (e.g. it was
				// converted from mzIdentML to mzTab)
				String mangledFilenameBase =
					FilenameUtils.getBaseName(tokens[0]);
				if (tokens[0].equals(mzTabFilename) ||
					mangledFilenameBase.equals(resultFilenameBase)) {
					mzTab.setMangledResultFilename(tokens[0]);
					mzTab.setUploadedResultPath(tokens[1]);
				}
			}
		}
		// if the mzTab file has an uploaded path, then it is based
		// on an input collection file and therefore may have its mapped
		// relative path recorded in "result_file_mapping" parameters
		String uploadedResultPath = mzTab.getUploadedResultPath();
		if (uploadedResultPath != null) {
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
					if (uploadedResultPath.endsWith(mapped[0])) {
						mzTab.setMappedResultPath(mapped[0]);
						break;
					}
				}
			}
		}
	}
	
	private void mapMsRun(
		MzTabMsRun msRun, String mappedResultPath, Document parameters
	) throws TransformerException {
		if (msRun == null || parameters == null)
			return;
		// get known ms_run file properties
		String cleanedMsRun = msRun.getCleanedMsRunLocation();
		// if the parent mzTab file has a mapped path, then it is based
		// on an input collection file and therefore may have its peak list
		// mappings recorded in "result_file_mapping" parameters
		if (mappedResultPath != null) {
			NodeList mappings = XPathAPI.selectNodeList(
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
					if (mappedResultPath.endsWith(mapped[0]) &&
						cleanedMsRun.equals(mapped[1])) {
						msRun.setMappedPeakListPath(tokens[1]);
						break;
					}
				}
			}
		}
		// now try to find the best match for this ms_run peak list file
		// from among this task's "upload_file_mapping" parameters
		String uploadedPeakListMatch = msRun.getMappedPeakListPath();
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
				// if an upload mapping exists for this ms_run,
				// then there are three possible scenarios:
				// 1. the ms_run-location value is the mangled filename,
				// e.g. analysis workflows with mzTab conversion integrated
				if (tokens[0].equals(cleanedMsRun) ||
				// 2. the ms_run-location value is some ending portion of the
				// uploaded peak list file path, e.g. the convert-tsv workflow
					tokens[1].endsWith(cleanedMsRun) ||
				// 3. a "result_file_mapping" exists and its value is some
				// ending portion of the uploaded peak list file path, e.g.
				// MassIVE dataset submission or any workflow with file mapping
					(uploadedPeakListMatch != null &&
						tokens[1].endsWith(uploadedPeakListMatch))) {
					msRun.setMangledPeakListFilename(tokens[0]);
					msRun.setUploadedPeakListPath(tokens[1]);
					return;
				}
			}
		}
	}
}
