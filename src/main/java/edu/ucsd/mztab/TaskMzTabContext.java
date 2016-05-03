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
import edu.ucsd.util.CommonUtils;
import edu.ucsd.util.FileIOUtils;

public class TaskMzTabContext
{
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private String                workflow;
	private Collection<MzTabFile> mzTabs;
	
	/*========================================================================
	 * Constructors
	 *========================================================================*/
	public TaskMzTabContext(File mzTabDirectory, File parametersFile) {
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
		// extract workflow from params.xml
		try {
			Node workflow = XPathAPI.selectSingleNode(
				parameters, "//parameter[@name='workflow']");
			// account for legacy workflows using "tool" parameter
			if (workflow == null)
				workflow = XPathAPI.selectSingleNode(
					parameters, "//parameter[@name='tool']");
			if (workflow == null)
				throw new NullPointerException(
					"A \"workflow\" or \"tool\" parameter could not " +
					"be found in the parsed params.xml document.");
			else this.workflow = workflow.getFirstChild().getNodeValue();
		} catch (Throwable error) {
			throw new IllegalArgumentException(
				"There was an error extracting workflow from params.xml",
				error);
		}
		// initialize mzTab file mapping collection
		File[] files = mzTabDirectory.listFiles();
		// sort files alphabetically
		Arrays.sort(files);
		if (files == null || files.length < 1)
			mzTabs = new ArrayList<MzTabFile>();
		else mzTabs = new ArrayList<MzTabFile>(files.length);
		// read all mzTab files, parse out ms_run declarations
		for (File file : files)
			mzTabs.add(new MzTabFile(file));
		// iterate through all mzTab and ms_run mappings
		// and fill them out with params.xml knowledge
		for (MzTabFile mzTab : mzTabs) try {
			// fill out this mzTab mapping
			mapMzTab(mzTab, parameters);
			Map<Integer, MzTabMsRun> msRuns = mzTab.getMsRuns();
			for (Integer msRunIndex : msRuns.keySet()) {
				// fill out this ms_run mapping
				mapMsRun(msRuns.get(msRunIndex),
					mzTab.getUploadedResultPath(), parameters);
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
		for (MzTabFile mzTab : mzTabs) {
			context.append("\n\t{\"");
			context.append(mzTab.getFile().getAbsolutePath());
			context.append("\":\n\t\t{\"mangled\":");
			String mangled = mzTab.getMangledResultFilename();
			if (mangled == null)
				context.append("null");
			else context.append("\"").append(mangled).append("\"");
			context.append(",\"uploaded\":");
			String uploaded = mzTab.getUploadedResultPath();
			if (uploaded == null)
				context.append("null");
			else context.append("\"").append(uploaded).append("\"");
			context.append(",\n\t\t\"ms_runs\":[");
			Map<Integer, MzTabMsRun> msRuns = mzTab.getMsRuns();
			for (Integer msRunIndex : msRuns.keySet()) {
				context.append("\n\t\t\t");
				context.append(msRuns.get(msRunIndex).toString());
				context.append(",");
			}
			// chomp trailing comma
			if (context.charAt(context.length() - 1) == ',')
				context.setLength(context.length() - 1);
			context.append("\n\t\t]}\n\t},");
		}
		// chomp trailing comma
		if (context.charAt(context.length() - 1) == ',')
			context.setLength(context.length() - 1);
		context.append("\n]");
		return context.toString();
	}
	
	public static void main(String[] args) {
		TaskMzTabContext context =
			new TaskMzTabContext(new File(args[0]), new File(args[1]));
		System.out.println(context.toString());
	}
	
	/*========================================================================
	 * Property accessor methods
	 *========================================================================*/
	public String getWorkflow() {
		return workflow;
	}
	
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
					return;
				}
			}
		}
	}
	
	private void mapMsRun(
		MzTabMsRun msRun, String uploadedResultPath, Document parameters
	) throws TransformerException {
		if (msRun == null || parameters == null)
			return;
		// get known ms_run file properties
		String msRunLocation = msRun.getMsRunLocation();
		String cleanedMsRun = CommonUtils.cleanFileURL(msRunLocation);
		// if the parent mzTab file has an uploaded path, then it is based
		// on an input collection file and therefore may have its peak list
		// mappings recorded in "result_file_mapping" parameters
		if (uploadedResultPath != null) {
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
					if (uploadedResultPath.endsWith(mapped[0]) &&
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
