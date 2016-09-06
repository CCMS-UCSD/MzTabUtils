package edu.ucsd.mztab.ui;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.w3c.dom.Document;

import edu.ucsd.mztab.model.ProteoSAFeFileMappingContext;
import edu.ucsd.mztab.model.ProteoSAFeFileMappingContext.UploadMapping;
import edu.ucsd.util.FileIOUtils;

public class ManglePeakListFiles
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String USAGE =
		"java -cp MzTabUtils.jar edu.ucsd.mztab.ui.ManglePeakListFiles" +
		"\n\t-peak   <PeakDirectoryInDataset>" +
		"\n\t-params <ProteoSAFeParametersFile>" +
		"\n\t-output <MangledPeakLinksDirectory>";
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public static void main(String[] args) {
		ManglePeakListFilesOperation mangle = extractArguments(args);
		if (mangle == null)
			die(USAGE);
		// link each peak list file to its mangled filename
		// within the designated output directory
		for (String mangledFilename : mangle.mangledPeakListFiles.keySet()) {
			File peakListFile =
				mangle.mangledPeakListFiles.get(mangledFilename);
			File mangledPeakListFileLink =
				new File(mangle.outputDirectory, mangledFilename);
			FileIOUtils.makeLink(peakListFile, mangledPeakListFileLink);
		}
	}
	
	/*========================================================================
	 * Convenience classes
	 *========================================================================*/
	/**
	 * Struct to maintain context data for each
	 * peak directory rebuilding operation.
	 */
	private static class ManglePeakListFilesOperation {
		/*====================================================================
		 * Properties
		 *====================================================================*/
		private Map<String, File> mangledPeakListFiles;
		private File              outputDirectory;
		
		/*====================================================================
		 * Constructors
		 *====================================================================*/
		public ManglePeakListFilesOperation(
			File peakDirectory, File parametersFile, File outputDirectory
		) {
			// validate input peak list files directory
			if (peakDirectory == null)
				throw new NullPointerException(
					"Peak list files directory cannot be null.");
			else if (peakDirectory.isDirectory() == false)
				throw new IllegalArgumentException(String.format(
					"Peak list files directory [%s] must be a directory.",
					peakDirectory.getAbsolutePath()));
			else if (peakDirectory.canRead() == false)
				throw new IllegalArgumentException(String.format(
					"Peak list files directory [%s] must be readable.",
					peakDirectory.getAbsolutePath()));
			// validate params.xml file
			if (parametersFile == null)
				throw new NullPointerException(
					"Argument params.xml file cannot be null.");
			else if (parametersFile.isFile() == false ||
				parametersFile.canRead() == false)
				throw new IllegalArgumentException(
					"Argument params.xml file must be a readable file.");
			// validate mangled peak list links output directory
			if (outputDirectory == null)
				throw new NullPointerException(
					"Output directory cannot be null.");
			else if (outputDirectory.isDirectory() == false)
				throw new IllegalArgumentException(
					String.format("Output directory [%s] must be a directory.",
						outputDirectory.getAbsolutePath()));
			else if (outputDirectory.canWrite() == false)
				throw new IllegalArgumentException(
					String.format("Output directory [%s] must be writable.",
						outputDirectory.getAbsolutePath()));
			else this.outputDirectory = outputDirectory;
			// parse params.xml file into a DOM object
			Document parameters = null;
			try { parameters = FileIOUtils.parseXML(parametersFile); }
			catch (Throwable error) {}
			if (parameters == null)
				throw new NullPointerException(
					"Argument params.xml file could not be parsed.");
			// get file mapping context from params.xml
			ProteoSAFeFileMappingContext context =
				new ProteoSAFeFileMappingContext(parameters);
			// recursively search input directory for all peak list files
			Collection<File> peakListFiles = findFiles(peakDirectory);
			// map all found peak list files to their mangled names
			mangledPeakListFiles = new LinkedHashMap<String, File>();
			String peakListRoot = peakDirectory.getAbsolutePath();
			for (File peakListFile : peakListFiles) {
				// get this peak list file's relative path
				// under the input peak list files directory
				String peakListRelativePath =
					peakListFile.getAbsolutePath().substring(
						peakListRoot.length());
				// chomp leading slash, if present
				if (peakListRelativePath.isEmpty() == false &&
					peakListRelativePath.charAt(0) == File.separatorChar)
					peakListRelativePath = peakListRelativePath.substring(1);
				// find this peak list file's upload mapping
				String mangledFilename =
					context.getMangledFilename(peakListRelativePath);
				if (mangledFilename == null)
					throw new IllegalArgumentException(String.format(
						"No \"upload_file_mapping\" parameter could be found " +
						"for peak list file [%s] in parameters file [%s].",
						peakListFile.getAbsolutePath(),
						parametersFile.getAbsolutePath()));
				else mangledPeakListFiles.put(mangledFilename, peakListFile);
			}
			// verify that all params.xml mapped files are present
			// in the input peak list files directory
			for (UploadMapping uploadMapping : context.getUploadMappings(
				"peak_list_files"))
				if (mangledPeakListFiles.containsKey(
					uploadMapping.getMangledFilename()) == false)
					throw new IllegalArgumentException(String.format(
						"Parameter file [%s] contained an " +
						"\"upload_file_mapping\" parameter for peak list " +
						"file [%s], yet no such file was found under input " +
						"peak list files directory [%s].",
						parametersFile.getAbsolutePath(),
						uploadMapping.getUploadFilePath(),
						peakDirectory.getAbsolutePath()));
		}
	}
	
	/*====================================================================
	 * Convenience methods
	 *====================================================================*/
	private static ManglePeakListFilesOperation extractArguments(
		String[] args
	) {
		if (args == null || args.length < 1)
			return null;
		File peakDirectory = null;
		File parametersFile = null;
		File outputDirectory = null;
		for (int i=0; i<args.length; i++) {
			String argument = args[i];
			if (argument == null)
				return null;
			else {
				i++;
				if (i >= args.length)
					return null;
				String value = args[i];
				if (argument.equals("-peak"))
					peakDirectory = new File(value);
				else if (argument.equals("-params"))
					parametersFile = new File(value);
				else if (argument.equals("-output"))
					outputDirectory = new File(value);
			}
		}
		try {
			return new ManglePeakListFilesOperation(
				peakDirectory, parametersFile, outputDirectory);
		} catch (Throwable error) {
			error.printStackTrace();
			return null;
		}
	}
	
	private static Collection<File> findFiles(File directory) {
		if (directory == null || directory.canRead() == false ||
			directory.isDirectory() == false)
			return null;
		File[] files = directory.listFiles();
		if (files == null || files.length < 1)
			return null;
		// sort files alphabetically
		Arrays.sort(files);
		// add all found files to collection
		Collection<File> foundFiles = new ArrayList<File>();
		for (File file : files) {
			// recurse into subdirectories
			if (file.isDirectory()) {
				Collection<File> descendantFiles = findFiles(file);
				if (descendantFiles != null &&
					descendantFiles.isEmpty() == false)
					foundFiles.addAll(descendantFiles);
			} else foundFiles.add(file);
		}
		return foundFiles;
	}
	
	private static void die(String message) {
		die(message, null);
	}
	
	private static void die(String message, Throwable error) {
		if (message == null)
			message = "There was an error mangling filenames " +
				"for this set of peak list files";
		if (error == null) {
			if (message.endsWith(".") == false)
				message += ".";
		} else {
			if (message.endsWith("."))
				message = message.substring(0, message.length() - 1);
			if (message.endsWith(":") == false)
				message += ":";
		}
		System.err.println(message);
		if (error != null)
			error.printStackTrace();
		System.exit(1);
	}
}
