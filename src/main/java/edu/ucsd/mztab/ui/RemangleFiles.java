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

public class RemangleFiles
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String USAGE =
		"java -cp MzTabUtils.jar edu.ucsd.mztab.ui.RemangleFiles" +
		"\n\t-input  <CollectionDirectoryInDataset>" +
		"\n\t-params <ProteoSAFeParametersFile>" +
		"\n\t-output <MangledLinksDirectory>";
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public static void main(String[] args) {
		RemangleFilesOperation mangle = extractArguments(args);
		if (mangle == null)
			die(USAGE);
		// link each peak list file to its mangled filename
		// within the designated output directory
		for (String mangledFilename : mangle.mangledFiles.keySet()) {
			File collectionFile = mangle.mangledFiles.get(mangledFilename);
			File mangledFileLink =
				new File(mangle.outputDirectory, mangledFilename);
			FileIOUtils.makeLink(collectionFile, mangledFileLink);
		}
	}
	
	/*========================================================================
	 * Convenience classes
	 *========================================================================*/
	/**
	 * Struct to maintain context data for each
	 * dataset file collection re-mangling operation.
	 */
	private static class RemangleFilesOperation {
		/*====================================================================
		 * Properties
		 *====================================================================*/
		private Map<String, File> mangledFiles;
		private File              outputDirectory;
		
		/*====================================================================
		 * Constructors
		 *====================================================================*/
		public RemangleFilesOperation(
			File collectionDirectory, File parametersFile, File outputDirectory
		) {
			// validate input collection directory
			if (collectionDirectory == null)
				throw new NullPointerException(
					"Dataset collection directory cannot be null.");
			else if (collectionDirectory.isDirectory() == false)
				throw new IllegalArgumentException(String.format(
					"Dataset collection directory [%s] must be a directory.",
					collectionDirectory.getAbsolutePath()));
			else if (collectionDirectory.canRead() == false)
				throw new IllegalArgumentException(String.format(
					"Dataset collection directory [%s] must be readable.",
					collectionDirectory.getAbsolutePath()));
			// validate params.xml file
			if (parametersFile == null)
				throw new NullPointerException(
					"Argument params.xml file cannot be null.");
			else if (parametersFile.isFile() == false ||
				parametersFile.canRead() == false)
				throw new IllegalArgumentException(
					"Argument params.xml file must be a readable file.");
			// validate mangled links output directory
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
			// recursively search input directory for all collection files
			Collection<File> collectionFiles = findFiles(collectionDirectory);
			// map all found collection files to their mangled names
			mangledFiles = new LinkedHashMap<String, File>();
			String collectionRoot = collectionDirectory.getAbsolutePath();
			for (File collectionFile : collectionFiles) {
				// get this collection file's relative path
				// under the input collection files directory
				String fileRelativePath =
					collectionFile.getAbsolutePath().substring(
						collectionRoot.length());
				// chomp leading slash, if present
				if (fileRelativePath.isEmpty() == false &&
					fileRelativePath.charAt(0) == File.separatorChar)
					fileRelativePath = fileRelativePath.substring(1);
				// find this collection file's upload mapping
				String mangledFilename =
					context.getMangledFilename(fileRelativePath);
				if (mangledFilename == null)
					throw new IllegalArgumentException(String.format(
						"No \"upload_file_mapping\" parameter could be found " +
						"for collection file [%s] in parameters file [%s].",
						collectionFile.getAbsolutePath(),
						parametersFile.getAbsolutePath()));
				else mangledFiles.put(mangledFilename, collectionFile);
			}
			// verify that all params.xml mapped files are present
			// in the input collection files directory
			String collectionName = collectionDirectory.getName();
			String collectionParameter = null;
			if (collectionName.equals("peak"))
				collectionParameter = "peak_list_files";
			else if (collectionName.equals("result"))
				collectionParameter = "result_files";
			// TODO: handle all other possible collections,
			// throw error if directory name isn't one of them
			for (UploadMapping uploadMapping : context.getUploadMappings(
				collectionParameter))
				if (mangledFiles.containsKey(uploadMapping.getMangledFilename())
					== false)
					throw new IllegalArgumentException(String.format(
						"Parameter file [%s] contained an " +
						"\"upload_file_mapping\" parameter for collection " +
						"file [%s], yet no such file was found under input " +
						"collection files directory [%s].",
						parametersFile.getAbsolutePath(),
						uploadMapping.getUploadFilePath(),
						collectionDirectory.getAbsolutePath()));
		}
	}
	
	/*====================================================================
	 * Convenience methods
	 *====================================================================*/
	private static RemangleFilesOperation extractArguments(
		String[] args
	) {
		if (args == null || args.length < 1)
			return null;
		File collectionDirectory = null;
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
				if (argument.equals("-input"))
					collectionDirectory = new File(value);
				else if (argument.equals("-params"))
					parametersFile = new File(value);
				else if (argument.equals("-output"))
					outputDirectory = new File(value);
			}
		}
		try {
			return new RemangleFilesOperation(
				collectionDirectory, parametersFile, outputDirectory);
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
