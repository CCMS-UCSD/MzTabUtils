package edu.ucsd.mztab.ui;

import java.io.File;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.w3c.dom.Document;

import edu.ucsd.mztab.model.ProteoSAFeFileMappingContext;
import edu.ucsd.mztab.model.ProteoSAFeFileMappingContext.UploadMapping;
import edu.ucsd.mztab.util.FileIOUtils;

public class RemangleFiles
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String USAGE =
		"java -cp MzTabUtils.jar edu.ucsd.mztab.ui.RemangleFiles" +
		"\n\t-input  <CollectionDirectoriesInDataset> " +
			"(semicolon-delimited list)" +
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
			Collection<File> collectionDirectories, File parametersFile,
			File outputDirectory
		) {
			// validate input collection directories
			if (collectionDirectories == null ||
				collectionDirectories.isEmpty())
				throw new NullPointerException("At least one dataset " +
					"collection directory must be provided.");
			else for (File collectionDirectory : collectionDirectories) {
				if (collectionDirectory == null)
					throw new NullPointerException(
						"Dataset collection directory cannot be null.");
				else if (collectionDirectory.isDirectory() == false)
					throw new IllegalArgumentException(String.format(
						"Dataset collection directory [%s] must be a " +
						"directory.", collectionDirectory.getAbsolutePath()));
				else if (collectionDirectory.canRead() == false)
					throw new IllegalArgumentException(String.format(
						"Dataset collection directory [%s] must be readable.",
						collectionDirectory.getAbsolutePath()));
			}
			// determine input collection parameter
			String collectionParameter = null;
			for (File collectionDirectory : collectionDirectories) {
				String collectionName = collectionDirectory.getName();
				String thisParameter = null;
				if (collectionName.equals("peak") ||
					collectionName.equals("ccms_peak"))
					thisParameter = "peak_list_files";
				else if (collectionName.equals("result"))
					thisParameter = "result_files";
				// TODO: handle all other possible collections,
				// throw error if directory name isn't one of them
				if (thisParameter == null)
					throw new IllegalArgumentException(String.format(
						"Unrecognized dataset collection directory type: [%s],",
						collectionDirectory.getAbsolutePath()));
				// if two different kinds of collection directories
				// were provided, throw an error
				else if (collectionParameter != null &&
					collectionParameter.equals(thisParameter) == false)
					throw new IllegalArgumentException(String.format(
						"Improperly mixed dataset collection directory " +
						"types: expected all collections to be of type [%s], " +
						"yet found a collection of type [%s].",
						collectionParameter, thisParameter));
				else collectionParameter = thisParameter;
			}
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
			// recursively search input directories for all collection files
			Collection<File> collectionFiles = new LinkedHashSet<File>();
			for (File collectionDirectory : collectionDirectories) {
				Collection<File> theseFiles =
					FileIOUtils.findFiles(collectionDirectory);
				if (theseFiles != null && theseFiles.isEmpty() == false)
					collectionFiles.addAll(theseFiles);
			}
			// map all found collection files to their mangled names
			mangledFiles = new LinkedHashMap<String, File>();
			for (File collectionFile : collectionFiles) {
				// find the input collection that this file belongs to
				String filePath = FilenameUtils.separatorsToUnix(
					collectionFile.getAbsolutePath());
				File collectionDirectory = null;
				String collectionPath = null;
				for (File collection : collectionDirectories) {
					collectionPath = FilenameUtils.separatorsToUnix(
						collection.getAbsolutePath());
					if (filePath.startsWith(collectionPath)) {
						collectionDirectory = collection;
						break;
					}
				}
				// collection directory should never be null
				// at this point, since all files were found
				// under some collection directory
				if (collectionDirectory == null)
					throw new IllegalStateException();
				// get mangled filename prefix (should be the same as
				// the collection directory name, in all upper case)
				String mangledPrefix =
					collectionDirectory.getName().toUpperCase();
				// if the collection directory is a CCMS collection (i.e. its
				// name starts with a "ccms_" prefix), remove that, since the
				// params.xml mapping will use the original upload collection
//				if (mangledPrefix.startsWith("CCMS_"))
//					mangledPrefix = mangledPrefix.substring(5);
				// get this collection file's relative path
				// under the input collection files directory
				String fileRelativePath =
					filePath.substring(collectionPath.length());
				// chomp leading slash, if present
				if (fileRelativePath.isEmpty() == false &&
					fileRelativePath.charAt(0) == '/')
					fileRelativePath = fileRelativePath.substring(1);
				// prepend collection name
				fileRelativePath = String.format("%s/%s",
					collectionDirectory.getName(), fileRelativePath);
				// find this collection file's upload mapping
				String mangledFilename =
					context.getMangledFilename(fileRelativePath, mangledPrefix);
				// it's okay if no mangled filename was found, since this
				// might be a collection that is only partially used by a
				// dataset child task, e.g. update or reanalysis
				if (mangledFilename != null)
					mangledFiles.put(mangledFilename, collectionFile);
			}
			// verify that all params.xml mapped files are present
			// in one of the input collection files directories
			Collection<UploadMapping> uploadMappings =
				context.getUploadMappings(collectionParameter);
			Collection<UploadMapping> missingFiles =
				new LinkedHashSet<UploadMapping>(uploadMappings);
			for (UploadMapping uploadMapping : uploadMappings)
				if (mangledFiles.containsKey(
					uploadMapping.getMangledFilename())) {
					missingFiles.remove(uploadMapping);
				}
			// if even one params.xml mapped file was not found in any
			// of the input collection directories, then throw an error
			if (missingFiles.isEmpty() == false)
				throw new IllegalArgumentException(String.format(
					"Parameter file [%s] contained an " +
					"\"upload_file_mapping\" parameter for " +
					"collection file [%s], yet no such file was " +
					"found under any of the input collection directories.",
					parametersFile.getAbsolutePath(),
					missingFiles.iterator().next().getUploadFilePath()));
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
		Collection<File> collectionDirectories = new LinkedHashSet<File>();
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
				if (argument.equals("-input")) {
					String[] directories = value.split(";");
					for (String directory : directories)
						collectionDirectories.add(new File(directory));
				} else if (argument.equals("-params"))
					parametersFile = new File(value);
				else if (argument.equals("-output"))
					outputDirectory = new File(value);
			}
		}
		try {
			return new RemangleFilesOperation(
				collectionDirectories, parametersFile, outputDirectory);
		} catch (Throwable error) {
			error.printStackTrace();
			return null;
		}
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
