package edu.ucsd.mztab.ui;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import edu.ucsd.mztab.TaskMzTabContext;
import edu.ucsd.mztab.model.MzTabFile;
import edu.ucsd.mztab.util.FileIOUtils;

public class CopyReprocessedFiles
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String USAGE =
		"java -cp MzTabUtils.jar edu.ucsd.mztab.ui.CopyReprocessedFiles" +
		"\n\t-mztab           <MzTabDirectory>" +
		"\n\t[-mztabPath      <MzTabRelativePath>]" +
		"\n\t[-peak           <PeakListFilesDirectory>]" +
		"\n\t[-peakPath       <PeakListRelativePath>]" +
		"\n\t[-peakCollection <PeakListCollectionName> " +
			"(default \"peak_list_files\")]" +
		"\n\t-params          <ProteoSAFeParametersFile>" +
		"\n\t-output          <OutputDirectory>" +
		"\n\t[-dataset        <DatasetID>|<DatasetIDFile>]";
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public static void main(String[] args) {
		ReprocessedFileCopyOperation copy = extractArguments(args);
		if (copy == null)
			die(USAGE);
		// build mzTab file-mapping context from cleaned result directory
		TaskMzTabContext context = new TaskMzTabContext(
			copy.inputDirectory, copy.mzTabRelativePath,
			copy.peakListDirectory, copy.peakListRelativePath,
			copy.peakListCollection, copy.parameters, copy.datasetID);
		// recursively search cleaned result directory for result files
		Collection<File> resultFiles = findFiles(copy.inputDirectory);
		if (resultFiles == null || resultFiles.isEmpty())
			die(String.format("No result files could be " +
				"found in cleaned result directory [%s].",
				copy.inputDirectory.getAbsolutePath()));
		// sort files alphabetically
		File[] files = resultFiles.toArray(new File[resultFiles.size()]);
		Arrays.sort(files);
		// copy all files to their final destination,
		// preserving directory structure
		for (File file : files) {
			// get final destination file
			MzTabFile mzTabFile = context.getMzTabFile(file);
			String mzTabPath = mzTabFile.getMappedMzTabPath();
			if (mzTabPath == null)
				mzTabPath = file.getName();
			File destinationFile = new File(copy.outputDirectory, mzTabPath);
			System.out.println(String.format("Copying cleaned " +
				"mzTab file [%s] to final output file [%s]...",
				file.getAbsolutePath(), destinationFile.getAbsolutePath()));
			try {
				FileIOUtils.copyFile(file,
					destinationFile.getParentFile(), destinationFile.getName());
			} catch (Throwable error) {
				die(String.format("Could not copy cleaned mzTab " +
					"file [%s] to final output file [%s].",
					file.getAbsolutePath(),
					destinationFile.getAbsolutePath()));
			}
		}
	}
	
	/*========================================================================
	 * Convenience classes
	 *========================================================================*/
	/**
	 * Struct to maintain context data for each
	 * reprocessed result file copy operation.
	 */
	private static class ReprocessedFileCopyOperation {
		/*====================================================================
		 * Properties
		 *====================================================================*/
		private File    inputDirectory;
		private File    outputDirectory;
		private File    peakListDirectory;
		private File    parameters;
		private String  mzTabRelativePath;
		private String  peakListRelativePath;
		private String  peakListCollection;
		private String  datasetID;
		
		/*====================================================================
		 * Constructors
		 *====================================================================*/
		public ReprocessedFileCopyOperation(
			File inputDirectory, String mzTabRelativePath,
			File peakListDirectory, String peakListRelativePath,
			String peakListCollection, File parameters,
			File outputDirectory, String datasetID
		) {
			// validate input directory
			if (inputDirectory == null)
				throw new NullPointerException(
					"Input directory cannot be null.");
			else if (inputDirectory.isDirectory() == false)
				throw new IllegalArgumentException(
					String.format("Input directory [%s] must be a directory.",
						inputDirectory.getAbsolutePath()));
			else if (inputDirectory.canRead() == false)
				throw new IllegalArgumentException(
					String.format("Input directory [%s] must be readable.",
						inputDirectory.getAbsolutePath()));
			else this.inputDirectory = inputDirectory;
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
			this.peakListDirectory = peakListDirectory;
			// validate params.xml file
			if (parameters == null)
				throw new NullPointerException(
					"Argument params.xml file cannot be null.");
			else if (parameters.isFile() == false ||
				parameters.canRead() == false)
				throw new IllegalArgumentException(
					"Argument params.xml file must be a readable file.");
			else this.parameters = parameters;
			// validate final mzTab output directory
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
			// initialize file context properties (any or all may be null)
			this.mzTabRelativePath = mzTabRelativePath;
			this.peakListRelativePath = peakListRelativePath;
			this.peakListCollection = peakListCollection;
			this.datasetID = datasetID;
		}
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private static ReprocessedFileCopyOperation extractArguments(String[] args) {
		if (args == null || args.length < 1)
			return null;
		File mzTabDirectory = null;
		String mzTabRelativePath = null;
		File peakListDirectory = null;
		String peakListRelativePath = null;
		String peakListCollection = null;
		File parameters = null;
		File outputDirectory = null;
		String datasetID = null;
		for (int i=0; i<args.length; i++) {
			String argument = args[i];
			if (argument == null)
				return null;
			else {
				i++;
				if (i >= args.length)
					return null;
				String value = args[i];
				if (argument.equals("-mztab"))
					mzTabDirectory = new File(value);
				else if (argument.equals("-mztabPath"))
					mzTabRelativePath = value;
				else if (argument.equals("-peak"))
					peakListDirectory = new File(value);
				else if (argument.equals("-peakPath"))
					peakListRelativePath = value;
				else if (argument.equals("-peakCollection"))
					peakListCollection = value;
				else if (argument.equals("-params"))
					parameters = new File(value);
				else if (argument.equals("-output"))
					outputDirectory = new File(value);
				else if (argument.equals("-dataset")) {
					// if this argument is a file, read it to get dataset ID
					File datasetIDFile = new File(value);
					if (datasetIDFile.isFile() && datasetIDFile.canRead()) {
						RandomAccessFile input = null;
						try {
							input = new RandomAccessFile(datasetIDFile, "r");
							datasetID = input.readLine();
						} catch (Throwable error) {
							die(String.format(
								"Could not read dataset ID from file [%s].",
								datasetIDFile.getAbsolutePath()), error);
						} finally {
							try { input.close(); } catch (Throwable error) {}
						}
					}
					// otherwise treat the argument as the literal dataset ID
					else datasetID = value;
				} else {
					System.err.println(String.format(
						"Unrecognized argument [%s].", argument));
					return null;
				}
			}
		}
		try {
			return new ReprocessedFileCopyOperation(
				mzTabDirectory, mzTabRelativePath,
				peakListDirectory, peakListRelativePath, peakListCollection,
				parameters, outputDirectory, datasetID);
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
			message = "There was an error copying this set " +
				"of reprocessed result files";
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
