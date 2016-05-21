package edu.ucsd.mztab.ui;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Arrays;

import edu.ucsd.mztab.MzTabReader;
import edu.ucsd.mztab.TaskMzTabContext;
import edu.ucsd.mztab.processors.MsRunCleanProcessor;
import edu.ucsd.mztab.processors.ValidityProcessor;

public class ProteoSAFeMzTabCleaner
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String USAGE =
		"java -cp MzTabUtils.jar edu.ucsd.mztab.ui.ProteoSAFeMzTabCleaner" +
		"\n\t-mztab      <MzTabDirectory>" +
		"\n\t[-mztabPath <MzTabRelativePath> (if not under MzTabDirectory)]" +
		"\n\t[-peak      <PeakListFilesDirectory>]" +
		"\n\t[-peakPath  <PeakListRelativePath> " +
			"(if not under PeakListFilesDirectory)]" +
		"\n\t-params     <ProteoSAFeParametersFile>" +
		"\n\t-output     <CleanedMzTabDirectory>" +
		"\n\t[-dataset   <DatasetID>|<DatasetIDFile>]";
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public static void main(String[] args) {
		ProteoSAFeMzTabCleanupOperation cleanup = extractArguments(args);
		if (cleanup == null)
			die(USAGE);
		// read through all mzTab files, run all relevant
		// MassIVE cleanup operations on each
		File[] files = cleanup.mzTabDirectory.listFiles();
		// sort files alphabetically
		Arrays.sort(files);
		for (File file : files) {
			// set up output file
			File outputFile = new File(cleanup.outputDirectory, file.getName());
			// set up reader
			MzTabReader reader =
				new MzTabReader(cleanup.context.getMzTabFile(file), outputFile);
			// clean all ms_run-location file references to use
			// fully qualified ProteoSAFe file descriptor paths
			reader.addProcessor(new MsRunCleanProcessor());
			// ensure that each PSM row has the special columns
			// needed by ProteoSAFe to ensure validity
			reader.addProcessor(new ValidityProcessor());
			// clean file
			reader.read();
		}
	}
	
	/*========================================================================
	 * Convenience classes
	 *========================================================================*/
	/**
	 * Struct to maintain context data for each mzTab ms_run cleanup operation.
	 */
	private static class ProteoSAFeMzTabCleanupOperation {
		/*====================================================================
		 * Properties
		 *====================================================================*/
		private File             mzTabDirectory;
		private File             outputDirectory;
		private TaskMzTabContext context;
		
		/*====================================================================
		 * Constructors
		 *====================================================================*/
		public ProteoSAFeMzTabCleanupOperation(
			File mzTabDirectory, String mzTabRelativePath,
			File peakListDirectory, String peakListRelativePath,
			File parameters, File outputDirectory, String datasetID
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
			else this.mzTabDirectory = mzTabDirectory;
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
			if (parameters == null)
				throw new NullPointerException(
					"Argument params.xml file cannot be null.");
			else if (parameters.isFile() == false ||
				parameters.canRead() == false)
				throw new IllegalArgumentException(
					"Argument params.xml file must be a readable file.");
			// validate cleaned mzTab output directory
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
			// build mzTab file-mapping context
			context = new TaskMzTabContext(
				mzTabDirectory, mzTabRelativePath,
				peakListDirectory, peakListRelativePath,
				parameters, datasetID);
		}
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private static ProteoSAFeMzTabCleanupOperation extractArguments(String[] args) {
		if (args == null || args.length < 1)
			return null;
		File mzTabDirectory = null;
		String mzTabRelativePath = null;
		File peakListDirectory = null;
		String peakListRelativePath = null;
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
				}
				else return null;
			}
		}
		try {
			return new ProteoSAFeMzTabCleanupOperation(
				mzTabDirectory, mzTabRelativePath,
				peakListDirectory, peakListRelativePath,
				parameters, outputDirectory, datasetID);
		} catch (Throwable error) {
			System.err.println(error.getMessage());
			return null;
		}
	}
	
	private static void die(String message) {
		die(message, null);
	}
	
	private static void die(String message, Throwable error) {
		if (message == null)
			message = "There was an error cleaning up ms_run locations " +
				"within this set of mzTab files";
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
