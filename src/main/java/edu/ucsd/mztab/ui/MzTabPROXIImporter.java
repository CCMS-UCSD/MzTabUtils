package edu.ucsd.mztab.ui;

import java.io.File;
import java.sql.Connection;
import java.util.Arrays;

import edu.ucsd.mztab.MzTabReader;
import edu.ucsd.mztab.TaskMzTabContext;
import edu.ucsd.mztab.processors.PROXIProcessor;
import edu.ucsd.util.CommonUtils;
import edu.ucsd.util.DatabaseUtils;

public class MzTabPROXIImporter
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String USAGE =
		"java -cp MzTabUtils.jar edu.ucsd.mztab.ui.MzTabPROXIImporter" +
		"\n\t-mztab    <MzTabDirectory>" +
		"\n\t-peak     <PeakListFilesDirectory>" +
		"\n\t-params   <ProteoSAFeParametersFile>" +
		"\n\t[-dataset <TaskID> <DatasetID>]" +
		"\n\t[-task    <TaskID> <Username>]";
	public static final String DATASET_ID_PREFIX = "MSV";
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public static void main(String[] args) {
		MzTabImportOperation importer = extractArguments(args);
		if (importer == null)
			die(USAGE);
		// set up database connection
		Connection connection = DatabaseUtils.getConnection();
		if (connection == null)
			die("Could not connect to the PROXI database server");
		// read through all mzTab files, import content to database
		try {
			long start = System.currentTimeMillis();
			// parse out file mapping context for this task from params.xml
			TaskMzTabContext context = null;
			if (importer.datasetID == null)
				context = new TaskMzTabContext(
					importer.mzTabDirectory, importer.peakListDirectory,
					importer.parameters);
			else context = new TaskMzTabContext(
					importer.mzTabDirectory, importer.peakListDirectory,
					importer.parameters, importer.username);
			System.out.println(String.format(
				"Parsed files in mzTab directory [%s] and parameters file " +
				"[%s] to build ProteoSAFe task filename context in %s.",
				importer.mzTabDirectory.getAbsolutePath(),
				importer.parameters.getAbsolutePath(),
				CommonUtils.formatMilliseconds(
					System.currentTimeMillis() - start)));
			File[] files = importer.mzTabDirectory.listFiles();
			// sort files alphabetically
			Arrays.sort(files);
			System.out.println(String.format(
				"Importing %d mzTab %s into the PROXI database...\n----------",
				files.length, CommonUtils.pluralize("file", files.length)));
			for (File file : files) {
				MzTabReader reader =
					new MzTabReader(context.getMzTabFile(file));
				reader.addProcessor(new PROXIProcessor(
					importer.taskID, importer.datasetID, connection));
				reader.read();
			}
			System.out.println(String.format(
				"Imported %d mzTab %s into the PROXI database in %s.",
				files.length, CommonUtils.pluralize("file", files.length),
				CommonUtils.formatMilliseconds(
					System.currentTimeMillis() - start)));
		} catch (Throwable error) {
			die("Error importing mzTab content to the PROXI database", error);
		} finally {
			try { connection.close(); } catch (Throwable error) {}
		}
	}
	
	/*========================================================================
	 * Convenience classes
	 *========================================================================*/
	/**
	 * Struct to maintain context data for each mzTab PROXI import operation.
	 */
	private static class MzTabImportOperation {
		/*====================================================================
		 * Constants
		 *====================================================================*/
		private static enum MzTabImportMode { DATASET, TASK }
		
		/*====================================================================
		 * Properties
		 *====================================================================*/
		private MzTabImportMode mode;
		private File            mzTabDirectory;
		private File            peakListDirectory;
		private File            parameters;
		private String          taskID;
		private String          username;
		private Integer         datasetID;
		
		/*====================================================================
		 * Constructors
		 *====================================================================*/
		public MzTabImportOperation(
			String mode, File mzTabDirectory, File peakListDirectory,
			File parameters, String taskID, String datasetID, String username
		) {
			// initialize nullable properties
			this.datasetID = null;
			// validate task ID
			if (taskID == null)
				throw new NullPointerException("Task ID cannot be null.");
			else this.taskID = taskID;
			// validate import mode
			if (mode == null)
				throw new NullPointerException(
					"MzTab import mode cannot be null.");
			else this.mode = MzTabImportMode.valueOf(mode.toUpperCase());
			// if mode is "dataset", then datasetID is required
			if (this.mode.equals(MzTabImportMode.DATASET)) {
				if (datasetID == null)
					throw new NullPointerException("Dataset ID cannot be " +
						"null when importing dataset mzTab files.");
				else this.username = datasetID;
				// record task and dataset ID
				try { this.datasetID = parseDatasetIDString(datasetID); }
				catch (Throwable error) {
					throw new IllegalArgumentException(String.format(
						"Dataset ID string [%s] could not be parsed into " +
						"a valid dataset ID.", datasetID), error);
				}
			}
			// if mode is "task", then username is required
			else if (this.mode.equals(MzTabImportMode.TASK)) {
				if (username == null)
					throw new NullPointerException("Username cannot be null " +
						"when importing workflow result mzTab files.");
				else this.username = username;
			}
			// the only recognized import modes are "dataset" and "task"
			else throw new IllegalArgumentException(String.format(
				"Unrecognized mzTab import mode [%s].", this.mode.name()));
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
			// validate peak list files directory
			if (peakListDirectory == null)
				throw new NullPointerException(
					"Peak list files directory cannot be null.");
			else if (peakListDirectory.isDirectory() == false)
				throw new IllegalArgumentException(String.format(
					"Peak list files directory [%s] must be a directory.",
					peakListDirectory.getAbsolutePath()));
			else if (peakListDirectory.canRead() == false)
				throw new IllegalArgumentException(String.format(
					"Peak list files directory [%s] must be readable.",
					peakListDirectory.getAbsolutePath()));
			else this.peakListDirectory = peakListDirectory;
			// validate params.xml file
			if (parameters == null)
				throw new NullPointerException(
					"Argument params.xml file cannot be null.");
			else if (parameters.isFile() == false ||
				parameters.canRead() == false)
				throw new IllegalArgumentException(
					"Argument params.xml file must be a readable file.");
			else this.parameters = parameters;
		}
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private static MzTabImportOperation extractArguments(String[] args) {
		if (args == null || args.length < 1)
			return null;
		String mode = null;
		File mzTabDirectory = null;
		File peakListDirectory = null;
		File parameters = null;
		String taskID = null;
		String datasetID = null;
		String username = null;
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
				else if (argument.equals("-peak"))
					peakListDirectory = new File(value);
				else if (argument.equals("-params"))
					parameters = new File(value);
				else if (argument.equals("-dataset")) {
					mode = "DATASET";
					taskID = value;
					// next argument is relevant in this case
					i++;
					if (i >= args.length)
						return null;
					datasetID = args[i];
				} else if (argument.equals("-task")) {
					mode = "TASK";
					taskID = value;
					// next argument is relevant in this case
					i++;
					if (i >= args.length)
						return null;
					username = args[i];
				} else return null;
			}
		}
		try {
			return new MzTabImportOperation(
				mode, mzTabDirectory, peakListDirectory,
				parameters, taskID, datasetID, username);
		} catch (Throwable error) {
			System.err.println(error.getMessage());
			return null;
		}
	}
	
	private static int parseDatasetIDString(String datasetID) {
		if (datasetID == null)
			throw new NullPointerException("Dataset ID string cannot be null.");
		else if (datasetID.startsWith(DATASET_ID_PREFIX) == false)
			throw new IllegalArgumentException(String.format(
				"Dataset ID string [%s] does not begin with " +
				"the required MassIVE dataset ID prefix \"%s\".",
				datasetID, DATASET_ID_PREFIX));
		else try {
			return Integer.parseInt(
				datasetID.substring(DATASET_ID_PREFIX.length()));
		} catch (NumberFormatException error) {
			throw new IllegalArgumentException(String.format(
				"Dataset ID string [%s] does not end with " +
				"a valid integer ID.", datasetID), error);
		}
	}
	
	private static void die(String message) {
		die(message, null);
	}
	
	private static void die(String message, Throwable error) {
		if (message == null)
			message = "There was an error generating statistics " +
				"on this set of mzTab files";
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
