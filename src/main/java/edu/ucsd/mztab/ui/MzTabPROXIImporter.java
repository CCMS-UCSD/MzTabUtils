package edu.ucsd.mztab.ui;

import java.io.File;
import java.sql.Connection;
import java.util.Arrays;

import edu.ucsd.mztab.MzTabReader;
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
		"\n\t[-dataset <TaskID> <DatasetID> <DatasetRelativePath>]" +
		"\n\t[-task    <TaskID> <Username> <TaskRelativePath>]" +
		"\n\t[-user    <Username> <UserSpaceRelativePath>]";
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
			File[] files = importer.mzTabDirectory.listFiles();
			// sort files alphabetically
			Arrays.sort(files);
			System.out.println(String.format(
				"Importing %d mzTab %s into the PROXI database...\n----------",
				files.length, CommonUtils.pluralize("file", files.length)));
			for (File file : files) {
				MzTabReader reader = new MzTabReader(file);
				String descriptor = String.format(
					"%s/%s", importer.descriptorBase, file.getName());
				reader.addProcessor(new PROXIProcessor(descriptor,
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
		private static enum MzTabImportMode { DATASET, TASK, USER }
		
		/*====================================================================
		 * Properties
		 *====================================================================*/
		private MzTabImportMode mode;
		private File            mzTabDirectory;
		private String          descriptorBase;
		private String          taskID;
		private Integer         datasetID;
		
		/*====================================================================
		 * Constructors
		 *====================================================================*/
		public MzTabImportOperation(
			String mode, File mzTabDirectory,
			String datasetID, String taskID, String username, String path
		) {
			// initialize nullable properties
			this.taskID = null;
			this.datasetID = null;
			// validate import mode
			if (mode == null)
				throw new NullPointerException(
					"MzTab import mode cannot be null.");
			else this.mode = MzTabImportMode.valueOf(mode.toUpperCase());
			// if mode is "dataset", then taskID and datasetID are required
			if (this.mode.equals(MzTabImportMode.DATASET)) {
				if (datasetID == null)
					throw new NullPointerException("Dataset ID cannot be " +
						"null when importing dataset mzTab files.");
				else if (taskID == null)
					throw new NullPointerException("Task ID cannot be null " +
						"when importing dataset mzTab files.");
				// record task and dataset ID
				this.taskID = taskID;
				try { this.datasetID = parseDatasetIDString(datasetID); }
				catch (Throwable error) {
					throw new IllegalArgumentException(String.format(
						"Dataset ID string [%s] could not be parsed into " +
						"a valid dataset ID.", datasetID), error);
				}
				// formulate mzTab file descriptor prefix
				StringBuffer descriptorBase = new StringBuffer("m.");
				descriptorBase.append(datasetID);
				if (path != null && path.trim().equals("") == false)
					descriptorBase.append("/").append(path);
				// chomp trailing slash, if present
				if (descriptorBase.charAt(descriptorBase.length() - 1) == '/')
					descriptorBase.setLength(descriptorBase.length() - 1);
				this.descriptorBase = descriptorBase.toString();
			}
			// if mode is "task", then taskID and username are required
			else if (this.mode.equals(MzTabImportMode.TASK)) {
				if (taskID == null)
					throw new NullPointerException("Task ID cannot be null " +
						"when importing workflow result mzTab files.");
				else if (username == null)
					throw new NullPointerException("Username cannot be null " +
						"when importing workflow result mzTab files.");
				// record task ID
				this.taskID = taskID;
				// formulate mzTab file descriptor prefix
				StringBuffer descriptorBase = new StringBuffer("u.");
				descriptorBase.append(username).append("/").append(taskID);
				if (path != null && path.trim().equals("") == false)
					descriptorBase.append("/").append(path);
				// chomp trailing slash, if present
				if (descriptorBase.charAt(descriptorBase.length() - 1) == '/')
					descriptorBase.setLength(descriptorBase.length() - 1);
				this.descriptorBase = descriptorBase.toString();
			}
			// if mode is "user", then username is required
			else if (this.mode.equals(MzTabImportMode.USER)) {
				if (username == null)
					throw new NullPointerException("Username cannot be null " +
						"when importing user-uploaded mzTab files.");
				// formulate mzTab file descriptor prefix
				StringBuffer descriptorBase = new StringBuffer("f.");
				descriptorBase.append(username);
				if (path != null && path.trim().equals("") == false)
					descriptorBase.append("/").append(path);
				// chomp trailing slash, if present
				if (descriptorBase.charAt(descriptorBase.length() - 1) == '/')
					descriptorBase.setLength(descriptorBase.length() - 1);
				this.descriptorBase = descriptorBase.toString();
			} else throw new IllegalArgumentException(String.format(
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
		String datasetID = null;
		String taskID = null;
		String username = null;
		String path = null;
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
				else if (argument.equals("-dataset")) {
					mode = "DATASET";
					taskID = value;
					// next two arguments are relevant in this case
					i++;
					if (i >= args.length)
						return null;
					datasetID = args[i];
					i++;
					if (i < args.length)
						path = args[i];
				} else if (argument.equals("-task")) {
					mode = "TASK";
					taskID = value;
					// next two arguments are relevant in this case
					i++;
					if (i >= args.length)
						return null;
					username = args[i];
					i++;
					if (i < args.length)
						path = args[i];
				} else if (argument.equals("-user")) {
					mode = "USER";
					username = value;
					// next argument is relevant in this case
					i++;
					if (i < args.length)
						path = args[i];
				}
				else return null;
			}
		}
		try {
			return new MzTabImportOperation(
				mode, mzTabDirectory, datasetID, taskID, username, path);
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
