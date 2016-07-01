package edu.ucsd.mztab.ui;

import java.io.File;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

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
		"\n\t-mztab      <MzTabDirectory>" +
		"\n\t[-mztabPath <MzTabRelativePath> (if not under MzTabDirectory)]" +
		"\n\t[-peak      <PeakListFilesDirectory>]" +
		"\n\t[-peakPath  <PeakListRelativePath> " +
			"(if not under PeakListFilesDirectory)]" +
		"\n\t-params     <ProteoSAFeParametersFile>" +
		"\n\t-task       <ProteoSAFeTaskID>" +
		"\n\t-dataset    <DatasetID>|<DatasetIDFile>";
	private static final String DATASET_ID_PREFIX = "MSV";
	
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
			long totalLines = 0;
			long totalPSMRows = 0;
			File[] files = importer.mzTabDirectory.listFiles();
			// sort files alphabetically
			Arrays.sort(files);
			System.out.println(String.format(
				"Importing %d mzTab %s into the PROXI database...\n----------",
				files.length, CommonUtils.pluralize("file", files.length)));
			for (File file : files) {
				MzTabReader reader =
					new MzTabReader(importer.context.getMzTabFile(file));
				PROXIProcessor processor = new PROXIProcessor(
					importer.taskID, importer.datasetID, connection);
				reader.addProcessor(processor);
				reader.read();
				totalLines += processor.getRowCount("lines_in_file");
				totalPSMRows += processor.getRowCount("PSM");
			}
			long elapsed = System.currentTimeMillis() - importer.start;
			double seconds = elapsed / 1000.0;
			System.out.println(String.format(
				"Imported %d mzTab %s into the PROXI database in %s " +
				"(%.2f lines/second, %.2f PSM rows/second).",
				files.length, CommonUtils.pluralize("file", files.length),
				CommonUtils.formatMilliseconds(elapsed),
				(totalLines / seconds), (totalPSMRows / seconds)));
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
		 * Properties
		 *====================================================================*/
		private File             mzTabDirectory;
		private String           taskID;
		private Integer          datasetID;
		private TaskMzTabContext context;
		private long             start;
		
		/*====================================================================
		 * Constructors
		 *====================================================================*/
		public MzTabImportOperation(
			File mzTabDirectory, String mzTabRelativePath,
			File peakListDirectory, String peakListRelativePath,
			File parameters, String taskID, String datasetID
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
			// validate task ID
			if (taskID == null)
				throw new NullPointerException(
					"Argument task ID cannot be null.");
			else this.taskID = taskID;
			// validate dataset ID
			if (datasetID == null)
				throw new NullPointerException(
					"Argument dataset ID cannot be null.");
			else try { this.datasetID = parseDatasetIDString(datasetID); }
			catch (Throwable error) {
				throw new IllegalArgumentException(String.format(
					"Dataset ID string [%s] could not be parsed into " +
					"a valid dataset ID.", datasetID), error);
			}
			// timestamp the beginning of the procedure
			start = System.currentTimeMillis();
			System.out.println(String.format(
				"Beginning mzTab PROXI import procedure at %s.",
				new SimpleDateFormat("hh:mm:ss.SSS a, EEEE, MMMM d, yyyy")
					.format(new Date(start))));
			// build mzTab file-mapping context
			context = new TaskMzTabContext(
				mzTabDirectory, mzTabRelativePath,
				peakListDirectory, peakListRelativePath,
				parameters, datasetID);
			System.out.println(String.format(
				"Parsed files in mzTab directory [%s] and parameters file " +
				"[%s] to build ProteoSAFe task filename context in %s.",
				mzTabDirectory.getAbsolutePath(), parameters.getAbsolutePath(),
				CommonUtils.formatMilliseconds(
					System.currentTimeMillis() - start)));
		}
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private static MzTabImportOperation extractArguments(String[] args) {
		if (args == null || args.length < 1)
			return null;
		File mzTabDirectory = null;
		String mzTabRelativePath = null;
		File peakListDirectory = null;
		String peakListRelativePath = null;
		File parameters = null;
		String taskID = null;
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
				else if (argument.equals("-task"))
					taskID = value;
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
				} else return null;
			}
		}
		try {
			return new MzTabImportOperation(
				mzTabDirectory, mzTabRelativePath,
				peakListDirectory, peakListRelativePath,
				parameters, taskID, datasetID);
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
