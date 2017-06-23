package edu.ucsd.mztab.ui;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.regex.Matcher;

import org.apache.commons.lang3.tuple.ImmutablePair;

import edu.ucsd.mztab.MzTabReader;
import edu.ucsd.mztab.TaskMzTabContext;
import edu.ucsd.mztab.model.MzTabConstants;
import edu.ucsd.mztab.model.MzTabSectionHeader;
import edu.ucsd.mztab.model.PSM;
import edu.ucsd.mztab.processors.PROXIProcessor;
import edu.ucsd.mztab.util.CommonUtils;
import edu.ucsd.mztab.util.DatabaseUtils;
import edu.ucsd.mztab.util.FileIOUtils;
import edu.ucsd.mztab.util.ProteomicsUtils;

public class MzTabPROXIImporter
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String USAGE =
		"java -cp MzTabUtils.jar edu.ucsd.mztab.ui.MzTabPROXIImporter" +
		"\n\t-mztab           <MzTabDirectory>" +
		"\n\t[-mztabPath      <MzTabRelativePath> " +
			"(if not under MzTabDirectory)]" +
		"\n\t[-peak           <PeakListFilesDirectory>]" +
		"\n\t[-peakPath       <PeakListRelativePath> " +
			"(if not under PeakListFilesDirectory)]" +
		"\n\t[-peakCollection <PeakListCollectionName> " +
			"(default \"peak_list_files\")]" +
		"\n\t-params          <ProteoSAFeParametersFile>" +
		"\n\t-task            <ProteoSAFeTaskID>" +
		"\n\t-dataset         <DatasetID>|<DatasetIDFile>" +
		"\n\t[-importByQValue true|false (default true)]";
	private static final String DATASET_ID_PREFIX = "MSV";
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public static void main(String[] args) {
		MzTabImportOperation importer = extractArguments(args);
		if (importer == null)
			die(USAGE);
		try {
			importDataset(importer.mzTabDirectory, importer.context,
				importer.taskID, importer.datasetID, importer.importByQValue,
				importer.start);
		} catch (Throwable error) {
			die(null, error);
		}
	}
	
	public static boolean importDataset(
		File mzTabDirectory, TaskMzTabContext context, String taskID,
		Integer datasetID, Boolean importByQValue, Long start
	) {
		if (mzTabDirectory == null || context == null || taskID == null)
			return false;
		// importByQValue defaults to true
		if (importByQValue == null)
			importByQValue = true;
		// start defaults to current time
		if (start == null)
			start = System.currentTimeMillis();
		// set up database connection
		Connection connection = DatabaseUtils.getConnection();
		if (connection == null)
			throw new NullPointerException(
				"Could not connect to the MassIVE search database server.");
		// read through all mzTab files, import content to database
		try {
			int filesImported = 0;
			long totalLines = 0;
			long totalPSMRows = 0;
			// recursively find mzTab files under the argument directory
			Collection<File> files = FileIOUtils.findFiles(mzTabDirectory);
			if (files == null || files.isEmpty()) {
				System.out.println(String.format("Could not find " +
					"any mzTab files to import under directory [%s].",
					mzTabDirectory.getAbsolutePath()));
				return false;
			}
			// import all found files
			System.out.println(String.format("Importing %d mzTab %s " +
				"into the MassIVE search database...\n----------",
				files.size(), CommonUtils.pluralize("file", files.size())));
			for (File file : files) {
				// try to import this file; null means it was not imported
				// for benign reasons, errors should throw an exception
				ImmutablePair<Integer, Integer> importCounts =
					importMzTabFile(file, context, taskID, datasetID,
						importByQValue, connection);
				if (importCounts == null)
					continue;
				filesImported++;
				totalLines += importCounts.getLeft();
				totalPSMRows += importCounts.getRight();
			}
			long elapsed = System.currentTimeMillis() - start;
			double seconds = elapsed / 1000.0;
			int notImported = files.size() - filesImported;
			System.out.println(String.format(
				"Imported %d mzTab %s%s into the MassIVE search database in %s " +
				"(%.2f lines/second, %.2f PSM rows/second).",
				filesImported, CommonUtils.pluralize("file", filesImported),
				notImported < 1 ? "" : String.format(
					" (%d skipped for having no importable PSMs)", notImported),
				CommonUtils.formatMilliseconds(elapsed),
				(totalLines / seconds), (totalPSMRows / seconds)));
			return true;
		} catch (Throwable error) {
			throw new RuntimeException(
				"Error importing mzTab content to the MassIVE search database.",
				error);
		} finally {
			try { connection.close(); } catch (Throwable error) {}
		}
	}
	
	public static ImmutablePair<Integer, Integer> importMzTabFile(
		File mzTabFile, TaskMzTabContext context, String taskID,
		Integer datasetID, Boolean importByQValue
	) {
		if (mzTabFile == null || context == null || taskID == null)
			return null;
		// set up database connection
		Connection connection = DatabaseUtils.getConnection();
		if (connection == null)
			throw new NullPointerException(
				"Could not connect to the MassIVE search database server.");
		try {
			return importMzTabFile(mzTabFile, context, taskID, datasetID,
				importByQValue, connection);
		} catch (Throwable error) {
			throw new RuntimeException(String.format(
				"Error importing mzTab file [%s] to the MassIVE search " +
				"database.", mzTabFile.getAbsolutePath()), error);
		} finally {
			try { connection.close(); } catch (Throwable error) {}
		}
	}
	
	public static ImmutablePair<Integer, Integer> importMzTabFile(
		File mzTabFile, TaskMzTabContext context, String taskID,
		Integer datasetID, Boolean importByQValue, Connection connection
	) {
		if (mzTabFile == null || context == null || taskID == null ||
			connection == null)
			return null;
		// importByQValue defaults to true
		if (importByQValue == null)
			importByQValue = true;
		// only import this mzTab file if it contains importable PSMs
		if (isImportable(mzTabFile, importByQValue) == false)
			return null;
		MzTabReader reader = new MzTabReader(context.getMzTabFile(mzTabFile));
		PROXIProcessor processor =
			new PROXIProcessor(taskID, datasetID, importByQValue, connection);
		reader.addProcessor(processor);
		reader.read();
		// return import counts:
		// left: total lines read in the mzTab file
		// right: total unique PSMs imported to search from the mzTab file
		return new ImmutablePair<Integer, Integer>(
			processor.getRowCount("lines_in_file"),
			processor.getRowCount("PSM"));
	}
	
	public static boolean isImportable(File mzTabFile, boolean importByQValue) {
		if (mzTabFile == null || mzTabFile.isFile() == false ||
			mzTabFile.canRead() == false)
			return false;
		// read through mzTab file to determine if
		// it contains any importable PSM rows
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(mzTabFile));
			String line = null;
			int lineNumber = 0;
			MzTabSectionHeader psmHeader = null;
			Integer validColumn = null;
			Integer qValueColumn = null;
			while (true) {
				line = reader.readLine();
				if (line == null)
					break;
				lineNumber++;
				// parse PSM section header to determine Q-Value column index
				if (line.startsWith("PSH")) {
					if (psmHeader != null)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:" +
							"\n----------\n%s\n----------\n" +
							"A \"PSH\" row was already seen previously in this file.",
							lineNumber, mzTabFile.getAbsolutePath(), line));
					psmHeader = new MzTabSectionHeader(line);
					// determine index of controlled validity column
					// (required for the file to be importable)
					validColumn =
						psmHeader.getColumnIndex(MzTabConstants.VALID_COLUMN);
					if (validColumn == null)
						return false;
					// determine index of controlled Q-value column
					// (required if importing by Q-Value)
					qValueColumn =
						psmHeader.getColumnIndex(MzTabConstants.Q_VALUE_COLUMN);
					if (importByQValue && qValueColumn == null)
						return false;
				}
				// check each PSM row to see if it's importable; as soon as
				// a fully validated importable row is found, return true
				else if (line.startsWith("PSM")) {
					if (psmHeader == null)
						throw new IllegalArgumentException(String.format(
							"Line %d of mzTab file [%s] is invalid:" +
							"\n----------\n%s\n----------\n" +
							"A \"PSM\" row was found before any \"PSH\" row.",
							lineNumber, mzTabFile.getAbsolutePath(), line));
					else psmHeader.validateMzTabRow(line);
					// check all the required elements of this PSM row
					String[] columns = line.split("\\t");
					// all importable PSMs must be marked as "VALID"
					String valid = columns[validColumn];
					if (valid == null ||
						valid.trim().equalsIgnoreCase("VALID") == false)
						continue;
					// if importing by Q-Value, determine
					// if this PSM makes the cut
					if (importByQValue) try {
						double qValue =
							Double.parseDouble(columns[qValueColumn]);
						if (qValue >
							MzTabConstants.DEFAULT_IMPORT_Q_VALUE_THRESHOLD)
							continue;
					} catch (Throwable error) {
						continue;
					}
					// try instantiating this row as a PSM object,
					// just as the importer would; if it builds
					// with no errors, then the row is good
					try {
						PSM psm = new PSM(
							columns[psmHeader.getColumnIndex("PSM_ID")],
							columns[psmHeader.getColumnIndex("spectra_ref")],
							columns[psmHeader.getColumnIndex("sequence")],
							columns[psmHeader.getColumnIndex("charge")],
							columns[psmHeader.getColumnIndex(
								"exp_mass_to_charge")],
							ProteomicsUtils.getModifications(
								columns[psmHeader.getColumnIndex(
									"modifications")])
						);
						// ensure that the parsed PSM's nativeID is acceptable
						Matcher matcher = MzTabConstants.QUERY_PATTERN.matcher(
							psm.getNativeID());
						if (matcher.matches())
							continue;
					} catch (Throwable error) {
						continue;
					}
					// if after all tests this PSM row was still found
					// to be importable, then the file is good
					return true;
				}
			}
			// if no importable PSM row was found anywhere
			// in the file, then it's not importable
			return false;
		} catch (RuntimeException error) {
			throw error;
		} catch (Throwable error) {
			throw new RuntimeException(error);
		} finally {
			try { reader.close(); } catch (Throwable error) {}
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
		private Boolean          importByQValue;
		private TaskMzTabContext context;
		private long             start;
		
		/*====================================================================
		 * Constructors
		 *====================================================================*/
		public MzTabImportOperation(
			File mzTabDirectory, String mzTabRelativePath,
			File peakListDirectory, String peakListRelativePath,
			String peakListCollection, File parameters,
			String taskID, String datasetID, String importByQValue
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
			// propagate importByQValue flag, if present (default true)
			if (importByQValue == null)
				this.importByQValue = true;
			else {
				this.importByQValue =
					CommonUtils.parseBooleanColumn(importByQValue);
				if (this.importByQValue == null)
					throw new IllegalArgumentException(String.format(
						"importByQValue argument [%s] could not be parsed " +
						"as a boolean value.", importByQValue));
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
				peakListDirectory, peakListRelativePath, peakListCollection,
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
		String peakListCollection = null;
		File parameters = null;
		String taskID = null;
		String datasetID = null;
		String importByQValue = null;
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
				} else if (argument.equals("-importByQValue"))
					importByQValue = value;
				else return null;
			}
		}
		try {
			return new MzTabImportOperation(
				mzTabDirectory, mzTabRelativePath,
				peakListDirectory, peakListRelativePath, peakListCollection,
				parameters, taskID, datasetID, importByQValue);
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
