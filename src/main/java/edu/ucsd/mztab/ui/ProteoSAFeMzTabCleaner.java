package edu.ucsd.mztab.ui;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;

import edu.ucsd.mztab.MzTabReader;
import edu.ucsd.mztab.TaskMzTabContext;
import edu.ucsd.mztab.model.MzTabFile;
import edu.ucsd.mztab.processors.MsRunCleanProcessor;
import edu.ucsd.mztab.processors.FDRCalculationProcessor;
import edu.ucsd.mztab.processors.ValidityProcessor;
import edu.ucsd.util.CommonUtils;

public class ProteoSAFeMzTabCleaner
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String USAGE =
		"java -cp MzTabUtils.jar edu.ucsd.mztab.ui.ProteoSAFeMzTabCleaner" +
		"\n\t[-mztab         <MzTabDirectory>" +
			"(if not provided, then cleanup is skipped]" +
		"\n\t[-mztabPath     <MzTabRelativePath> " +
			"(if not under MzTabDirectory)]" +
		"\n\t[-peak          <PeakListFilesDirectory>]" +
		"\n\t[-peakPath      <PeakListRelativePath> " +
			"(if not under PeakListFilesDirectory)]" +
		"\n\t-params         <ProteoSAFeParametersFile>" +
		"\n\t-output         <CleanedMzTabDirectory>" +
		"\n\t[-dataset       <DatasetID>|<DatasetIDFile>]" +
		"\n\t[-passThreshold <PassThresholdColumn>]" +
		"\n\t[-decoy         <IsDecoyColumn>]" +
		"\n\t[-decoyPattern  <SubstringIndicatingDecoy>]" +
		"\n\t[-qvalue        <QValueColumn>]" +
		"\n\t[-filter        true/false (default false; " +
			"if specified, PSM rows not meeting the FDR " +
			"threshold will be removed from the file)]" +
		"\n\t[-filterFDR     0-1 (if not specified, and filter=true, then " +
			"only PSMs marked as decoy or passThreshold=false will be removed]";
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public static void main(String[] args) {
		ProteoSAFeMzTabCleanupOperation cleanup = extractArguments(args);
		if (cleanup == null)
			die(USAGE);
		// if no mzTab files were found, then this is a partial
		// submission and no cleanup needs to occur
		if (cleanup.context == null)
			return;
		// otherwise, read through all mzTab files, and run
		// all relevant MassIVE cleanup operations on each
		File[] files = cleanup.mzTabDirectory.listFiles();
		// sort files alphabetically
		Arrays.sort(files);
		for (File file : files) {
			// get this input mzTab file
			MzTabFile inputFile = cleanup.context.getMzTabFile(file);
			// set up intermediate output file
			File tempFile = new File(String.format("%s.temp", file.getName()));
			// set up reader
			MzTabReader reader = new MzTabReader(inputFile, tempFile);
			// clean all ms_run-location file references to use
			// fully qualified ProteoSAFe file descriptor paths
			reader.addProcessor(new MsRunCleanProcessor());
			// ensure that each PSM row has the FDR columns
			// needed by ProteoSAFe to enforce quality control
			Map<String, Integer> counts = new HashMap<String, Integer>(6);
			Map<String, ImmutablePair<Boolean, Boolean>> peptides =
				new HashMap<String, ImmutablePair<Boolean, Boolean>>();
			Map<String, ImmutablePair<Boolean, Boolean>> proteins =
				new HashMap<String, ImmutablePair<Boolean, Boolean>>();
			reader.addProcessor(new FDRCalculationProcessor(
				counts, peptides, proteins, cleanup.passThresholdColumn,
				cleanup.decoyColumn, cleanup.decoyPattern,
				cleanup.qValueColumn));
			// ensure that each PSM row has the special columns
			// needed by ProteoSAFe to ensure validity
			reader.addProcessor(new ValidityProcessor());
			// clean file
			reader.read();
			// calculate global FDR values from returned count maps
			Double psmFDR = MzTabFDRCleaner.calculateFDR(
				counts.get("targetPSM"), counts.get("decoyPSM"));
			// peptide-level FDR
			Double peptideFDR = MzTabFDRCleaner.calculateFDR(
				counts.get("targetPeptide"), counts.get("decoyPeptide"));
			// protein-level FDR
			Double proteinFDR = MzTabFDRCleaner.calculateFDR(
				counts.get("targetProtein"), counts.get("decoyProtein"));
			// set up output file
			File outputFile = new File(cleanup.outputDirectory, file.getName());
			// add global FDR values to output file's metadata section
			MzTabFDRCleaner.doSecondFDRPass(tempFile, outputFile,
				inputFile.getMzTabFilename(), cleanup.filter, cleanup.filterFDR,
				psmFDR, peptideFDR, proteinFDR);
			// remove temporary file
			tempFile.delete();
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
		private String           passThresholdColumn;
		private String           decoyColumn;
		private String           decoyPattern;
		private String           qValueColumn;
		private boolean          filter;
		private Double           filterFDR;
		
		/*====================================================================
		 * Constructors
		 *====================================================================*/
		public ProteoSAFeMzTabCleanupOperation(
			File mzTabDirectory, String mzTabRelativePath,
			File peakListDirectory, String peakListRelativePath,
			File parameters, File outputDirectory, String datasetID,
			String passThresholdColumn, String decoyColumn, String decoyPattern,
			String qValueColumn, boolean filter, Double filterFDR
		) {
			// validate mzTab directory (if null,
			// then no cleanup is necessary)
			if (mzTabDirectory != null) {
				if (mzTabDirectory.isDirectory() == false)
					throw new IllegalArgumentException(String.format(
						"MzTab directory [%s] must be a directory.",
						mzTabDirectory.getAbsolutePath()));
				else if (mzTabDirectory.canRead() == false)
					throw new IllegalArgumentException(String.format(
						"MzTab directory [%s] must be readable.",
						mzTabDirectory.getAbsolutePath()));
			}
			this.mzTabDirectory = mzTabDirectory;
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
			// build mzTab file-mapping context, if applicable
			if (mzTabDirectory != null)
				context = new TaskMzTabContext(
					mzTabDirectory, mzTabRelativePath,
					peakListDirectory, peakListRelativePath,
					parameters, datasetID);
			else context = null;
			// initialize FDR columns (any or all may be null)
			this.passThresholdColumn = passThresholdColumn;
			this.decoyColumn = decoyColumn;
			this.decoyPattern = decoyPattern;
			this.qValueColumn = qValueColumn;
			// initialize filter settings
			this.filter = filter;
			this.filterFDR = filterFDR;
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
		String passThresholdColumn = null;
		String decoyColumn = null;
		String decoyPattern = null;
		String qValueColumn = null;
		Boolean filter = false;
		Double filterFDR = null;
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
				} else if (argument.equals("-passThreshold"))
					passThresholdColumn = value;
				else if (argument.equals("-decoy"))
					decoyColumn = value;
				else if (argument.equals("-decoyPattern"))
					decoyPattern = value;
				else if (argument.equals("-qvalue"))
					qValueColumn = value;
				else if (argument.equals("-filter")) {
					filter = CommonUtils.parseBooleanColumn(value);
					if (filter == null)
						throw new IllegalArgumentException(String.format(
							"Unrecognized value for \"-filter\": [%s]", value));
				} else if (argument.equals("-filterFDR")) try {
					filterFDR = Double.parseDouble(value);
					// enforce the FDR filter range of 0-1
					if (filterFDR < 0.0 || filterFDR > 1.0)
						throw new IllegalArgumentException("The argument to " +
							"\"-filterFDR\" must be a number in the range 0-1");
				} catch (Throwable error) {
					throw new IllegalArgumentException(String.format(
						"Illegal value for \"-filterFDR\": [%s]", value),
						error);
				} else return null;
			}
		}
		try {
			return new ProteoSAFeMzTabCleanupOperation(
				mzTabDirectory, mzTabRelativePath,
				peakListDirectory, peakListRelativePath,
				parameters, outputDirectory, datasetID,
				passThresholdColumn, decoyColumn, decoyPattern, qValueColumn,
				filter, filterFDR);
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
