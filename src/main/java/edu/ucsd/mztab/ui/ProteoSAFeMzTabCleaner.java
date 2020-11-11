package edu.ucsd.mztab.ui;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;

import org.apache.commons.io.FilenameUtils;

import edu.ucsd.mztab.TaskMzTabContext;
import edu.ucsd.mztab.model.MzTabConstants.FDRType;
import edu.ucsd.mztab.model.MzTabFile;
import edu.ucsd.mztab.model.MzTabProcessor;
import edu.ucsd.mztab.processors.MsRunCleanProcessor;
import edu.ucsd.mztab.processors.ValidityProcessor;
import edu.ucsd.mztab.util.CommonUtils;
import edu.ucsd.mztab.util.FileIOUtils;

public class ProteoSAFeMzTabCleaner
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String USAGE =
		"java -cp MzTabUtils.jar edu.ucsd.mztab.ui.ProteoSAFeMzTabCleaner" +
		"\n\t[-mztab          <MzTabDirectory>" +
			"(if not provided, then cleanup is skipped]" +
		"\n\t[-mztabPath      <MzTabRelativePath> " +
			"(if not under MzTabDirectory)]" +
        "\n\t[-mztabErrors    <MzTabErrorsDirectory>]" +
		"\n\t[-peak           <PeakListFilesDirectory>]" +
		"\n\t[-peakPath       <PeakListRelativePath> " +
			"(if not under PeakListFilesDirectory)]" +
		"\n\t[-peakCollection <PeakListCollectionName> " +
			"(default \"peak_list_files\")]" +
		"\n\t-params          <ProteoSAFeParametersFile>" +
		"\n\t-output          <CleanedMzTabDirectory>" +
		"\n\t[-dataset        <DatasetID>|<DatasetIDFile>]" +
		"\n\t[-passThreshold  <PassThresholdColumn>]" +
		"\n\t[-decoy          <IsDecoyColumn>]" +
		"\n\t[-decoyPattern   <SubstringIndicatingDecoy>]" +
		"\n\t[-psmQValue      <PSMQValueColumn>]" +
		"\n\t[-peptideQValue  <PSMQValueColumn>]" +
		"\n\t[-proteinQValue  <PSMQValueColumn>]" +
		"\n\t[-psmFDR         <GlobalPSMLevelFDR> " +
			"(leave blank if decoy or PSM Q-value column are specified]" +
		"\n\t[-peptideFDR     <GlobalPeptideLevelFDR> " +
			"(leave blank if decoy or peptide Q-value column are specified]" +
		"\n\t[-proteinFDR     <GlobalProteinLevelFDR> " +
			"(leave blank if decoy or protein Q-value column are specified]" +
		"\n\t[-filter         true/false (default false; " +
			"if specified, PSM rows not meeting the FDR " +
			"threshold will be removed from the file)]" +
		"\n\t[-filterType     psm/peptide/protein (default psm; " +
			"determines which Q-Value column to use when filtering, " +
			"if filter=true and filterFDR is specified)]" +
		"\n\t[-filterFDR      0-1 (if not specified, and filter=true, then " +
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
        // check mzTab errors directory, note any files that failed
        Collection<String> failedMangledNames = null;
        if (cleanup.mzTabErrorsDirectory != null) {
            File[] errorFiles = cleanup.mzTabErrorsDirectory.listFiles();
            if (errorFiles != null && errorFiles.length > 0) {
                failedMangledNames = new LinkedHashSet<String>(errorFiles.length);
                for (File file : errorFiles) {
                    failedMangledNames.add(FilenameUtils.getBaseName(file.getName()));
                }
            }
        }
		// otherwise, read through all mzTab files, and run
		// all relevant MassIVE cleanup operations on each
		Collection<File> files = FileIOUtils.findFiles(cleanup.mzTabDirectory);
		if (files != null && files.isEmpty() == false) {
			// sort files alphabetically
			Collections.sort(new ArrayList<File>(files));
			for (File file : files) {
                // if this file failed previously for any reason, don't clean it
                String mangledBase = FilenameUtils.getBaseName(file.getName());
                if (failedMangledNames != null && failedMangledNames.contains(mangledBase))
                    continue;
				// only process this file if it's an mzTab file
				String extension = FilenameUtils.getExtension(file.getName());
				if (extension == null ||
					extension.trim().equalsIgnoreCase("mztab") == false)
					continue;
				// get this input mzTab file
				MzTabFile inputFile = cleanup.context.getMzTabFile(file);
				// get final output file
				File outputFile = getOutputFile(
					file, cleanup.mzTabDirectory, cleanup.outputDirectory);
				// add all processors needed for general mzTab file cleanup
				Collection<MzTabProcessor> processors =
					new LinkedHashSet<MzTabProcessor>(2);
				// clean all ms_run-location file references to use
				// fully qualified ProteoSAFe file descriptor paths
				processors.add(new MsRunCleanProcessor());
				// ensure that each PSM row has the special columns
				// needed by ProteoSAFe to ensure validity
				processors.add(new ValidityProcessor());
				// FDR-process this mzTab file
				MzTabFDRCleaner.processMzTabFileFDR(inputFile, outputFile,
					processors, cleanup.passThresholdColumn,
					cleanup.decoyColumn, cleanup.decoyPattern,
					cleanup.psmQValueColumn, cleanup.peptideQValueColumn,
					cleanup.proteinQValueColumn, cleanup.filter,
					cleanup.filterType, cleanup.filterFDR,
					cleanup.psmFDR, cleanup.peptideFDR, cleanup.proteinFDR);
			}
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
        private File             mzTabErrorsDirectory;
		private File             outputDirectory;
		private TaskMzTabContext context;
		private String           passThresholdColumn;
		private String           decoyColumn;
		private String           decoyPattern;
		private String           psmQValueColumn;
		private String           peptideQValueColumn;
		private String           proteinQValueColumn;
		private boolean          filter;
		private FDRType          filterType;
		private Double           filterFDR;
		private Double           psmFDR;
		private Double           peptideFDR;
		private Double           proteinFDR;
		
		/*====================================================================
		 * Constructors
		 *====================================================================*/
		public ProteoSAFeMzTabCleanupOperation(
			File mzTabDirectory, String mzTabRelativePath, File mzTabErrorsDirectory,
			File peakListDirectory, String peakListRelativePath,
			String peakListCollection, File parameters,
			File outputDirectory, String datasetID,
			String passThresholdColumn, String decoyColumn, String decoyPattern,
			String psmQValueColumn, String peptideQValueColumn,
			String proteinQValueColumn,
			boolean filter, String filterType, Double filterFDR,
			Double psmFDR, Double peptideFDR, Double proteinFDR
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
            // validate mzTab errors directory (may be null)
            if (mzTabErrorsDirectory != null) {
                if (mzTabErrorsDirectory.isDirectory() == false)
                    throw new IllegalArgumentException(String.format(
                        "MzTab errors directory [%s] must be a directory.",
                        mzTabErrorsDirectory.getAbsolutePath()));
                else if (mzTabErrorsDirectory.canRead() == false)
                    throw new IllegalArgumentException(String.format(
                        "MzTab errors directory [%s] must be readable.",
                        mzTabErrorsDirectory.getAbsolutePath()));
            }
            this.mzTabErrorsDirectory = mzTabErrorsDirectory;
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
					peakListCollection, parameters, datasetID);
			else context = null;
			// initialize FDR columns (any or all may be null)
			this.passThresholdColumn = passThresholdColumn;
			this.decoyColumn = decoyColumn;
			this.decoyPattern = decoyPattern;
			this.psmQValueColumn = psmQValueColumn;
			this.peptideQValueColumn = peptideQValueColumn;
			this.proteinQValueColumn = proteinQValueColumn;
			// initialize filter settings
			this.filter = filter;
			this.filterFDR = filterFDR;
			// validate filter type
			if (filterType == null || filterType.trim().equals(""))
				this.filterType = null;
			else try {
				this.filterType =
					FDRType.valueOf(filterType.trim().toUpperCase());
			} catch (Throwable error) {
				throw new IllegalArgumentException(
					String.format("Unrecognized filter type [%s]: must be  " +
						"\"psm\", \"peptide\" or \"protein\".", filterType));
			}
			// initialize user-specified global FDR values
			this.psmFDR = psmFDR;
			this.peptideFDR = peptideFDR;
			this.proteinFDR = proteinFDR;
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
        File mzTabErrorsDirectory = null;
		File peakListDirectory = null;
		String peakListRelativePath = null;
		String peakListCollection = null;
		File parameters = null;
		File outputDirectory = null;
		String datasetID = null;
		String passThresholdColumn = null;
		String decoyColumn = null;
		String decoyPattern = null;
		String psmQValueColumn = null;
		String peptideQValueColumn = null;
		String proteinQValueColumn = null;
		String filterType = null;
		Boolean filter = false;
		Double filterFDR = null;
		Double psmFDR = null;
		Double peptideFDR = null;
		Double proteinFDR = null;
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
                else if (argument.equals("-mztabErrors"))
                    mzTabErrorsDirectory = new File(value);
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
				} else if (argument.equals("-passThreshold"))
					passThresholdColumn = value;
				else if (argument.equals("-decoy"))
					decoyColumn = value;
				else if (argument.equals("-decoyPattern"))
					decoyPattern = value;
				else if (argument.equals("-psmQValue"))
					psmQValueColumn = value;
				else if (argument.equals("-peptideQValue"))
					peptideQValueColumn = value;
				else if (argument.equals("-proteinQValue"))
					proteinQValueColumn = value;
				else if (argument.equals("-filterType"))
					filterType = value;
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
				} else if (argument.equals("-psmFDR")) try {
					psmFDR = Double.parseDouble(value);
					// enforce the FDR range of 0-1
					if (psmFDR < 0.0 || psmFDR > 1.0)
						throw new IllegalArgumentException("The argument to " +
							"\"-psmFDR\" must be a number in the range 0-1");
				} catch (Throwable error) {
					throw new IllegalArgumentException(String.format(
						"Illegal value for \"-psmFDR\": [%s]", value),
						error);
				} else if (argument.equals("-peptideFDR")) try {
					peptideFDR = Double.parseDouble(value);
					// enforce the FDR range of 0-1
					if (peptideFDR < 0.0 || peptideFDR > 1.0)
						throw new IllegalArgumentException(
							"The argument to \"-peptideFDR\" must " +
							"be a number in the range 0-1");
				} catch (Throwable error) {
					throw new IllegalArgumentException(String.format(
						"Illegal value for \"-peptideFDR\": [%s]", value),
						error);
				} else if (argument.equals("-proteinFDR")) try {
					proteinFDR = Double.parseDouble(value);
					// enforce the FDR range of 0-1
					if (proteinFDR < 0.0 || proteinFDR > 1.0)
						throw new IllegalArgumentException(
							"The argument to \"-proteinFDR\" must " +
							"be a number in the range 0-1");
				} catch (Throwable error) {
					throw new IllegalArgumentException(String.format(
						"Illegal value for \"-proteinFDR\": [%s]", value),
						error);
				} else return null;
			}
		}
		try {
			return new ProteoSAFeMzTabCleanupOperation(
				mzTabDirectory, mzTabRelativePath, mzTabErrorsDirectory,
				peakListDirectory, peakListRelativePath, peakListCollection,
				parameters, outputDirectory, datasetID,
				passThresholdColumn, decoyColumn, decoyPattern,
				psmQValueColumn, peptideQValueColumn, proteinQValueColumn,
				filter, filterType, filterFDR, psmFDR, peptideFDR, proteinFDR);
		} catch (Throwable error) {
			error.printStackTrace();
			return null;
		}
	}
	
	private static File getOutputFile(
		File inputFile, File inputDirectory, File outputDirectory
	) {
		if (inputFile == null || outputDirectory == null ||
			outputDirectory.isDirectory() == false)
			return null;
		else if (inputDirectory == null)
			return new File(outputDirectory, inputFile.getName());
		// get relative path of the input file
		// with respect to the input directory
		String inputFilePath =
			FilenameUtils.separatorsToUnix(inputFile.getAbsolutePath());
		String inputDirectoryPath =
			FilenameUtils.separatorsToUnix(inputDirectory.getAbsolutePath());
		if (inputFilePath.startsWith(inputDirectoryPath) == false)
			throw new IllegalStateException();
		String relativePath =
			inputFilePath.substring(inputDirectoryPath.length());
		// trim off leading slash, if present
		if (relativePath.startsWith("/"))
			relativePath = relativePath.substring(1);
		// append relative path to output directory
		return new File(outputDirectory, relativePath);
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
