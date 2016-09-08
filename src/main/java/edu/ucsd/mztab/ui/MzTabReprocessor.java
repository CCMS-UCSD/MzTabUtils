package edu.ucsd.mztab.ui;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.xpath.XPathAPI;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import edu.ucsd.mztab.MzTabReader;
import edu.ucsd.mztab.TaskMzTabContext;
import edu.ucsd.mztab.converters.MzIdToMzTabConverter;
import edu.ucsd.mztab.converters.PRIDEXMLToMzTabConverter;
import edu.ucsd.mztab.exceptions.UnverifiableNativeIDException;
import edu.ucsd.mztab.model.MzTabFDRStatistics;
import edu.ucsd.mztab.model.MzTabFile;
import edu.ucsd.mztab.model.MzTabConstants.FDRType;
import edu.ucsd.mztab.processors.CountProcessor;
import edu.ucsd.mztab.processors.FDRCalculationProcessor;
import edu.ucsd.mztab.processors.MsRunCleanProcessor;
import edu.ucsd.mztab.processors.PSMValidationProcessor;
import edu.ucsd.mztab.processors.SpectraRefValidationProcessor;
import edu.ucsd.mztab.processors.ValidityProcessor;
import edu.ucsd.mztab.ui.MzTabFDRCleaner.FDRFilterType;
import edu.ucsd.util.CommonUtils;
import edu.ucsd.util.FileIOUtils;

public class MzTabReprocessor
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String USAGE =
		"java -cp MzTabUtils.jar edu.ucsd.mztab.ui.MzTabReprocessor" +
		"\n\t-input          <ResultDirectory>" +
		"\n\t[-mztabPath     <MzTabRelativePath>]" +
		"\n\t[-peak          <PeakListFilesDirectory>]" +
		"\n\t[-peakPath      <PeakListRelativePath>]" +
		"\n\t[-scans         <ScansDirectory>]" +
		"\n\t-params         <ProteoSAFeParametersFile>" +
		"\n\t-output         <OutputDirectory>" +
		"\n\t[-dataset       <DatasetID>|<DatasetIDFile>]" +
		"\n\t[-threshold     <InvalidPSMPercentageToFail: 0-100> " +
			"(default 10)]" +
		"\n\t[-psmQValue     <PSMQValueColumn>]" +
		"\n\t[-peptideQValue <PSMQValueColumn>]" +
		"\n\t[-proteinQValue <PSMQValueColumn>]" +
		"\n\t[-psmFDR        <GlobalPSMLevelFDR> " +
			"(leave blank if decoy or PSM Q-value column are specified]" +
		"\n\t[-peptideFDR    <GlobalPeptideLevelFDR> " +
			"(leave blank if decoy or peptide Q-value column are specified]" +
		"\n\t[-proteinFDR    <GlobalProteinLevelFDR> " +
			"(leave blank if decoy or protein Q-value column are specified]" +
		"\n\t[-filter        true/false (default false; " +
			"if specified, PSM rows not meeting the FDR " +
			"threshold will be removed from the file)]" +
		"\n\t[-filterType    psm/peptide/protein (default psm; " +
			"determines which Q-Value column to use when filtering, " +
			"if filter=true and filterFDR is specified)]" +
		"\n\t[-filterFDR     0-1 (if not specified, and filter=true, then " +
			"only PSMs marked as decoy or passThreshold=false will be removed]";
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public static void main(String[] args) {
		MzTabReprocessingOperation reprocessing = extractArguments(args);
		if (reprocessing == null)
			die(USAGE);
		// recursively search input directory for result files
		Collection<File> resultFiles = findFiles(reprocessing.resultDirectory);
		if (resultFiles == null || resultFiles.isEmpty())
			die(String.format(
				"No result files could be found in input directory [%s].",
				reprocessing.resultDirectory.getAbsolutePath()));
		// set up output converted result directory
		File convertedResult =
			new File(reprocessing.outputDirectory, "convertedResult");
		convertedResult.mkdirs();
		// convert or copy all result files
		for (File resultFile : resultFiles) {
			File destinationFile = getDestinationFile(
				resultFile, reprocessing.resultDirectory, convertedResult);
			// if the source file is already an mzTab, just copy it
			String extension = FilenameUtils.getExtension(resultFile.getName());
			if (extension.equalsIgnoreCase("mzTab")) try {
				System.out.println(String.format("Copying mzTab result file " +
					"[%s] to destination file [%s]...",
					resultFile.getAbsolutePath(),
					destinationFile.getAbsolutePath()));
				FileIOUtils.copyFile(resultFile,
					destinationFile.getParentFile(), destinationFile.getName());
			} catch (Throwable error) {
				die(String.format("Could not copy mzTab file [%s] " +
					"to destination file [%s].",
					resultFile.getAbsolutePath(),
					destinationFile.getAbsolutePath()));
			}
			// if the source file is a convertible file, then convert it
			else if (extension.equalsIgnoreCase("mzid")) {
				System.out.println(String.format("Converting mzIdentML " +
					"result file [%s] to mzTab format destination file [%s]...",
					resultFile.getAbsolutePath(),
					destinationFile.getAbsolutePath()));
					MzIdToMzTabConverter.convert(resultFile, destinationFile);
			} else if (extension.equalsIgnoreCase("xml")) {
				System.out.println(String.format("Converting PRIDE XML " +
					"result file [%s] to mzTab format destination file [%s]...",
					resultFile.getAbsolutePath(),
					destinationFile.getAbsolutePath()));
				PRIDEXMLToMzTabConverter.convert(
					resultFile, destinationFile);
			} else die(String.format("Could not copy or convert result "+
				"file [%s]: unrecognized filename extension [%s]",
				resultFile.getAbsolutePath(), extension));
		}
		// build mzTab file-mapping context from converted result directory
		TaskMzTabContext context = new TaskMzTabContext(
			convertedResult, reprocessing.mzTabRelativePath,
			reprocessing.peakListDirectory, reprocessing.peakListRelativePath,
			reprocessing.parameters, reprocessing.datasetID);
		// set up validated output result directory
		File validatedResult =
			new File(reprocessing.outputDirectory, "validatedResult");
		validatedResult.mkdirs();
		// set up validation log directory
		File validationLogDirectory =
			new File(reprocessing.outputDirectory, "validationLog");
		validationLogDirectory.mkdirs();
		// recursively search converted result directory for result files
		resultFiles = findFiles(convertedResult);
		if (resultFiles == null || resultFiles.isEmpty())
			die(String.format("No result files could be " +
				"found in converted result directory [%s].",
				convertedResult.getAbsolutePath()));
		// set up log output stream
		PrintWriter writer = null;
		// validate all converted result files
		try {
			File validationLog =
				new File(validationLogDirectory, "statistics.tsv");
			if (validationLog.exists() == false &&
				validationLog.createNewFile() == false)
				die(String.format("Could not create validation log file [%s]",
					validationLog.getAbsolutePath()));
			writer = new PrintWriter(new BufferedWriter(
				new FileWriter(validationLog, false)));
			// write the header line of the log file
			writer.println(
				"MzTab_file\tUploaded_file\tPSM_rows\tInvalid_PSM_rows");
			// validate all PSM rows, write proper line for each to the log
			for (File resultFile : resultFiles) {
				File destinationFile = getDestinationFile(
					resultFile, convertedResult, validatedResult);
				System.out.println(String.format("Validating converted mzTab " +
					"result file [%s] to destination file [%s]...",
					resultFile.getAbsolutePath(),
					destinationFile.getAbsolutePath()));
				// get this mzTab file
				MzTabFile mzTabFile = context.getMzTabFile(resultFile);
				// set up reader
				MzTabReader reader =
					new MzTabReader(mzTabFile, destinationFile);
				reader.addProcessor(new SpectraRefValidationProcessor(
					//reprocessing.resultDirectory,
					null,	// using null here for performance reasons
					reprocessing.scansDirectory));
				Map<String, Integer> counts = new HashMap<String, Integer>(2);
				reader.addProcessor(new PSMValidationProcessor(counts));
				// validate file
				try { reader.read(); }
				catch (RuntimeException error) {
					// if a RuntimeException is caught, and its parent
					// is an UnverifiableNativeIDException, then that
					// means that the validator tried to interpret
					// ambiguous nativeIDs as scan numbers but failed
					// somewhere along the way; in this case, try again
					// with a hard-coded interpretation scheme of indices
					Throwable parent = error.getCause();
					if (parent != null && parent.getClass().isInstance(
						UnverifiableNativeIDException.class)) {
						// be sure to clear the output file, since the previous
						// attempt may have written some rows to it
						try {
							if (destinationFile.delete() == false)
								throw new RuntimeException(
									"File.delete() returned false.");
						} catch (Throwable innerError) {
							die(String.format("Could not delete output " +
								"file [%s] to attempt a second validation " +
								"pass on input mzTab file [%s].",
								destinationFile.getAbsolutePath(),
								mzTabFile.getMzTabPath()), innerError);
						}
						reader = new MzTabReader(mzTabFile, destinationFile);
						reader.addProcessor(new SpectraRefValidationProcessor(
							//reprocessing.resultDirectory,
							null,	// using null here for performance reasons
							reprocessing.scansDirectory, false));
						counts = new HashMap<String, Integer>(2);
						reader.addProcessor(new PSMValidationProcessor(counts));
						reader.read();
					}
					// otherwise, it's some other kind of
					// RuntimeException, so just throw it
					else throw error;
				}
				// calculate invalid percentage, apply specified threshold
				Integer psmRows = counts.get("psmRows");
				if (psmRows == null)
					psmRows = 0;
				Integer invalidRows = counts.get("invalidRows");
				if (invalidRows == null)
					invalidRows = 0;
				// if the mzTab file has more than the indicated
				// percentage of invalid PSMs, then fail
				Double percentage = null;
				if (psmRows == 0)
					percentage = 0.0;
				else percentage = (double)invalidRows / (double)psmRows * 100.0;
				if (percentage > reprocessing.failureThreshold) {
					//System.err.println(validation.context.toString());
					die(String.format("Result file [%s] contains %s%% " +
						"invalid PSM rows. Please correct the file and " +
						"ensure that its referenced spectra are accessible " +
						"within linked peak list files, and then re-submit.",
						mzTabFile.getUploadedResultPath(), percentage));
				}
				// get relevant file name to print to output file
				String uploadedFilename = mzTabFile.getUploadedResultPath();
				if (uploadedFilename == null)
					uploadedFilename = mzTabFile.getMzTabFilename();
				// write log line
				writer.println(String.format("%s\t%s\t%d\t%d",
					resultFile.getName(), uploadedFilename, psmRows,
					invalidRows));
				writer.flush();
			}
		} catch (Throwable error) {
			die(error.getMessage(), error);
		} finally {
			try { writer.close(); }
			catch (Throwable error) {}
		}
		// rebuild mzTab file-mapping context from validated result directory
		context = new TaskMzTabContext(
			validatedResult, reprocessing.mzTabRelativePath,
			reprocessing.peakListDirectory, reprocessing.peakListRelativePath,
			reprocessing.parameters, reprocessing.datasetID);
		// set up cleaned output result directory
		File cleanedResult =
			new File(reprocessing.outputDirectory, "cleanedResult");
		cleanedResult.mkdirs();
		// recursively search validated result directory for result files
		resultFiles = findFiles(validatedResult);
		if (resultFiles == null || resultFiles.isEmpty())
			die(String.format("No result files could be " +
				"found in validated result directory [%s].",
				validatedResult.getAbsolutePath()));
		// clean all validated result files
		for (File resultFile : resultFiles) {
			File destinationFile = getDestinationFile(
				resultFile, validatedResult, cleanedResult);
			System.out.println(String.format("Cleaning validated mzTab " +
				"result file [%s] to destination file [%s]...",
				resultFile.getAbsolutePath(),
				destinationFile.getAbsolutePath()));
			// get this input mzTab file
			MzTabFile inputFile = context.getMzTabFile(resultFile);
			// set up first intermediate output file
			File tempFile1 = new File(destinationFile.getParentFile(),
				String.format("%s.1.temp", resultFile.getName()));
			// set up reader
			MzTabReader reader = new MzTabReader(inputFile, tempFile1);
			// clean all ms_run-location file references to use
			// fully qualified ProteoSAFe file descriptor paths
			reader.addProcessor(new MsRunCleanProcessor());
			// ensure that each PSM row has the FDR columns
			// needed by ProteoSAFe to enforce quality control
			MzTabFDRStatistics statistics = new MzTabFDRStatistics();
			reader.addProcessor(new FDRCalculationProcessor(
				statistics, reprocessing.passThresholdColumn,
				reprocessing.decoyColumn, reprocessing.decoyPattern,
				reprocessing.psmQValueColumn));
			// ensure that each PSM row has the special columns
			// needed by ProteoSAFe to ensure validity
			reader.addProcessor(new ValidityProcessor());
			// clean file
			reader.read();
			// calculate global FDR values from returned count maps
			Double psmFDR = MzTabFDRCleaner.calculateFDR(
				statistics.getElementCount("targetPSM"),
				statistics.getElementCount("decoyPSM"));
			// if no FDR could be calculated, use highest found Q-Value, if any
			if (psmFDR == null)
				psmFDR = statistics.getMaxQValue(FDRType.PSM);
			// if FDR could still not be determined, use user-specified FDR
			if (psmFDR == null)
				psmFDR = reprocessing.psmFDR;
			// peptide-level FDR
			Double peptideFDR = MzTabFDRCleaner.calculateFDR(
				statistics.getElementCount("targetPeptide"),
				statistics.getElementCount("decoyPeptide"));
			if (peptideFDR == null)
				peptideFDR = statistics.getMaxQValue(FDRType.PEPTIDE);
			if (peptideFDR == null)
				peptideFDR = reprocessing.peptideFDR;
			// protein-level FDR
			Double proteinFDR = MzTabFDRCleaner.calculateFDR(
				statistics.getElementCount("targetProtein"),
				statistics.getElementCount("decoyProtein"));
			if (proteinFDR == null)
				proteinFDR = statistics.getMaxQValue(FDRType.PROTEIN);
			if (proteinFDR == null)
				proteinFDR = reprocessing.proteinFDR;
			// set up second intermediate output file
			File tempFile2 = new File(destinationFile.getParentFile(),
				String.format("%s.2.temp", resultFile.getName()));
			// add global FDR values to output file's metadata section,
			// filter out all PSM rows that do not meet the FDR cutoff,
			// and propagate calculated global FDR to any empty Q-Values
			MzTabFDRCleaner.doSecondFDRPass(tempFile1, tempFile2,
				inputFile.getMzTabFilename(), reprocessing.filter,
				reprocessing.filterType, reprocessing.filterFDR,
				reprocessing.peptideQValueColumn,
				reprocessing.proteinQValueColumn,
				psmFDR, peptideFDR, proteinFDR, statistics);
			// filter out all protein and peptide rows no
			// longer supported by remaining PSM rows
			MzTabFDRCleaner.doThirdFDRPass(tempFile2, destinationFile,
				inputFile.getMzTabFilename(), reprocessing.filter, statistics);
			// remove temporary files
			tempFile1.delete();
			tempFile2.delete();
		}
		// rebuild mzTab file-mapping context from cleaned result directory
		context = new TaskMzTabContext(
			cleanedResult, reprocessing.mzTabRelativePath,
			reprocessing.peakListDirectory, reprocessing.peakListRelativePath,
			reprocessing.parameters, reprocessing.datasetID);
		// set up statistics directory
		File statisticsDirectory =
			new File(reprocessing.outputDirectory, "ccms_statistics");
		statisticsDirectory.mkdirs();
		// recursively search cleaned result directory for result files
		resultFiles = findFiles(cleanedResult);
		if (resultFiles == null || resultFiles.isEmpty())
			die(String.format("No result files could be " +
				"found in final cleaned result directory [%s].",
				cleanedResult.getAbsolutePath()));
		// sort files alphabetically
		File[] files = resultFiles.toArray(new File[resultFiles.size()]);
		Arrays.sort(files);
		// set up statistics file writer
		writer = null;
		try {
			File statistics = new File(statisticsDirectory, "statistics.tsv");
			if (statistics.exists() == false &&
				statistics.createNewFile() == false)
				die(String.format("Could not create statistics file [%s]",
					statistics.getAbsolutePath()));
			writer = new PrintWriter(new BufferedWriter(
				new FileWriter(statistics, false)));
			writer.println("MzTab_file\tUploaded_file\tDataset_mzTab\t" +
				"File_descriptor\tPSM_rows\tInvalid_PSM_rows\tFound_PSMs\t" +
				"PSM_FDR\tPeptide_rows\tFound_Peptides\tPeptide_FDR\t" +
				"Protein_rows\tFound_Proteins\tProtein_FDR\tFound_Mods");
			// read through all mzTab files, write counts to output file
			for (File file : files) {
				System.out.println(String.format("Writing counts in " +
					"cleaned mzTab result file [%s] to statistics file [%s]...",
					file.getAbsolutePath(), statistics.getAbsolutePath()));
				Map<String, Integer> counts = new HashMap<String, Integer>(7);
				MzTabFile mzTabFile = context.getMzTabFile(file);
				MzTabReader reader = new MzTabReader(mzTabFile);
				reader.addProcessor(new CountProcessor(counts));
				reader.read();
				// get relevant file names to print to output file
				String uploadedFilename = mzTabFile.getUploadedResultPath();
				if (uploadedFilename == null)
					uploadedFilename = mzTabFile.getMzTabFilename();
				String mzTabPath = mzTabFile.getMappedMzTabPath();
				if (mzTabPath == null)
					mzTabPath = file.getName();
				StringBuilder datasetFilePath = new StringBuilder();
				if (reprocessing.mzTabRelativePath != null) {
					datasetFilePath.append(reprocessing.mzTabRelativePath);
					// add trailing slash, if not already present
					if (datasetFilePath.charAt(datasetFilePath.length() - 1)
						!= '/')
						datasetFilePath.append("/");
				} else datasetFilePath.append("ccms_result/");
				datasetFilePath.append(mzTabPath);
				// extract global FDR values to print to output file
				String[] fdr = MzTabCounter.extractGlobalFDRValues(file);
				writer.println(String.format(
					"%s\t%s\t%s\t%s\t%d\t%d\t%d\t%s\t" +
					"%d\t%d\t%s\t%d\t%d\t%s\t%d",
					file.getName(), uploadedFilename,
					datasetFilePath.toString(), mzTabFile.getDescriptor(),
					counts.get("PSM"), counts.get("invalid_PSM"),
					counts.get("PSM_ID"), fdr[0],
					counts.get("PEP"), counts.get("sequence"), fdr[1],
					counts.get("PRT"), counts.get("accession"), fdr[2],
					counts.get("modification")));
			}
		} catch (Throwable error) {
			die(error.getMessage(), error);
		} finally {
			try { writer.close(); }
			catch (Throwable error) {}
		}
		// set up final processed result directory
		File outputDirectory =
			new File(reprocessing.outputDirectory, "ccms_result");
		outputDirectory.mkdirs();
		// copy all files to their final destination,
		// preserving directory structure
		for (File file : files) {
			// get final destination file
			MzTabFile mzTabFile = context.getMzTabFile(file);
			String mzTabPath = mzTabFile.getMappedMzTabPath();
			if (mzTabPath == null)
				mzTabPath = file.getName();
			File destinationFile = new File(outputDirectory, mzTabPath);
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
		// clear out intermediate file directories
		try { FileUtils.deleteDirectory(convertedResult); }
		catch (Throwable error) {}
		try { FileUtils.deleteDirectory(validatedResult); }
		catch (Throwable error) {}
		try { FileUtils.deleteDirectory(cleanedResult); }
		catch (Throwable error) {}
	}
	
	/*========================================================================
	 * Convenience classes
	 *========================================================================*/
	/**
	 * Struct to maintain context data for each
	 * result file reprocessing operation.
	 */
	private static class MzTabReprocessingOperation {
		/*====================================================================
		 * Properties
		 *====================================================================*/
		private File          resultDirectory;
		private File          outputDirectory;
		private File          peakListDirectory;
		private File          scansDirectory;
		private File          parameters;
		private String        mzTabRelativePath;
		private String        peakListRelativePath;
		private String        datasetID;
		private String        passThresholdColumn;
		private String        decoyColumn;
		private String        decoyPattern;
		private String        psmQValueColumn;
		private String        peptideQValueColumn;
		private String        proteinQValueColumn;
		private boolean       filter;
		private FDRFilterType filterType;
		private Double        failureThreshold;
		private Double        filterFDR;
		private Double        psmFDR;
		private Double        peptideFDR;
		private Double        proteinFDR;
		
		/*====================================================================
		 * Constructors
		 *====================================================================*/
		public MzTabReprocessingOperation(
			File resultDirectory, String mzTabRelativePath,
			File peakListDirectory, String peakListRelativePath,
			File scansDirectory, File parameters,
			File outputDirectory, String datasetID, String failureThreshold,
			String passThresholdColumn, String decoyColumn, String decoyPattern,
			String psmQValueColumn, String peptideQValueColumn,
			String proteinQValueColumn,
			boolean filter, String filterType, Double filterFDR,
			Double psmFDR, Double peptideFDR, Double proteinFDR
		) {
			// validate result directory
			if (resultDirectory == null)
				throw new NullPointerException(
					"Result directory cannot be null.");
			else if (resultDirectory.isDirectory() == false)
				throw new IllegalArgumentException(
					String.format("Result directory [%s] must be a directory.",
						resultDirectory.getAbsolutePath()));
			else if (resultDirectory.canRead() == false)
				throw new IllegalArgumentException(
					String.format("Result directory [%s] must be readable.",
						resultDirectory.getAbsolutePath()));
			else this.resultDirectory = resultDirectory;
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
			// validate scans directory (can be null)
			if (scansDirectory != null) {
				if (scansDirectory.isDirectory() == false)
					throw new IllegalArgumentException(String.format(
						"Spectrum ID files directory [%s] must be a directory.",
						scansDirectory.getAbsolutePath()));
				else if (scansDirectory.canRead() == false)
					throw new IllegalArgumentException(String.format(
						"Spectrum ID files directory [%s] must be readable.",
						scansDirectory.getAbsolutePath()));
			}
			this.scansDirectory = scansDirectory;
			// validate params.xml file
			if (parameters == null)
				throw new NullPointerException(
					"Argument params.xml file cannot be null.");
			else if (parameters.isFile() == false ||
				parameters.canRead() == false)
				throw new IllegalArgumentException(
					"Argument params.xml file must be a readable file.");
			else this.parameters = parameters;
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
			// initialize file context properties (any or all may be null)
			this.peakListDirectory = peakListDirectory;
			this.mzTabRelativePath = mzTabRelativePath;
			this.peakListRelativePath = peakListRelativePath;
			this.datasetID = datasetID;
			// determine failure threshold for mzTab validation
			if (failureThreshold != null) try {
				this.failureThreshold = Double.parseDouble(failureThreshold);
			} catch (NumberFormatException error) {
				throw new IllegalArgumentException(String.format(
					"Failure threshold [%s] must be a real number " +
					"between 0 and 100.", failureThreshold));
			} else this.failureThreshold =
				MzTabValidator.DEFAULT_FAILURE_THRESHOLD;
			// initialize FDR columns (any or all may be null)
			this.passThresholdColumn = passThresholdColumn;
			this.decoyColumn = decoyColumn;
			this.decoyPattern = decoyPattern;
			this.psmQValueColumn = psmQValueColumn;
			this.peptideQValueColumn = peptideQValueColumn;
			this.proteinQValueColumn = proteinQValueColumn;
			// TODO: check params.xml to fill in any missing values above
			// initialize filter settings
			this.filter = filter;
			this.filterFDR = filterFDR;
			// validate filter type
			if (filterType == null || filterType.trim().equals(""))
				this.filterType = null;
			else try {
				this.filterType =
					FDRFilterType.valueOf(filterType.trim().toUpperCase());
			} catch (Throwable error) {
				throw new IllegalArgumentException(
					String.format("Unrecognized filter type [%s]: must be  " +
						"\"psm\", \"peptide\" or \"protein\".", filterType));
			}
			// initialize user-specified global FDR values
			this.psmFDR = psmFDR;
			this.peptideFDR = peptideFDR;
			this.proteinFDR = proteinFDR;
			// if any of the global FDR values are
			// null, look them up in params.xml
			if (psmFDR == null || peptideFDR == null || proteinFDR == null) {
				Document params = null;
				try { params = FileIOUtils.parseXML(parameters); }
				catch (Throwable error) {}
				if (params != null) {
					// PSM-level FDR
					if (psmFDR == null) try {
						Node parameter = XPathAPI.selectSingleNode(
							params, "//parameter[@name='fdr.psm']");
						if (parameter != null)
							this.psmFDR = Double.parseDouble(
								parameter.getFirstChild().getNodeValue());
					} catch (Throwable error) {}
					// peptide-level FDR
					if (peptideFDR == null) try {
						Node parameter = XPathAPI.selectSingleNode(
							params, "//parameter[@name='fdr.peptide']");
						if (parameter != null)
							this.peptideFDR = Double.parseDouble(
								parameter.getFirstChild().getNodeValue());
					} catch (Throwable error) {}
					// protein-level FDR
					if (proteinFDR == null) try {
						Node parameter = XPathAPI.selectSingleNode(
							params, "//parameter[@name='fdr.protein']");
						if (parameter != null)
							this.proteinFDR = Double.parseDouble(
								parameter.getFirstChild().getNodeValue());
					} catch (Throwable error) {}
				}
			}
		}
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private static MzTabReprocessingOperation extractArguments(String[] args) {
		if (args == null || args.length < 1)
			return null;
		File resultDirectory = null;
		String mzTabRelativePath = null;
		File peakListDirectory = null;
		String peakListRelativePath = null;
		File scansDirectory = null;
		File parameters = null;
		File outputDirectory = null;
		String datasetID = null;
		String failureThreshold = null;
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
				if (argument.equals("-input"))
					resultDirectory = new File(value);
				else if (argument.equals("-mztabPath"))
					mzTabRelativePath = value;
				else if (argument.equals("-peak"))
					peakListDirectory = new File(value);
				else if (argument.equals("-peakPath"))
					peakListRelativePath = value;
				else if (argument.equals("-scans"))
					scansDirectory = new File(value);
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
				} else if (argument.equals("-threshold"))
					failureThreshold = value;
				else if (argument.equals("-passThreshold"))
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
				} else {
					System.err.println(String.format(
						"Unrecognized argument [%s].", argument));
					return null;
				}
			}
		}
		try {
			return new MzTabReprocessingOperation(
				resultDirectory, mzTabRelativePath,
				peakListDirectory, peakListRelativePath, scansDirectory,
				parameters, outputDirectory, datasetID, failureThreshold,
				passThresholdColumn, decoyColumn, decoyPattern,
				psmQValueColumn, peptideQValueColumn, proteinQValueColumn,
				filter, filterType, filterFDR, psmFDR, peptideFDR, proteinFDR);
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
	
	private static File getDestinationFile(
		File sourceFile, File sourceRoot, File destinationDirectory
	) {
		if (sourceFile == null || sourceRoot == null ||
			destinationDirectory == null)
			return null;
		// extract relative path of this source
		// file under the source directory
		String sourceFileRelativePath = sourceFile.getAbsolutePath().substring(
			sourceRoot.getAbsolutePath().length());
		// chomp leading slash, if present
		if (sourceFileRelativePath.charAt(0) == File.separatorChar)
			sourceFileRelativePath = sourceFileRelativePath.substring(1);
		// extract base filename of this source file
		String sourceFilename = sourceFile.getName();
		sourceFileRelativePath = sourceFileRelativePath.substring(
			0, sourceFileRelativePath.lastIndexOf(sourceFilename));
		// chomp trailing slash, if present
		if (sourceFileRelativePath.isEmpty() == false &&
			sourceFileRelativePath.charAt(
			sourceFileRelativePath.length() - 1) == File.separatorChar)
			sourceFileRelativePath = sourceFileRelativePath.substring(
				0, sourceFileRelativePath.length() - 1);
		// prepare destination directory
		File destinationParent =
			new File(destinationDirectory, sourceFileRelativePath);
		destinationParent.mkdirs();
		// return destination file
		return new File(destinationParent, String.format(
			"%s.mzTab", FilenameUtils.getBaseName(sourceFilename)));
	}
	
	private static void die(String message) {
		die(message, null);
	}
	
	private static void die(String message, Throwable error) {
		if (message == null)
			message =
				"There was an error reprocessing this set of result files";
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
