package edu.ucsd.mztab.processors;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;

import edu.ucsd.mztab.model.Modification;
import edu.ucsd.mztab.model.MzTabConstants;
import edu.ucsd.mztab.model.MzTabFile;
import edu.ucsd.mztab.model.MzTabProcessor;
import edu.ucsd.mztab.model.MzTabSectionHeader;
import edu.ucsd.mztab.model.MzTabConstants.MzTabSection;
import edu.ucsd.mztab.model.PSM;
import edu.ucsd.mztab.model.MzTabMsRun;
import edu.ucsd.mztab.ui.MzTabPROXIImporter;
import edu.ucsd.mztab.util.CommonUtils;
import edu.ucsd.mztab.util.ProteomicsUtils;

public class PROXIProcessor implements MzTabProcessor
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String[] RELEVANT_PRT_COLUMNS =
		new String[]{ "accession", "modifications" };
	private static final String[] RELEVANT_PEP_COLUMNS =
		new String[]{ "sequence", "accession", "modifications" };
	private static final String[] RELEVANT_PSM_COLUMNS = new String[]{
		"PSM_ID", "sequence", "accession", "modifications", "spectra_ref",
		"charge", "exp_mass_to_charge"
	};
	private static final int DEFAULT_INSERT_BATCH_SIZE = 100000;
    private static final String DEBUG_LOG_HEADER =
        "category\ttotal_batch_count\ttotal_element_count\ttotal_time_ns" +
        "\tslowest_batch_element_count\tslowest_batch_time_ns" +
        "\tfastest_batch_element_count\tfastest_batch_time_ns" +
        "\taverage_batch_element_count\taverage_batch_time_ns";
    private static final String TEMP_DIRECTORY_NAME = "temp";
    private static final String PSMS_LOAD_FILE_NAME = "psms.tsv";
    private static final String PSMS_LOAD_FILE_HEADER =
        "id_in_file\tnativeid\tvariant_sequence\tcharge\texp_mass_to_charge" +
        "\tresultfile_id\tspectrumfile_id\tpeptide_id\tvariant_id\tdataset_id";
	
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private Connection                        connection;
	private Map<String, Map<String, Integer>> uniqueElements;
	private Map<String, Map<String, Integer>> globalElements;
	private Map<String, Integer>              rowCounts;
	private Set<String>                       psmIDs;
	private PROXIMzTabRecord                  mzTabRecord;
	private PROXIBatchManager                 batchManager;
	private MzTabSectionHeader                prtHeader;
	private MzTabSectionHeader                pepHeader;
	private MzTabSectionHeader                psmHeader;
	private boolean                           importByQValue;
	private Integer                           lastPSMIndex;
	private Integer                           validColumn;
	private Integer                           qValueColumn;
	private Integer                           batchSize;
	private File                              psmsLoadFile;
	private PrintWriter                       psmsLoadFileWriter;
	private File                              debugLogFile;
	private Long                              start;
	
	/*========================================================================
	 * Constructor
	 *========================================================================*/
	public PROXIProcessor(
		String taskID, String datasetID, boolean importByQValue,
		Connection connection
	) {
		this(taskID, datasetID, importByQValue, null, null, connection);
	}
	
	public PROXIProcessor(
		String taskID, String datasetID, boolean importByQValue, Integer batchSize,
		Map<String, Map<String, Integer>> globalElements, Connection connection
	) {
		// validate database connection
		if (connection == null)
			throw new NullPointerException(
				"Argument database connection is null.");
		else this.connection = connection;
		// initialize PROXI mzTab file record
		mzTabRecord = new PROXIMzTabRecord();
		// validate task ID
		if (taskID == null)
			throw new NullPointerException(
				"Argument ProteoSAFe task ID is null.");
		else mzTabRecord.taskID = taskID;
		// dataset ID can be null
		mzTabRecord.datasetID = datasetID;
		// initialize counter maps
		uniqueElements = new HashMap<String, Map<String, Integer>>();
		if (globalElements != null)
			this.globalElements = globalElements;
		rowCounts = new HashMap<String, Integer>(3);
		// initialize mzTab file parameters
		prtHeader = null;
		pepHeader = null;
		psmHeader = null;
		qValueColumn = null;
		this.importByQValue = importByQValue;
		// initialize batch state
		this.batchSize = batchSize;
		// initialize PSMs load file
		File tempDirectory = new File(TEMP_DIRECTORY_NAME);
		tempDirectory.mkdir();
		if (tempDirectory.isDirectory() == false || tempDirectory.canWrite() == false)
			throw new RuntimeException(String.format(
				"Could not create temp directory [%s].", tempDirectory.getAbsolutePath()));
		psmsLoadFile = new File(tempDirectory, PSMS_LOAD_FILE_NAME);
		// initialize PSM ID set
		psmIDs = new TreeSet<String>();
		// intialize start time
		start = null;
	}
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public void setUp(MzTabFile mzTabFile) {
		// validate argument mzTab file
		if (mzTabFile == null)
			throw new NullPointerException("Argument mzTab file is null.");
		else this.mzTabRecord.mzTabFile = mzTabFile;
		// intialize start time for logging purposes
		start = System.nanoTime();
		// insert mzTab file into database,
		// populate mzTabFile object with column values
		insertMzTabFile();
		// initialize batch manager
		batchManager = new PROXIBatchManager(mzTabRecord, batchSize, connection);
		// record all of this mzTab file's referenced spectrum files
		for (Integer msRun : mzTabRecord.mzTabFile.getMsRuns().keySet())
			processSpectrumFile(mzTabRecord.mzTabFile.getMsRun(msRun).getDescriptor());
		// set up PSMs load file writer
		try {
			psmsLoadFileWriter = new PrintWriter(psmsLoadFile);
			psmsLoadFileWriter.println(PSMS_LOAD_FILE_HEADER);
		} catch (Throwable error) {
			throw new RuntimeException(String.format(
				"Could not open PSMs load file [%s] for writing.",
				psmsLoadFile.getAbsolutePath()), error);
		}
	}
	
	public String processMzTabLine(String line, int lineNumber) {
		if (line == null)
			throw new NullPointerException(
				"Processed mzTab line cannot be null.");
		else incrementRowCount("lines_in_file");
		String mzTabFilename = mzTabRecord.mzTabFile.getMzTabFilename();
		// read line and, if it's a content row, parse and insert its content
		// protein section
		if (line.startsWith("PRH")) {
			if (prtHeader != null)
				throw new IllegalArgumentException(String.format(
					"Line %d of mzTab file [%s] is invalid:" +
					"\n----------\n%s\n----------\n" +
					"A \"PRH\" row was already seen previously in this file.",
					lineNumber, mzTabFilename, line));
			prtHeader = new MzTabSectionHeader(line);
			prtHeader.validateHeaderExpectations(
				MzTabSection.PRT, Arrays.asList(RELEVANT_PRT_COLUMNS));
		} else if (line.startsWith("PRT")) {
//			incrementRowCount("PRT");
//			if (prtHeader == null)
//				throw new IllegalArgumentException(String.format(
//					"Line %d of mzTab file [%s] is invalid:" +
//					"\n----------\n%s\n----------\n" +
//					"A \"PRT\" row was found before any \"PRH\" row.",
//					lineNumber, mzTabFilename, line));
//			else prtHeader.validateMzTabRow(line);
//			// extract insertable elements from this PRT row
//			String[] columns = line.split("\\t");
//			// record this protein
//			Collection<Modification> modifications =
//				ProteomicsUtils.getModifications(
//					columns[prtHeader.getColumnIndex("modifications")]);
//			try {
//				cascadeProtein(
//					columns[prtHeader.getColumnIndex("accession")],
//					modifications);
//				connection.commit();
//			} catch (Throwable error) {
//				try { connection.rollback(); } catch (Throwable innerError) {}
//				// log this insertion failure
//				incrementRowCount("invalid_PRT");
//				// print warning and continue
//				System.err.println(String.format(
//					"Line %d of mzTab file [%s] is invalid:" +
//					"\n----------\n%s\n----------\n%s",
//					lineNumber, mzTabFilename, line,
//					getRootCause(error).getMessage()));
//				//error.printStackTrace();
//			}
		}
		// peptide section
		else if (line.startsWith("PEH")) {
			if (pepHeader != null)
				throw new IllegalArgumentException(String.format(
					"Line %d of mzTab file [%s] is invalid:" +
					"\n----------\n%s\n----------\n" +
					"A \"PEH\" row was already seen previously in this file.",
					lineNumber, mzTabFilename, line));
			pepHeader = new MzTabSectionHeader(line);
			pepHeader.validateHeaderExpectations(
				MzTabSection.PEP, Arrays.asList(RELEVANT_PEP_COLUMNS));
//		} else if (line.startsWith("PEP")) {
//			incrementRowCount("PEP");
//			if (pepHeader == null)
//				throw new IllegalArgumentException(String.format(
//					"Line %d of mzTab file [%s] is invalid:" +
//					"\n----------\n%s\n----------\n" +
//					"A \"PEP\" row was found before any \"PEH\" row.",
//					lineNumber, mzTabFilename, line));
//			else pepHeader.validateMzTabRow(line);
//			// extract insertable elements from this PEP row
//			String[] columns = line.split("\\t");
//			// record this peptide
//			Collection<Modification> modifications =
//				ProteomicsUtils.getModifications(
//					columns[pepHeader.getColumnIndex("modifications")]);
//			try {
//				cascadePeptide(
//					columns[pepHeader.getColumnIndex("sequence")],
//					columns[pepHeader.getColumnIndex("accession")],
//					modifications);
//				connection.commit();
//			} catch (Throwable error) {
//				try { connection.rollback(); } catch (Throwable innerError) {}
//				// log this insertion failure
//				incrementRowCount("invalid_PEP");
//				// print warning and continue
//				System.err.println(String.format(
//					"Line %d of mzTab file [%s] is invalid:\n" +
//					"----------\n%s\n----------\n%s",
//					lineNumber, mzTabFilename, line,
//					getRootCause(error).getMessage()));
//				//error.printStackTrace();
//			}
		}
		// PSM section
		else if (line.startsWith("PSH")) {
			if (psmHeader != null)
				throw new IllegalArgumentException(String.format(
					"Line %d of mzTab file [%s] is invalid:" +
					"\n----------\n%s\n----------\n" +
					"A \"PSH\" row was already seen previously in this file.",
					lineNumber, mzTabFilename, line));
			psmHeader = new MzTabSectionHeader(line);
			psmHeader.validateHeaderExpectations(
				MzTabSection.PSM, Arrays.asList(RELEVANT_PSM_COLUMNS));
			// determine index of controlled validity flag column, if present
			validColumn =
				psmHeader.getColumnIndex(MzTabConstants.VALID_COLUMN);
			// determine index of controlled Q-value column, if present
			qValueColumn =
				psmHeader.getColumnIndex(MzTabConstants.Q_VALUE_COLUMN);
		} else if (line.startsWith("PSM")) {
			long start = System.nanoTime();
			long checkpoint = start;
			incrementRowCount("PSM");
			if (psmHeader == null)
				throw new IllegalArgumentException(String.format(
					"Line %d of mzTab file [%s] is invalid:" +
					"\n----------\n%s\n----------\n" +
					"A \"PSM\" row was found before any \"PSH\" row.",
					lineNumber, mzTabFilename, line));
			else psmHeader.validateMzTabRow(line);
			// extract insertable elements from this PSM row
			String[] columns = line.split("\\t");
			// get this PSM's index
			String psmID = columns[psmHeader.getColumnIndex("PSM_ID")];
			if (lastPSMIndex == null)
				lastPSMIndex = 1;
			else if (psmIDs.contains(psmID) == false) {
				lastPSMIndex++;
				psmIDs.add(psmID);
			}
			// if this PSM is not explicitly marked as valid, do not import
			boolean importable = true;
			try {
				String valid = columns[validColumn];
				if (valid == null ||
					valid.trim().equalsIgnoreCase("VALID") == false)
					importable = false;
			} catch (Throwable error) {
				importable = false;
			}
			// if flag is set to only import PSMs at or below the designated
			// Q-value threshold, determine if this PSM makes the cut
			if (importable && importByQValue) try {
				double qValue = Double.parseDouble(columns[qValueColumn]);
				if (qValue > MzTabConstants.DEFAULT_IMPORT_Q_VALUE_THRESHOLD)
					importable = false;
			} catch (Throwable error) {
				importable = false;
			}
			// instantiate and validate the PSM
			Collection<Modification> modifications = null;
			PSM psm = null;
			if (importable) try {
				modifications = cleanModificationsForSearch(
					ProteomicsUtils.getModifications(
						columns[psmHeader.getColumnIndex("modifications")]));
				psm = new PSM(
					psmID, lastPSMIndex,
					columns[psmHeader.getColumnIndex("spectra_ref")],
					columns[psmHeader.getColumnIndex("sequence")],
					columns[psmHeader.getColumnIndex("charge")],
					columns[psmHeader.getColumnIndex("exp_mass_to_charge")],
					modifications
				);
				// if this PSM doesn't pass basic validation, do not import
				if (MzTabPROXIImporter.isImportable(psm) == false)
					importable = false;
			} catch (Throwable error) {
				importable = false;
			}
			long end = System.nanoTime();
			batchManager.addTiming("psm_row_preprocessing", end - checkpoint, 1);
			checkpoint = end;
			// only record this PSM if it passes the threshold
			if (importable) {
				// split protein list, if aggregated (should only be one per
				// PSM row, but mzTab producers sometimes don't follow rules)
				String[] proteins = null;
				String accession =
					columns[psmHeader.getColumnIndex("accession")];
				if (accession != null)
					proteins = accession.split(";");
				if (proteins == null)
					proteins = new String[]{null};
				// import PSM separately for each matched protein
				for (String protein : proteins) {
					// get filtered and cleaned protein accession
					String cleanedAccession =
						ProteomicsUtils.cleanProteinAccession(
							ProteomicsUtils.filterProteinAccession(protein));
					end = System.nanoTime();
					batchManager.addTiming("psm_row_protein_clean", end - checkpoint, 1);
					checkpoint = end;
					// process this PSM into the current batch
					cascadePSM(psm, cleanedAccession, modifications);
					end = System.nanoTime();
					batchManager.addTiming("psm_row_cascade", end - checkpoint, 1);
					checkpoint = end;
					// if the current batch size exceeds the insert
					// threshold, insert all current batches
					if (batchManager.maxBatchSize >= batchManager.batchInsertThreshold) try {
						insertBatch();
						end = System.nanoTime();
						batchManager.addTiming("psm_row_trigger", end - checkpoint, 1);
						checkpoint = end;
					} catch (SQLException error) {
						throw new RuntimeException(error);
					}
				}
			} else incrementRowCount("unimportable_PSM");
		}
		return line;
	}
	
	public void tearDown() {
		// insert final batch
		if (batchManager.maxBatchSize > 0) try {
			insertBatch();
		} catch (SQLException error) {
			throw new RuntimeException(error);
		}
		// close batch manager
		batchManager.close();
		// close PSMs load file writer
		if (psmsLoadFileWriter != null) try {
			psmsLoadFileWriter.close();
		} catch (Throwable error) {}
		// report import results
		StringBuilder success = new StringBuilder("Imported file [");
		success.append(mzTabRecord.mzTabFile.getMzTabFilename());
		success.append("] (");
		success.append(
			CommonUtils.formatBytes(mzTabRecord.mzTabFile.getFile().length()));
		success.append(", ");
		int lines = getRowCount("lines_in_file");
		success.append(lines).append(" ");
		success.append(CommonUtils.pluralize("line", lines));
		success.append(")");
		double seconds = 0.0;
		if (start != null) {
			long elapsed = System.nanoTime() - start;
			seconds = elapsed / 1000.0;
			success.append(" in ");
			success.append(CommonUtils.formatMilliseconds(elapsed));
			success.append(" (");
			success.append(String.format("%.2f", lines / seconds));
			success.append(" lines/second)");
		}
		success.append(".");
		success.append("\n\tPSMs:     ");
		success.append(formatRowCount(getElementCount("psm"),
			getRowCount("PSM"), getRowCount("invalid_PSM"),
			getRowCount("unimportable_PSM"), seconds));
		success.append("\n\tPeptides: ");
		success.append(formatRowCount(getElementCount("sequence"),
			getRowCount("PEP"), getRowCount("invalid_PEP"),
			getRowCount("unimportable_PEP"), seconds));
		success.append("\n\tVariants: ").append(getElementCount("variant"));
		success.append("\n\tProteins: ");
		success.append(formatRowCount(getElementCount("accession"),
			getRowCount("PRT"), getRowCount("invalid_PRT"),
			getRowCount("unimportable_PRT"), seconds));
		success.append("\n\tPTMs:     ")
			.append(getElementCount("modification"));
		success.append("\n----------");
		System.out.println(success.toString());
		// report all import timing
		dumpTiming();
	}
	
	public int getElementCount(String type) {
		if (type == null)
			return 0;
		Map<String, Integer> values = uniqueElements.get(type);
		if (values == null || values.isEmpty())
			return 0;
		else return values.size();
	}
	
	public int getRowCount(String type) {
		if (type == null)
			return 0;
		Integer count = rowCounts.get(type);
		if (count == null)
			return 0;
		else return count;
	}

    public void setDebugLogFile(File debugLogFile) {
        this.debugLogFile = debugLogFile;
    }

	/*========================================================================
	 * Convenience classes
	 *========================================================================*/
	private static class PROXIMzTabRecord {
		/*====================================================================
		 * Properties
		 *====================================================================*/
		private MzTabFile mzTabFile;
		private Integer   id;
		private String    taskID;
		private String    datasetID;
		
		/*====================================================================
		 * Constructor
		 *====================================================================*/
		public PROXIMzTabRecord() {
			// initialize properties
			mzTabFile = null;
			id = null;
			taskID = null;
			datasetID = null;
		}
	}

    /*========================================================================
     * Convenience classes
     *========================================================================*/
    private static class PROXIBatchManager {
        /*====================================================================
         * Properties
         *====================================================================*/
        // SQL INSERT statement templates
        private Map<String, PreparedStatement>               insertTemplates;
        // top-level ID collections
        // PSM -> psms.id
        private Map<PSM, Integer>                            psms;
        // file descriptor -> spectrumfiles.id
        private Map<String, Integer>                         spectrumFiles;
        // sequence -> peptides.id
        private Map<String, Integer>                         peptides;
        // sequence/charge -> variants.id
        private Map<ImmutablePair<String, Integer>, Integer> variants;
        // accession -> proteins.id
        private Map<String, Integer>                         proteins;
        // modification -> modifications.id
        private Map<String, Integer>                         modifications;
        // flat foreign key collections
        // PSM -> file descriptor/peptide sequence/variant
        private Map<PSM, ImmutableTriple<String, String, ImmutablePair<String, Integer>>>
                                                             psmProperties;
        // variant sequence/charge -> peptide sequence
        private Map<ImmutablePair<String, Integer>, String>  variantPeptides;
        // join collections
        // PSM flat 
        // dataset peptide sequence
        private Set<String>                                  datasetPeptides;
        // dataset variant sequence/charge
        private Set<ImmutablePair<String, Integer>>          datasetVariants;
        // dataset protein accession
        private Set<String>                                  datasetProteins;
        // dataset modification
        private Set<String>                                  datasetModifications;
        // resultfile peptide sequence
        private Set<String>                                  resultFilePeptides;
        // resultfile variant sequence/charge
        private Set<ImmutablePair<String, Integer>>          resultFileVariants;
        // resultfile protein accession
        private Set<String>                                  resultFileProteins;
        // resultfile modification
        private Set<String>                                  resultFileModifications;
        // PSM -> protein accessions
        private Map<PSM, Set<String>>                        psmProteins;
        // PSM -> modifications
        private Map<PSM, Set<String>>                        psmModifications;
        // peptide sequence -> protein accessions
        private Map<String, Set<String>>                     peptideProteins;
        // peptide sequence -> modifications
        private Map<String, Set<String>>                     peptideModifications;
        // variant sequence/charge -> protein accessions
        private Map<ImmutablePair<String, Integer>, Set<String>>
                                                             variantProteins;
        // variant sequence/charge -> modifications
        private Map<ImmutablePair<String, Integer>, Set<Modification>>
                                                             variantModifications;
        // protein accession -> modifications
        private Map<String, Set<String>>                     proteinModifications;
        // batch state properties
        private boolean                                      isDatasetResult;
        private Map<String, PROXITimingRecord>               timingRecords;
        private Map<String, Integer>                         newElementBatchSizes;
        private int                                          maxBatchSize;
        private int                                          totalBatchSize;
        private int                                          batchInsertThreshold;

        /*====================================================================
         * Constructor
         *====================================================================*/
        public PROXIBatchManager(
            PROXIMzTabRecord mzTabRecord, Integer batchSize, Connection connection
        ) {
            if (mzTabRecord == null)
                throw new NullPointerException("mzTab record cannot be null.");
            else if (mzTabRecord.id == null)
                throw new NullPointerException("mzTab record resultfile ID cannot be null.");
            else if (connection == null)
                throw new NullPointerException("Connection cannot be null.");
            // initialize SQL INSERT statement templates
            insertTemplates = new HashMap<String, PreparedStatement>();
            try {
                // PSMs
                StringBuilder sql = new StringBuilder(
                    "INSERT IGNORE INTO proxi.psms " +
                    "(id_in_file, index_in_file, nativeid, variant_sequence, " +
                    "charge, exp_mass_to_charge, " +
                    "resultfile_id, spectrumfile_id, peptide_id, variant_id");
                if (mzTabRecord.datasetID != null)
                    sql.append(", dataset_id");
                sql.append(String.format(
                    ") VALUES(?, ?, ?, ?, ?, %d, ?, ?, ?", mzTabRecord.id));
                if (mzTabRecord.datasetID != null)
                    sql.append(String.format(", '%s'", mzTabRecord.datasetID));
                sql.append(")");
                PreparedStatement statement = connection.prepareStatement(sql.toString());
                insertTemplates.put("psms", statement);
                // spectrum files
                sql = new StringBuilder(
                    "INSERT IGNORE INTO proxi.spectrumfiles (file_descriptor, task_id");
                if (mzTabRecord.datasetID != null)
                    sql.append(", dataset_id");
                sql.append(String.format(") VALUES(?, '%s'", mzTabRecord.taskID));
                if (mzTabRecord.datasetID != null)
                    sql.append(String.format(", '%s'", mzTabRecord.datasetID));
                sql.append(")");
                statement = connection.prepareStatement(sql.toString());
                insertTemplates.put("spectrumfiles", statement);
                // PSM proteins
                statement = connection.prepareStatement(
                    "INSERT IGNORE INTO proxi.psm_proteins (psm_id, protein_id) VALUES(?, ?)");
                insertTemplates.put("psm_proteins", statement);
                // PSM modifications
                statement = connection.prepareStatement(
                    "INSERT IGNORE INTO proxi.psm_modifications " +
                    "(psm_id, modification_id) VALUES(?, ?)");
                insertTemplates.put("psm_modifications", statement);
                // peptides
                statement = connection.prepareStatement(
                    "INSERT IGNORE INTO proxi.peptides (sequence) VALUES(?)");
                insertTemplates.put("peptides", statement);
                // resultfile peptides
                statement = connection.prepareStatement(String.format(
                    "INSERT IGNORE INTO proxi.resultfile_peptides " +
                    "(sequence, resultfile_id, peptide_id) VALUES(?, %d, ?)", mzTabRecord.id));
                insertTemplates.put("resultfile_peptides", statement);
                // peptide proteins
                statement = connection.prepareStatement(
                    "INSERT IGNORE INTO proxi.peptide_proteins " +
                    "(peptide_id, protein_id, sequence) VALUES(?, ?, ?)");
                insertTemplates.put("peptide_proteins", statement);
                // peptide modifications
                statement = connection.prepareStatement(
                    "INSERT IGNORE INTO proxi.peptide_modifications " +
                    "(peptide_id, modification_id) VALUES(?, ?)");
                insertTemplates.put("peptide_modifications", statement);
                // variants
                statement = connection.prepareStatement(
                    "INSERT IGNORE INTO proxi.variants " +
                    "(sequence, charge, peptide_id) VALUES(?, ?, ?)");
                insertTemplates.put("variants", statement);
                // resultfile variants
                statement = connection.prepareStatement(String.format(
                    "INSERT IGNORE INTO proxi.resultfile_variants " +
                    "(sequence, resultfile_id, variant_id) VALUES(?, %d, ?)", mzTabRecord.id));
                insertTemplates.put("resultfile_variants", statement);
                // variant proteins
                statement = connection.prepareStatement(
                    "INSERT IGNORE INTO proxi.variant_proteins " +
                    "(variant_id, protein_id) VALUES(?, ?)");
                insertTemplates.put("variant_proteins", statement);
                // variant modifications
                statement = connection.prepareStatement(
                    "INSERT IGNORE INTO proxi.variant_modifications " +
                    "(variant_id, modification_id, location) VALUES(?, ?, ?)");
                insertTemplates.put("variant_modifications", statement);
                // proteins
                statement = connection.prepareStatement(
                    "INSERT IGNORE INTO proxi.proteins (name) VALUES(?)");
                insertTemplates.put("proteins", statement);
                // resultfile proteins
                statement = connection.prepareStatement(String.format(
                    "INSERT IGNORE INTO proxi.resultfile_proteins " +
                    "(resultfile_id, protein_id) VALUES(%d, ?)", mzTabRecord.id));
                insertTemplates.put("resultfile_proteins", statement);
                // protein modifications
                statement = connection.prepareStatement(
                    "INSERT IGNORE INTO proxi.protein_modifications " +
                    "(protein_id, modification_id) VALUES(?, ?)");
                insertTemplates.put("protein_modifications", statement);
                // modifications
                statement = connection.prepareStatement(
                    "INSERT IGNORE INTO proxi.modifications (name, mass) VALUES(?, ?)");
                insertTemplates.put("modifications", statement);
                // resultfile modifications
                statement = connection.prepareStatement(String.format(
                    "INSERT IGNORE INTO proxi.resultfile_modifications " +
                    "(resultfile_id, modification_id) VALUES(%d, ?)", mzTabRecord.id));
                insertTemplates.put("resultfile_modifications", statement);
                // SQL INSERT statement templates that depend on dataset ID
                if (mzTabRecord.datasetID != null) {
                    // dataset peptides
                    statement = connection.prepareStatement(String.format(
                        "INSERT IGNORE INTO proxi.dataset_peptides " +
                        "(sequence, dataset_id, peptide_id) VALUES(?, '%s', ?)",
                        mzTabRecord.datasetID));
                    insertTemplates.put("dataset_peptides", statement);
                    // dataset variants
                    statement = connection.prepareStatement(String.format(
                        "INSERT IGNORE INTO proxi.dataset_variants " +
                        "(sequence, dataset_id, variant_id) VALUES(?, '%s', ?)",
                        mzTabRecord.datasetID));
                    insertTemplates.put("dataset_variants", statement);
                    // dataset proteins
                    statement = connection.prepareStatement(String.format(
                        "INSERT IGNORE INTO proxi.dataset_proteins " +
                        "(dataset_id, protein_id) VALUES('%s', ?)",
                        mzTabRecord.datasetID));
                    insertTemplates.put("dataset_proteins", statement);
                    // dataset modifications
                    statement = connection.prepareStatement(String.format(
                        "INSERT IGNORE INTO proxi.dataset_modifications " +
                        "(dataset_id, modification_id) VALUES('%s', ?)",
                        mzTabRecord.datasetID));
                    insertTemplates.put("dataset_modifications", statement);
                }
            } catch (SQLException error) {
                throw new RuntimeException(error);
            }
            // initialize batch state
            isDatasetResult = mzTabRecord.datasetID != null;
            timingRecords = new LinkedHashMap<String, PROXITimingRecord>();
            if (batchSize == null || batchSize < 1)
                batchSize = DEFAULT_INSERT_BATCH_SIZE;
            batchInsertThreshold = batchSize;
            clear();
        }

        /*====================================================================
         * Public interface methods
         *====================================================================*/
        public void clear() {
            // initialize all maps
            psms = new LinkedHashMap<PSM, Integer>();
            spectrumFiles = new LinkedHashMap<String, Integer>();
            peptides = new LinkedHashMap<String, Integer>();
            variants = new LinkedHashMap<ImmutablePair<String, Integer>, Integer>();
            proteins = new LinkedHashMap<String, Integer>();
            modifications = new LinkedHashMap<String, Integer>();
            psmProperties = new LinkedHashMap<PSM,
                ImmutableTriple<String, String, ImmutablePair<String,Integer>>>();
            variantPeptides = new LinkedHashMap<ImmutablePair<String, Integer>, String>();
            datasetPeptides = new LinkedHashSet<String>();
            datasetVariants = new LinkedHashSet<ImmutablePair<String, Integer>>();
            datasetProteins = new LinkedHashSet<String>();
            datasetModifications = new LinkedHashSet<String>();
            resultFilePeptides = new LinkedHashSet<String>();
            resultFileVariants = new LinkedHashSet<ImmutablePair<String, Integer>>();
            resultFileProteins = new LinkedHashSet<String>();
            resultFileModifications = new LinkedHashSet<String>();
            psmProteins = new LinkedHashMap<PSM, Set<String>>();
            psmModifications = new LinkedHashMap<PSM, Set<String>>();
            peptideProteins = new LinkedHashMap<String, Set<String>>();
            peptideModifications = new LinkedHashMap<String, Set<String>>();
            variantProteins = new LinkedHashMap<ImmutablePair<String, Integer>, Set<String>>();
            variantModifications =
                new LinkedHashMap<ImmutablePair<String, Integer>, Set<Modification>>();
            proteinModifications = new LinkedHashMap<String, Set<String>>();
            // clear current batch size counters
            newElementBatchSizes = new HashMap<String, Integer>();
            maxBatchSize = 0;
            totalBatchSize = 0;
        }

        public void close() {
            // close all batch statements
            for (Entry<String, PreparedStatement> entry : insertTemplates.entrySet()) {
                PreparedStatement statement = entry.getValue();
                if (statement != null) try {
                    statement.close();
                } catch (Throwable error) {}
            }
        }

        public void processSpectrumFile(String fileDescriptor, Integer id) {
            if (fileDescriptor == null)
                throw new NullPointerException("File descriptor cannot be null.");
            // if this spectrum file has already been processed in this batch, skip
            else if (spectrumFiles.containsKey(fileDescriptor))
                return;
            // update the proper batch state ID map with this spectrum file
            spectrumFiles.put(fileDescriptor, id);
            // if the ID is not known, queue an insert
            if (id == null) try {
                PreparedStatement statement = insertTemplates.get("spectrumfiles");
                if (statement == null)
                    throw new IllegalStateException(
                        "Could not find SQL INSERT statement template for table \"spectrumfiles\".");
                statement.setString(1, fileDescriptor);
                statement.addBatch();
                statement.clearParameters();
                // increment this batch size and check if max batch size should be increased
                considerBatchSize(incrementNewElementBatchSize("spectrumfiles"));
            } catch (SQLException error) {
                throw new RuntimeException(error);
            }
        }

        public void processPeptide(String sequence, Integer id) {
            if (sequence == null)
                throw new NullPointerException("Peptide sequence cannot be null.");
            // if this peptide has already been processed in this batch, skip
            else if (peptides.containsKey(sequence))
                return;
            // add this peptide to the proper batch state ID map
            peptides.put(sequence, id);
            // if the ID is not known, queue an insert
            if (id == null) try {
                PreparedStatement statement = insertTemplates.get("peptides");
                if (statement == null)
                    throw new IllegalStateException(
                        "Could not find SQL INSERT statement template for table \"peptides\".");
                statement.setString(1, sequence);
                statement.addBatch();
                statement.clearParameters();
                // increment this batch size and check if max batch size should be increased
                considerBatchSize(incrementNewElementBatchSize("peptides"));
            } catch (SQLException error) {
                throw new RuntimeException(error);
            }
        }

        public void processResultFilePeptide(
            String sequence, Integer id
        ) {
            if (sequence == null)
                throw new NullPointerException("Peptide sequence cannot be null.");
            // if the ID is known, then no join insert needs to happen
            else if (id != null)
                return;
            // if this resultfile/peptide join has already been processed in this batch, skip
            else if (resultFilePeptides.contains(sequence))
                return;
            // otherwise add this peptide to the proper batch state join set
            resultFilePeptides.add(sequence);
            // increment this batch size and check if max batch size should be increased
            considerBatchSize(incrementNewElementBatchSize("resultfile_peptides"));
        }

        public void processDatasetPeptide(
            String sequence, Integer id
        ) {
            if (sequence == null)
                throw new NullPointerException("Peptide sequence cannot be null.");
            // if this is not a dataset result, do nothing
            else if (isDatasetResult == false)
                return;
            // if the ID is known, then no join insert needs to happen
            else if (id != null)
                return;
            // if this dataset/peptide join has already been processed in this batch, skip
            else if (datasetPeptides.contains(sequence))
                return;
            // otherwise add this peptide to the proper batch state join set
            datasetPeptides.add(sequence);
            // increment this batch size and check if max batch size should be increased
            considerBatchSize(incrementNewElementBatchSize("dataset_peptides"));
        }

        public void processVariant(
            ImmutablePair<String, Integer> variant, String peptideSequence, Integer id
        ) {
            if (variant == null)
                throw new NullPointerException("Variant cannot be null.");
            else if (variant.getLeft() == null)
                throw new NullPointerException("Variant sequence cannot be null.");
            else if (variant.getRight() == null)
                throw new NullPointerException("Variant charge cannot be null.");
            else if (peptideSequence == null)
                throw new NullPointerException("Peptide sequence cannot be null.");
            // if this variant has already been processed in this batch, skip
            if (variants.containsKey(variant))
                return;
            // add this variant to the proper batch state ID map
            variants.put(variant, id);
            // if the ID is not known, queue an insert
            if (id == null) {
                // the actual insert cannot be prepared until the peptide ID is known,
                // so just map this variant to the peptide sequence for now
                variantPeptides.put(variant, peptideSequence);
                // increment this batch size and check if max batch size should be increased
                considerBatchSize(incrementNewElementBatchSize("variants"));
            }
        }

        public void processResultFileVariant(
            ImmutablePair<String, Integer> variant, Integer id
        ) {
            if (variant == null)
                throw new NullPointerException("Variant cannot be null.");
            // if the ID is known, then no join insert needs to happen
            else if (id != null)
                return;
            // if this resultfile/variant join has already been processed in this batch, skip
            else if (resultFileVariants.contains(variant))
                return;
            // otherwise add this variant to the proper batch state join set
            resultFileVariants.add(variant);
            // increment this batch size and check if max batch size should be increased
            considerBatchSize(incrementNewElementBatchSize("resultfile_variants"));
        }

        public void processDatasetVariant(
            ImmutablePair<String, Integer> variant, Integer id
        ) {
            if (variant == null)
                throw new NullPointerException("Variant cannot be null.");
            // if this is not a dataset result, do nothing
            else if (isDatasetResult == false)
                return;
            // if the ID is known, then no join insert needs to happen
            else if (id != null)
                return;
            // if this dataset/variant join has already been processed in this batch, skip
            else if (datasetVariants.contains(variant))
                return;
            // otherwise add this variant to the proper batch state join set
            datasetVariants.add(variant);
            // increment this batch size and check if max batch size should be increased
            considerBatchSize(incrementNewElementBatchSize("dataset_variants"));
        }

        public void processProtein(String accession, Integer id) {
            if (accession == null)
                throw new NullPointerException("Protein accession cannot be null.");
            // if this protein has already been processed in this batch, skip
            else if (proteins.containsKey(accession))
                return;
            // add this protein to the proper batch state ID map
            proteins.put(accession, id);
            // if the ID is not known, queue an insert
            if (id == null) try {
                PreparedStatement statement = insertTemplates.get("proteins");
                if (statement == null)
                    throw new IllegalStateException(
                        "Could not find SQL INSERT statement template for table \"proteins\".");
                statement.setString(1, accession);
                statement.addBatch();
                statement.clearParameters();
                // increment this batch size and check if max batch size should be increased
                considerBatchSize(incrementNewElementBatchSize("proteins"));
            } catch (SQLException error) {
                throw new RuntimeException(error);
            }
        }

        public void processResultFileProtein(
            String accession, Integer id
        ) {
            if (accession == null)
                throw new NullPointerException("Protein accession cannot be null.");
            // if the ID is known, then no join insert needs to happen
            else if (id != null)
                return;
            // if this resultfile/protein join has already been processed in this batch, skip
            else if (resultFileProteins.contains(accession))
                return;
            // otherwise add this protein to the proper batch state join set
            resultFileProteins.add(accession);
            // increment this batch size and check if max batch size should be increased
            considerBatchSize(incrementNewElementBatchSize("resultfile_proteins"));
        }

        public void processDatasetProtein(
            String accession, Integer id
        ) {
            if (accession == null)
                throw new NullPointerException("Protein accession cannot be null.");
            // if this is not a dataset result, do nothing
            else if (isDatasetResult == false)
                return;
            // if the ID is known, then no join insert needs to happen
            else if (id != null)
                return;
            // if this dataset/protein join has already been processed in this batch, skip
            else if (datasetProteins.contains(accession))
                return;
            // otherwise add this protein to the proper batch state join set
            datasetProteins.add(accession);
            // increment this batch size and check if max batch size should be increased
            considerBatchSize(incrementNewElementBatchSize("dataset_proteins"));
        }

        public void processModification(Modification modification, Integer id) {
            if (modification == null)
                throw new NullPointerException("Modification cannot be null.");
            // if this modification has already been processed in this batch, skip
            String name = modification.getName();
            if (modifications.containsKey(name))
                return;
            // add this modification to the proper batch state ID map
            modifications.put(name, id);
            // if the ID is not known, queue an insert
            if (id == null) try {
                PreparedStatement statement = insertTemplates.get("modifications");
                if (statement == null)
                    throw new IllegalStateException(
                        "Could not find SQL INSERT statement template for table \"modifications\".");
                statement.setString(1, modification.getName());
                Double mass = modification.getMass();
                if (mass != null)
                    statement.setDouble(2, mass);
                else statement.setNull(2, Types.DOUBLE);
                statement.addBatch();
                statement.clearParameters();
                // increment this batch size and check if max batch size should be increased
                considerBatchSize(incrementNewElementBatchSize("modifications"));
            } catch (SQLException error) {
                throw new RuntimeException(error);
            }
        }

        public void processResultFileModification(
            Modification modification, Integer id
        ) {
            if (modification == null)
                throw new NullPointerException("Modification cannot be null.");
            // if the ID is known, then no join insert needs to happen
            else if (id != null)
                return;
            // if this resultfile/modification join has already been processed in this batch, skip
            String name = modification.getName();
            if (resultFileModifications.contains(name))
                return;
            // otherwise add this modification to the proper batch state join set
            resultFileModifications.add(name);
            // increment this batch size and check if max batch size should be increased
            considerBatchSize(incrementNewElementBatchSize("resultfile_modifications"));
        }

        public void processDatasetModification(
            Modification modification, Integer id
        ) {
            if (modification == null)
                throw new NullPointerException("Modification cannot be null.");
            // if this is not a dataset result, do nothing
            else if (isDatasetResult == false)
                return;
            // if the ID is known, then no join insert needs to happen
            else if (id != null)
                return;
            // if this dataset/modification join has already been processed in this batch, skip
            String name = modification.getName();
            if (datasetModifications.contains(name))
                return;
            // otherwise add this modification to the proper batch state join set
            datasetModifications.add(name);
            // increment this batch size and check if max batch size should be increased
            considerBatchSize(incrementNewElementBatchSize("dataset_modifications"));
        }

        public void processPSM(
            PSM psm, String fileDescriptor, String peptideSequence,
            ImmutablePair<String, Integer> variant, Integer id
        ) {
            if (psm == null)
                throw new NullPointerException("PSM cannot be null.");
            else if (peptideSequence == null)
                throw new NullPointerException("Peptide sequence cannot be null.");
            else if (variant == null)
                throw new NullPointerException("Variant cannot be null.");
            else if (variant.getLeft() == null)
                throw new NullPointerException("Variant sequence cannot be null.");
            else if (variant.getRight() == null)
                throw new NullPointerException("Variant charge cannot be null.");
            // if this PSM has already been processed in this batch, skip
            if (psms.containsKey(psm))
                return;
            // add this PSM to the proper batch state ID map
            psms.put(psm, id);
            // if the ID is not known, queue an insert
            if (id == null) {
                ImmutableTriple<String, String, ImmutablePair<String, Integer>> properties =
                    new ImmutableTriple<String, String, ImmutablePair<String, Integer>>(
                        fileDescriptor, peptideSequence, variant);
                psmProperties.put(psm, properties);
                // increment this batch size and check if max batch size should be increased
                considerBatchSize(incrementNewElementBatchSize("psms"));
            }
        }

        public void processPeptideProtein(
            String sequence, String accession, Integer id
        ) {
            if (sequence == null)
                throw new NullPointerException("Peptide sequence cannot be null.");
            else if (accession == null)
                throw new NullPointerException("Protein accession cannot be null.");
            // if the ID is known, then no join insert needs to happen
            else if (id != null)
                return;
            // if this peptide/protein join has already been processed in this batch, skip
            Set<String> accessions = peptideProteins.get(sequence);
            if (accessions != null && accessions.contains(accession))
                return;
            // otherwise add this peptide/protein pair to the proper batch state join map
            if (accessions == null)
                accessions = new LinkedHashSet<String>();
            accessions.add(accession);
            peptideProteins.put(sequence, accessions);
            // increment this batch size and check if max batch size should be increased
            considerBatchSize(incrementNewElementBatchSize("peptide_proteins"));
        }

        public void processPeptideModification(
            String sequence, Modification modification, Integer id
        ) {
            if (sequence == null)
                throw new NullPointerException("Peptide sequence cannot be null.");
            else if (modification == null)
                throw new NullPointerException("Modification cannot be null.");
            // if the ID is known, then no join insert needs to happen
            else if (id != null)
                return;
            // if this peptide/modification join has already been processed in this batch, skip
            String name = modification.getName();
            Set<String> modifications = peptideModifications.get(sequence);
            if (modifications != null && modifications.contains(name))
                return;
            // otherwise add this peptide/modification pair to the proper batch state join map
            if (modifications == null)
                modifications = new LinkedHashSet<String>();
            modifications.add(name);
            peptideModifications.put(sequence, modifications);
            // increment this batch size and check if max batch size should be increased
            considerBatchSize(incrementNewElementBatchSize("peptide_modifications"));
        }

        public void processVariantProtein(
            ImmutablePair<String, Integer> variant, String accession, Integer id
        ) {
            if (variant == null)
                throw new NullPointerException("Variant cannot be null.");
            else if (variant.getLeft() == null)
                throw new NullPointerException("Variant sequence cannot be null.");
            else if (variant.getRight() == null)
                throw new NullPointerException("Variant charge cannot be null.");
            else if (accession == null)
                throw new NullPointerException("Protein accession cannot be null.");
            // if the ID is known, then no join insert needs to happen
            else if (id != null)
                return;
            // if this variant/protein join has already been processed in this batch, skip
            Set<String> accessions = variantProteins.get(variant);
            if (accessions != null && accessions.contains(accession))
                return;
            // otherwise add this variant/modification pair to the proper batch state join map
            if (accessions == null)
                accessions = new LinkedHashSet<String>();
            accessions.add(accession);
            variantProteins.put(variant, accessions);
            // increment this batch size and check if max batch size should be increased
            considerBatchSize(incrementNewElementBatchSize("variant_proteins"));
        }

        public void processVariantModification(
            ImmutablePair<String, Integer> variant, Modification modification, Integer id
        ) {
            if (variant == null)
                throw new NullPointerException("Variant cannot be null.");
            else if (variant.getLeft() == null)
                throw new NullPointerException("Variant sequence cannot be null.");
            else if (variant.getRight() == null)
                throw new NullPointerException("Variant charge cannot be null.");
            else if (modification == null)
                throw new NullPointerException("Modification cannot be null.");
            // if the ID is known, then no join insert needs to happen
            else if (id != null)
                return;
            // if the location is not known, then this variant/modification join cannot be inserted
            else if (modification.getPosition() == null)
                return;
            // if this variant/modification join has already been processed in this batch, skip
            Set<Modification> modifications = variantModifications.get(variant);
            if (modifications != null && modifications.contains(modification))
                return;
            // otherwise add this variant/modification pair to the proper batch state join map
            if (modifications == null)
                modifications = new LinkedHashSet<Modification>();
            modifications.add(modification);
            variantModifications.put(variant, modifications);
            // increment this batch size and check if max batch size should be increased
            considerBatchSize(incrementNewElementBatchSize("variant_modifications"));
        }

        public void processProteinModification(
            String accession, Modification modification, Integer id
        ) {
            if (accession == null)
                throw new NullPointerException("Protein accession cannot be null.");
            else if (modification == null)
                throw new NullPointerException("Modification cannot be null.");
            // if the ID is known, then no join insert needs to happen
            else if (id != null)
                return;
            // if this protein/modification join has already been processed in this batch, skip
            String name = modification.getName();
            Set<String> modifications = proteinModifications.get(accession);
            if (modifications != null && modifications.contains(name))
                return;
            // otherwise add this protein/modification pair to the proper batch state join map
            if (modifications == null)
                modifications = new LinkedHashSet<String>();
            modifications.add(name);
            proteinModifications.put(accession, modifications);
            // increment this batch size and check if max batch size should be increased
            considerBatchSize(incrementNewElementBatchSize("protein_modifications"));
        }

        public void processPSMProtein(
            PSM psm, String accession, Integer id
        ) {
            if (psm == null)
                throw new NullPointerException("PSM cannot be null.");
            else if (accession == null)
                throw new NullPointerException("Protein accession cannot be null.");
            // if the ID is known, then no join insert needs to happen
            else if (id != null)
                return;
            // if this PSM/protein join has already been processed in this batch, skip
            Set<String> accessions = psmProteins.get(psm);
            if (accessions != null && accessions.contains(accession))
                return;
            // otherwise add this PSM/protein pair to the proper batch state join map
            if (accessions == null)
                accessions = new LinkedHashSet<String>();
            accessions.add(accession);
            psmProteins.put(psm, accessions);
            // increment this batch size and check if max batch size should be increased
            considerBatchSize(incrementNewElementBatchSize("psm_proteins"));
        }

        public void processPSMModification(
            PSM psm, Modification modification, Integer id
        ) {
            if (psm == null)
                throw new NullPointerException("PSM cannot be null.");
            else if (modification == null)
                throw new NullPointerException("Modification cannot be null.");
            // if the ID is known, then no join insert needs to happen
            else if (id != null)
                return;
            // if this PSM/modification join has already been processed in this batch, skip
            String name = modification.getName();
            Set<String> modifications = psmModifications.get(psm);
            if (modifications != null && modifications.contains(name))
                return;
            // otherwise add this PSM/modification pair to the proper batch state join map
            if (modifications == null)
                modifications = new LinkedHashSet<String>();
            modifications.add(name);
            psmModifications.put(psm, modifications);
            // increment this batch size and check if max batch size should be increased
            considerBatchSize(incrementNewElementBatchSize("psm_modifications"));
        }

        /*====================================================================
         * Convenience methods
         *====================================================================*/
        private Integer incrementNewElementBatchSize(String type) {
            if (type == null)
                return null;
            // get current batch size for elements of this type and increment
            Integer batchSize = newElementBatchSizes.get(type);
            if (batchSize == null)
                batchSize = 1;
            else batchSize++;
            newElementBatchSizes.put(type, batchSize);
            // also increment the total batch size
            totalBatchSize++;
            return batchSize;
        }

        private void considerBatchSize(Integer batchSize) {
            if (batchSize == null)
                return;
            else if (batchSize > maxBatchSize)
                maxBatchSize = batchSize;
        }

        private void addTiming(String type, long time, int batchSize) {
            if (type == null)
                return;
            PROXITimingRecord timingRecord = timingRecords.get(type);
            if (timingRecord == null)
                timingRecord = new PROXITimingRecord();
            timingRecord.add(time, batchSize);
            timingRecords.put(type, timingRecord);
        }
    }

    private static class PROXITimingRecord {
        /*====================================================================
         * Properties
         *====================================================================*/
        private int                          count;
        private ImmutablePair<Long, Integer> total;
        private ImmutablePair<Long, Integer> max;
        private ImmutablePair<Long, Integer> min;

        /*====================================================================
         * Constructor
         *====================================================================*/
        public PROXITimingRecord() {
            count = 0;
            total = null;
            max = null;
            min = null;
        }

        /*====================================================================
         * Public interface methods
         *====================================================================*/
        public void add(long time, int batchSize) {
            ImmutablePair<Long, Integer> timing =
                new ImmutablePair<Long, Integer>(time, batchSize);
            // add this timing to running total
            if (total == null)
                total = timing;
            else total = new ImmutablePair<Long, Integer>(
                total.getLeft() + time, total.getRight() + batchSize);
            // compare this timing to current max and min
            if (max == null || max.getLeft() < time)
                max = timing;
            if (min == null || min.getLeft() > time)
                min = timing;
            // increment count
            count++;
        }

        public ImmutablePair<Double, Double> getAverage() {
            if (count < 1 || total == null)
                return null;
            else return new ImmutablePair<Double, Double>(
                (double)total.getLeft() / count, (double)total.getRight() / count);
        }

        @Override
        public String toString() {
            StringBuilder record = new StringBuilder();
            record.append(count);
            if (total != null)
                record.append("\t").append(total.getRight()).append("\t").append(total.getLeft());
            if (count > 1) {
                record.append("\t").append(max.getRight()).append("\t").append(max.getLeft());
                record.append("\t").append(min.getRight()).append("\t").append(min.getLeft());
                ImmutablePair<Double, Double> average = getAverage();
                record.append("\t").append(
                    average.getRight()).append("\t").append(average.getLeft());
            } else record.append("\t--\t--\t--\t--\t--\t--");
            return record.toString();
        }

        @Override
        public int hashCode() {
            return toString().hashCode();
        }
    }

	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private void insertMzTabFile() {
		// insert resultfile row into database
		String descriptor = mzTabRecord.mzTabFile.getDescriptor();
		PreparedStatement statement = null;
		ResultSet result = null;
		try {
			StringBuilder sql = new StringBuilder(
				"INSERT IGNORE INTO proxi.resultfiles " +
				"(file_descriptor, task_id");
			if (mzTabRecord.datasetID != null)
				sql.append(", dataset_id");
			sql.append(") VALUES(?, ?");
			if (mzTabRecord.datasetID != null)
				sql.append(", ?");
			sql.append(")");
			statement = connection.prepareStatement(
				sql.toString(), Statement.RETURN_GENERATED_KEYS);
			statement.setString(1, descriptor);
			statement.setString(2, mzTabRecord.taskID);
			if (mzTabRecord.datasetID != null)
				statement.setString(3, mzTabRecord.datasetID);
			int insertion = statement.executeUpdate();
			// if the row already exists, need to look it up manually to get ID
			if (insertion == 0) {
				try { statement.close(); } catch (Throwable error) {}
				statement = connection.prepareStatement(
					"SELECT id FROM proxi.resultfiles " +
					"WHERE file_descriptor=?");
				statement.setString(1, descriptor);
				result = statement.executeQuery();
				if (result.next())
					mzTabRecord.id = result.getInt(1);
				else throw new RuntimeException(String.format(
					"No resultfile row was found for descriptor [%s] " +
					"even though the previous insert was ignored.",
					descriptor));
			}
			// if the insert succeeded, get its generated row ID
			else if (insertion == 1) {
				result = statement.getGeneratedKeys();
				if (result.next())
					mzTabRecord.id = result.getInt(1);
				else throw new RuntimeException("The resultfile " +
					"insert statement did not generate a row ID.");
			}
			else throw new RuntimeException(String.format(
				"The resultfile insert statement returned a value of \"%d\".",
				insertion));
		} catch (Throwable error) {
			throw new RuntimeException("Error recording resultfile: There " +
				"was an error inserting the resultfile row into the database.",
				error);
		} finally {
			try { statement.close(); } catch (Throwable error) {}
			try { result.close(); } catch (Throwable error) {}
		}
	}

    private void cascadePSM(
        PSM psm, String accession, Collection<Modification> modifications
    ) {
        if (psm == null)
            return;
        // retrieve and process this PSM's spectrum file
        int msRun = psm.getMsRun();
        MzTabMsRun spectrumFile = mzTabRecord.mzTabFile.getMsRun(msRun);
        if (spectrumFile == null)
            throw new NullPointerException(String.format(
                "No spectrum file could be found " +
                "for ms_run[%d] of mzTab file [%s].",
                msRun, mzTabRecord.mzTabFile.getMzTabFilename()));
        String fileDescriptor = spectrumFile.getDescriptor();
        processSpectrumFile(fileDescriptor);
        // process this PSM's peptide and variant
        String peptideSequence = psm.getSequence();
        processPeptide(peptideSequence);
        ImmutablePair<String, Integer> variant =
            new ImmutablePair<String, Integer>(psm.getModifiedSequence(), psm.getCharge());
        processVariant(variant, peptideSequence);
        // process this PSM's protein and related joins
        processProtein(accession);
        processPeptideProtein(peptideSequence, accession);
        processVariantProtein(variant, accession);
        // process this PSM itself and related joins
        processPSM(psm, fileDescriptor, peptideSequence, variant);
        processPSMProtein(psm, accession);
        // process all modifications and realated joins
        if (modifications != null && modifications.isEmpty() == false) {
            for (Modification modification : modifications) {
                processModification(modification);
                processPeptideModification(peptideSequence, modification);
                processVariantModification(variant, modification);
                processProteinModification(accession, modification);
                processPSMModification(psm, modification);
            }
        }
    }

    private void processSpectrumFile(String fileDescriptor) {
        if (fileDescriptor == null)
            return;
        // process this spectrum file into the current batch
        batchManager.processSpectrumFile(
            fileDescriptor, getElementID("spectrumFile", fileDescriptor));
    }

    private void processPeptide(String sequence) {
        if (sequence == null)
            return;
        // process this peptide into the current batch
        batchManager.processPeptide(sequence, getElementID("sequence", sequence));
        // process this resultfile/peptide join into the current batch
        batchManager.processResultFilePeptide(sequence, getElementID("resultFilePeptide", sequence));
        // if this is a dataset result, process this dataset/peptide join into the current batch
        if (batchManager.isDatasetResult)
            batchManager.processDatasetPeptide(sequence,
                getElementID("datasetPeptide",
                    String.format("%s_%s", mzTabRecord.datasetID, sequence)));
    }

    private void processVariant(ImmutablePair<String, Integer> variant, String peptideSequence) {
        if (variant == null || peptideSequence == null)
            return;
        // process this variant into the current batch
        String variantID = String.format("%s_%d", variant.getLeft(), variant.getRight());
        batchManager.processVariant(variant, peptideSequence, getElementID("variant", variantID));
        // process this resultfile/variant join into the current batch
        batchManager.processResultFileVariant(variant,
            getElementID("resultFileVariant", variantID));
        // if this is a dataset result, process this dataset/variant join into the current batch
        if (batchManager.isDatasetResult)
            batchManager.processDatasetVariant(variant,
                getElementID("datasetVariant",
                    String.format("%s_%s", mzTabRecord.datasetID, variantID)));
    }

    private void processProtein(String accession) {
        if (accession == null)
            return;
        // process this protein into the current batch
        batchManager.processProtein(accession, getElementID("accession", accession));
        // process this resultfile/protein join into the current batch
        batchManager.processResultFileProtein(accession,
            getElementID("resultFileProtein", accession));
        // if this is a dataset result, process this dataset/protein join into the current batch
        if (batchManager.isDatasetResult)
            batchManager.processDatasetProtein(accession,
                getElementID("datasetProtein",
                    String.format("%s_%s", mzTabRecord.datasetID, accession)));
    }

    private void processModification(Modification modification) {
        if (modification == null)
            return;
        // process this modification into the current batch
        batchManager.processModification(
            modification, getElementID("modification", modification.getName()));
        // process this resultfile/modification join into the current batch
        batchManager.processResultFileModification(modification,
            getElementID("resultFileModification", modification.getName()));
        // if this is a dataset result, process this dataset/modification join into the current batch
        if (batchManager.isDatasetResult)
            batchManager.processDatasetModification(modification,
                getElementID("datasetModification",
                    String.format("%s_%s", mzTabRecord.datasetID, modification.getName())));
    }

    private void processPSM(
        PSM psm, String spectrumFileDescriptor, String peptideSequence,
        ImmutablePair<String, Integer> variant
    ) {
        if (psm == null || spectrumFileDescriptor == null || peptideSequence == null ||
            variant == null)
            return;
        // process this PSM into the current batch
        batchManager.processPSM(psm, spectrumFileDescriptor, peptideSequence, variant,
            getElementID("psm", psm.getID().toString()));
    }

    private void processPeptideProtein(String sequence, String accession) {
        if (sequence == null || accession == null)
            return;
        // process this peptide/protein join into the current batch
        batchManager.processPeptideProtein(sequence, accession,
            getElementID("peptideProtein", String.format("%s_%s", sequence, accession)));
    }

    private void processPeptideModification(String sequence, Modification modification) {
        if (sequence == null || modification == null)
            return;
        // process this peptide/modification join into the current batch
        batchManager.processPeptideModification(sequence, modification,
            getElementID("peptideModification",
                String.format("%s_%s", sequence, modification.getName())));
    }
    
    private void processVariantProtein(ImmutablePair<String, Integer> variant, String accession) {
        if (variant == null || accession == null)
            return;
        // process this variant/protein join into the current batch
        batchManager.processVariantProtein(variant, accession,
            getElementID("variantProtein",
                String.format("%s_%d_%s", variant.getLeft(), variant.getRight(), accession)));
    }

    private void processVariantModification(
        ImmutablePair<String, Integer> variant, Modification modification
    ) {
        if (variant == null || modification == null)
            return;
        // if the location is not known, then this variant/modification join cannot be inserted
        Integer position = modification.getPosition();
        if (position == null)
            return;
        // otherwise process this variant/modification join into the current batch
        batchManager.processVariantModification(variant, modification,
            getElementID("variantModification", String.format("%s_%d_%s_%d",
                variant.getLeft(), variant.getRight(), modification.getName(), position)));
    }

    private void processProteinModification(String accession, Modification modification) {
        if (accession == null || modification == null)
            return;
        // process this protein/modification join into the current batch
        batchManager.processProteinModification(accession, modification,
            getElementID("proteinModification",
                String.format("%s_%s", accession, modification.getName())));
    }

    private void processPSMProtein(PSM psm, String accession) {
        if (psm == null || accession == null)
            return;
        // process this PSM/protein join into the current batch
        batchManager.processPSMProtein(psm, accession,
            getElementID("psmProtein", String.format("%s_%s", psm.getID().toString(), accession)));
    }

    private void processPSMModification(PSM psm, Modification modification) {
        if (psm == null || modification == null)
            return;
        // process this PSM/modification join into the current batch
        batchManager.processPSMModification(psm, modification,
            getElementID("psmModification", String.format("%s_%s",
                psm.getID().toString(), modification.getName())));
    }

    private void insertBatch() throws SQLException {
        // execute insert batches in the proper order
        insertSpectrumFilesBatch();
        insertPeptidesBatch();
        insertResultFilePeptidesBatch();
        insertDatasetPeptidesBatch();
        insertVariantsBatch();
        insertResultFileVariantsBatch();
        insertDatasetVariantsBatch();
        insertPSMsBatch();
        insertProteinsBatch();
        insertResultFileProteinsBatch();
        insertDatasetProteinsBatch();
        insertPeptideProteinsBatch();
        insertVariantProteinsBatch();
        insertPSMProteinsBatch();
        insertModificationsBatch();
        insertResultFileModificationsBatch();
        insertDatasetModificationsBatch();
        insertPeptideModificationsBatch();
        insertVariantModificationsBatch();
        insertProteinModificationsBatch();
        insertPSMModificationsBatch();
        // clear batch state
        batchManager.clear();
    }

    private void insertSpectrumFilesBatch() throws SQLException {
        long start = System.nanoTime();
        // get file descriptors for all new spectrum files
        List<String> values = new ArrayList<String>(batchManager.spectrumFiles.size());
        for (Entry<String, Integer> entry : batchManager.spectrumFiles.entrySet())
            if (entry.getValue() == null)
                values.add(entry.getKey());
        // if there were any new spectrum files, insert the batch
        PreparedStatement statement = null;
        ResultSet result = null;
        if (values.isEmpty() == false) {
            statement = batchManager.insertTemplates.get("spectrumfiles");
            if (statement == null)
                throw new IllegalStateException("Could not find SQL INSERT " +
                    "statement template for table \"spectrumfiles\".");
            long checkpoint = System.nanoTime();
            int[] results = statement.executeBatch();
            long end = System.nanoTime();
            batchManager.addTiming("spectrumfiles_insert", end - checkpoint, results.length);
            // query IDs of all spectrum file rows just inserted
            StringBuilder valueSet = new StringBuilder("(");
            for (int i=0; i<values.size(); i++)
                valueSet.append("?,");
            if (valueSet.charAt(valueSet.length() - 1) == ',')
                valueSet.setLength(valueSet.length() - 1);
            valueSet.append(")");
            statement = connection.prepareStatement(String.format(
                "SELECT id, file_descriptor " +
                "FROM proxi.spectrumfiles " +
                "WHERE file_descriptor IN %s", valueSet.toString()));
            for (int i=0; i<values.size(); i++)
                statement.setString((i+1), values.get(i));
            checkpoint = System.nanoTime();
            result = statement.executeQuery();
            end = System.nanoTime();
            // store queried IDs for subsequent inserts to use
            int resultCount = 0;
            while (result.next()) {
                resultCount++;
                String fileDescriptor = result.getString("file_descriptor");
                int id = result.getInt("id");
                batchManager.spectrumFiles.put(fileDescriptor, id);
                addElement("spectrumFile", fileDescriptor, id);
                values.remove(fileDescriptor);
            }
            batchManager.addTiming("spectrumfiles_query", end - checkpoint, resultCount);
            result.close();
            statement.close();
            // IDs should now be stored for all new spectrum files
            if (values.isEmpty() == false) {
                // verify missing IDs in the actual ID map
                Collection<String> missingIDs = new LinkedHashSet<String>(values.size());
                for (Entry<String, Integer> entry : batchManager.spectrumFiles.entrySet())
                    if (entry.getValue() == null)
                        missingIDs.add(entry.getKey());
                if (missingIDs.isEmpty() == false) {
                    StringBuilder message = new StringBuilder(
                        "ERROR inserting spectrumfiles batch: Even after inserting the " +
                        "latest batch, IDs are still missing for the following set ");
                    message.append("(size ").append(missingIDs.size()).append("):");
                    message.append("\n----------");
                    for (String fileDescriptor : missingIDs)
                        message.append("\n").append(fileDescriptor);
                    message.append("\n----------");
                    throw new IllegalStateException(message.toString());
                } else {
                    StringBuilder message = new StringBuilder(
                        "WARNING: After inserting the latest batch of new " +
                        "spectrumfiles rows, no IDs are missing, but follow-up query " +
                        "somehow did not return IDs for the following set ");
                    message.append("(size ").append(values.size()).append("):");
                    message.append("\n----------");
                    for (String fileDescriptor : values) {
                        message.append("\n").append(fileDescriptor);
                        message.append(" : ").append(batchManager.spectrumFiles.get(fileDescriptor));
                    }
                    message.append("\n----------");
                    System.out.println(message.toString());
                }
            }
        }
        long end = System.nanoTime();
        batchManager.addTiming("spectrumfiles", end - start, 1);
    }

    private void insertPeptidesBatch() throws SQLException {
        long start = System.nanoTime();
        // get sequences for all new peptides
        List<String> values = new ArrayList<String>(batchManager.peptides.size());
        for (Entry<String, Integer> entry : batchManager.peptides.entrySet())
            if (entry.getValue() == null)
                values.add(entry.getKey());
        // if there were any new peptides, insert the batch
        PreparedStatement statement = null;
        ResultSet result = null;
        if (values.isEmpty() == false) {
            statement = batchManager.insertTemplates.get("peptides");
            if (statement == null)
                throw new IllegalStateException("Could not find SQL INSERT " +
                    "statement template for table \"peptides\".");
            long checkpoint = System.nanoTime();
            int[] results = statement.executeBatch();
            long end = System.nanoTime();
            batchManager.addTiming("peptides_insert", end - checkpoint, results.length);
            // query IDs of all peptide rows just inserted
            StringBuilder valueSet = new StringBuilder("(");
            for (int i=0; i<values.size(); i++)
                valueSet.append("?,");
            if (valueSet.charAt(valueSet.length() - 1) == ',')
                valueSet.setLength(valueSet.length() - 1);
            valueSet.append(")");
            statement = connection.prepareStatement(String.format(
                "SELECT id, sequence " +
                "FROM proxi.peptides " +
                "WHERE sequence IN %s", valueSet.toString()));
            for (int i=0; i<values.size(); i++)
                statement.setString((i+1), values.get(i));
            checkpoint = System.nanoTime();
            result = statement.executeQuery();
            end = System.nanoTime();
            // store queried IDs for subsequent inserts to use
            int resultCount = 0;
            while (result.next()) {
                resultCount++;
                String sequence = result.getString("sequence");
                int id = result.getInt("id");
                batchManager.peptides.put(sequence, id);
                addElement("sequence", sequence, id);
                values.remove(sequence);
            }
            batchManager.addTiming("peptides_query", end - checkpoint, resultCount);
            result.close();
            statement.close();
            // IDs should now be stored for all new peptides
            if (values.isEmpty() == false) {
                // verify missing IDs in the actual ID map
                Collection<String> missingIDs = new LinkedHashSet<String>(values.size());
                for (Entry<String, Integer> entry : batchManager.peptides.entrySet())
                    if (entry.getValue() == null)
                        missingIDs.add(entry.getKey());
                if (missingIDs.isEmpty() == false) {
                    StringBuilder message = new StringBuilder(
                        "ERROR inserting peptides batch: Even after inserting the " +
                        "latest batch, IDs are still missing for the following set ");
                    message.append("(size ").append(missingIDs.size()).append("):");
                    message.append("\n----------");
                    for (String sequence : missingIDs)
                        message.append("\n").append(sequence);
                    message.append("\n----------");
                    throw new IllegalStateException(message.toString());
                } else {
                    StringBuilder message = new StringBuilder(
                        "WARNING: After inserting the latest batch of new " +
                        "peptides rows, no IDs are missing, but follow-up query " +
                        "somehow did not return IDs for the following set ");
                    message.append("(size ").append(values.size()).append("):");
                    message.append("\n----------");
                    for (String sequence : values) {
                        message.append("\n").append(sequence);
                        message.append(" : ").append(batchManager.peptides.get(sequence));
                    }
                    message.append("\n----------");
                    System.out.println(message.toString());
                }
            }
        }
        long end = System.nanoTime();
        batchManager.addTiming("peptides", end - start, 1);
    }

    private void insertResultFilePeptidesBatch() throws SQLException {
        long start = System.nanoTime();
        // get all new resultfile/peptide join pairs
        Set<String> values = batchManager.resultFilePeptides;
        // if there were any new joins, set up the batch
        // (using recently generated peptide IDs) and insert it
        PreparedStatement statement = null;
        if (values.isEmpty() == false) {
            statement = batchManager.insertTemplates.get("resultfile_peptides");
            if (statement == null)
                throw new IllegalStateException("Could not find SQL INSERT " +
                    "statement template for table \"resultfile_peptides\".");
            long checkpoint = System.nanoTime();
            for (String sequence : values) {
                Integer peptideID = batchManager.peptides.get(sequence);
                if (peptideID == null)
                    throw new IllegalStateException(String.format(
                        "ERROR setting up batch insert for resultfile/peptide join [%d/%s]: " +
                        "No database ID has been determined yet for this peptide.",
                        mzTabRecord.id, sequence));
                statement.setString(1, sequence);
                statement.setInt(2, peptideID);
                statement.addBatch();
                statement.clearParameters();
            }
            long end = System.nanoTime();
            batchManager.addTiming("resultfile_peptides_batch", end - checkpoint, values.size());
            checkpoint = end;
            int[] results = statement.executeBatch();
            end = System.nanoTime();
            batchManager.addTiming("resultfile_peptides_insert", end - checkpoint, results.length);
            // note each resultfile/peptide join pair as having been inserted with a dummy ID of 1 -
            // no need to actually query for these rows' IDs, just note that they're there
            for (String sequence : values)
                addElement("resultFilePeptide", sequence, 1, true);
        }
        long end = System.nanoTime();
        batchManager.addTiming("resultfile_peptides", end - start, 1);
    }

    private void insertDatasetPeptidesBatch() throws SQLException {
        // if this is not a dataset result, do nothing
        if (batchManager.isDatasetResult == false)
            return;
        long start = System.nanoTime();
        // get all new dataset/peptide join pairs
        Set<String> values = batchManager.datasetPeptides;
        // if there were any new joins, set up the batch
        // (using recently generated peptide IDs) and insert it
        PreparedStatement statement = null;
        if (values.isEmpty() == false) {
            statement = batchManager.insertTemplates.get("dataset_peptides");
            if (statement == null)
                throw new IllegalStateException("Could not find SQL INSERT " +
                    "statement template for table \"dataset_peptides\".");
            long checkpoint = System.nanoTime();
            for (String sequence : values) {
                Integer peptideID = batchManager.peptides.get(sequence);
                if (peptideID == null)
                    throw new IllegalStateException(String.format(
                        "ERROR setting up batch insert for dataset/peptide join [%s/%s]: " +
                        "No database ID has been determined yet for this peptide.",
                        mzTabRecord.datasetID, sequence));
                statement.setString(1, sequence);
                statement.setInt(2, peptideID);
                statement.addBatch();
                statement.clearParameters();
            }
            long end = System.nanoTime();
            batchManager.addTiming("dataset_peptides_batch", end - checkpoint, values.size());
            checkpoint = end;
            int[] results = statement.executeBatch();
            end = System.nanoTime();
            batchManager.addTiming("dataset_peptides_insert", end - checkpoint, results.length);
            // note each dataset/peptide join pair as having been inserted with a dummy ID of 1 -
            // no need to actually query for these rows' IDs, just note that they're there
            for (String sequence : values)
                addElement("datasetPeptide",
                    String.format("%s_%s", mzTabRecord.datasetID, sequence), 1);
        }
        long end = System.nanoTime();
        batchManager.addTiming("dataset_peptides", end - start, 1);
    }

    private void insertVariantsBatch() throws SQLException {
        long start = System.nanoTime();
        // get sequence/charge for all new variants
        List<ImmutablePair<String, Integer>> values =
            new ArrayList<ImmutablePair<String, Integer>>(batchManager.variants.size());
        for (Entry<ImmutablePair<String, Integer>, Integer> entry : batchManager.variants.entrySet())
            if (entry.getValue() == null)
                values.add(entry.getKey());
        // if there were any new variants, set up the batch
        // (using recently generated peptide IDs) and insert it
        PreparedStatement statement = null;
        ResultSet result = null;
        if (values.isEmpty() == false) {
            statement = batchManager.insertTemplates.get("variants");
            if (statement == null)
                throw new IllegalStateException("Could not find SQL INSERT " +
                    "statement template for table \"variants\".");
            long checkpoint = System.nanoTime();
            for (ImmutablePair<String, Integer> variant : values) {
                String peptideSequence = batchManager.variantPeptides.get(variant);
                if (peptideSequence == null)
                    throw new IllegalStateException(String.format(
                        "ERROR setting up batch insert for variant [%s/%d]: " +
                        "No peptide sequence was registered for this variant.",
                        variant.getLeft(), variant.getRight()));
                Integer peptideID = batchManager.peptides.get(peptideSequence);
                if (peptideID == null)
                    throw new IllegalStateException(String.format(
                        "ERROR setting up batch insert for variant [%s/%d]: " +
                        "No database ID has been determined yet for associated peptide [%s].",
                        variant.getLeft(), variant.getRight(), peptideSequence));
                statement.setString(1, variant.getLeft());
                statement.setInt(2, variant.getRight());
                statement.setInt(3, peptideID);
                statement.addBatch();
                statement.clearParameters();
            }
            long end = System.nanoTime();
            batchManager.addTiming("variants_batch", end - checkpoint, values.size());
            checkpoint = end;
            int[] results = statement.executeBatch();
            end = System.nanoTime();
            batchManager.addTiming("variants_insert", end - checkpoint, results.length);
            // query IDs of all variant rows just inserted
            StringBuilder valueSet = new StringBuilder("(");
            for (int i=0; i<values.size(); i++)
                valueSet.append("(?,?),");
            if (valueSet.charAt(valueSet.length() - 1) == ',')
                valueSet.setLength(valueSet.length() - 1);
            valueSet.append(")");
            statement = connection.prepareStatement(String.format(
                "SELECT id, sequence, charge " +
                "FROM proxi.variants " +
                "WHERE (sequence, charge) IN %s", valueSet.toString()));
            int valueCounter = 1;
            for (ImmutablePair<String, Integer> variant : values) {
                statement.setString(valueCounter++, variant.getLeft());
                statement.setInt(valueCounter++, variant.getRight());
            }
            checkpoint = System.nanoTime();
            result = statement.executeQuery();
            end = System.nanoTime();
            // store queried IDs for subsequent inserts to use
            int resultCount = 0;
            while (result.next()) {
                resultCount++;
                String sequence = result.getString("sequence");
                int charge = result.getInt("charge");
                int id = result.getInt("id");
                ImmutablePair<String, Integer> variant =
                    new ImmutablePair<String, Integer>(sequence, charge);
                batchManager.variants.put(variant, id);
                addElement("variant", String.format("%s_%d", sequence, charge), id);
                values.remove(variant);
            }
            batchManager.addTiming("variants_query", end - checkpoint, resultCount);
            result.close();
            statement.close();
            // IDs should now be stored for all new variants
            if (values.isEmpty() == false) {
                // verify missing IDs in the actual ID map
                Collection<ImmutablePair<String, Integer>> missingIDs =
                    new LinkedHashSet<ImmutablePair<String, Integer>>(values.size());
                for (Entry<ImmutablePair<String, Integer>, Integer> entry :
                    batchManager.variants.entrySet())
                    if (entry.getValue() == null)
                        missingIDs.add(entry.getKey());
                if (missingIDs.isEmpty() == false) {
                    StringBuilder message = new StringBuilder(
                        "ERROR inserting variants batch: Even after inserting the " +
                        "latest batch, IDs are still missing for the following set ");
                    message.append("(size ").append(missingIDs.size()).append("):");
                    message.append("\n----------");
                    for (ImmutablePair<String, Integer> variant : missingIDs)
                        message.append("\n").append(variant.getLeft())
                            .append("/").append(variant.getRight());
                    message.append("\n----------");
                    throw new IllegalStateException(message.toString());
                } else {
                    StringBuilder message = new StringBuilder(
                        "WARNING: After inserting the latest batch of new " +
                        "peptides rows, no IDs are missing, but follow-up query " +
                        "somehow did not return IDs for the following set ");
                    message.append("(size ").append(values.size()).append("):");
                    message.append("\n----------");
                    for (ImmutablePair<String, Integer> variant : values) {
                        message.append("\n").append(variant.getLeft())
                            .append("/").append(variant.getRight());
                        message.append(" : ").append(batchManager.variants.get(variant));
                    }
                    message.append("\n----------");
                    System.out.println(message.toString());
                }
            }
        }
        long end = System.nanoTime();
        batchManager.addTiming("variants", end - start, 1);
    }

    private void insertResultFileVariantsBatch() throws SQLException {
        long start = System.nanoTime();
        // get all new resultfile/variant join pairs
        Set<ImmutablePair<String, Integer>> values = batchManager.resultFileVariants;
        // if there were any new joins, set up the batch
        // (using recently generated variant IDs) and insert it
        PreparedStatement statement = null;
        if (values.isEmpty() == false) {
            statement = batchManager.insertTemplates.get("resultfile_variants");
            if (statement == null)
                throw new IllegalStateException("Could not find SQL INSERT " +
                    "statement template for table \"resultfile_variants\".");
            long checkpoint = System.nanoTime();
            for (ImmutablePair<String, Integer> variant : values) {
                Integer variantID = batchManager.variants.get(variant);
                if (variantID == null)
                    throw new IllegalStateException(String.format(
                        "ERROR setting up batch insert for resultfile/variant join [%d/%s/%d]: " +
                        "No database ID has been determined yet for this variant.",
                        mzTabRecord.id, variant.getLeft(), variant.getRight()));
                statement.setString(1, variant.getLeft());
                statement.setInt(2, variantID);
                statement.addBatch();
                statement.clearParameters();
            }
            long end = System.nanoTime();
            batchManager.addTiming("resultfile_variants_batch", end - checkpoint, values.size());
            checkpoint = end;
            int[] results = statement.executeBatch();
            end = System.nanoTime();
            batchManager.addTiming("resultfile_variants_insert", end - checkpoint, results.length);
            // note each resultfile/variant join pair as having been inserted with a dummy ID of 1 -
            // no need to actually query for these rows' IDs, just note that they're there
            for (ImmutablePair<String, Integer> variant : values)
                addElement("resultFileVariant",
                    String.format("%s_%d", variant.getLeft(), variant.getRight()), 1, true);
        }
        long end = System.nanoTime();
        batchManager.addTiming("resultfile_variants", end - start, 1);
    }

    private void insertDatasetVariantsBatch() throws SQLException {
        // if this is not a dataset result, do nothing
        if (batchManager.isDatasetResult == false)
            return;
        long start = System.nanoTime();
        // get all new dataset/variant join pairs
        Set<ImmutablePair<String, Integer>> values = batchManager.datasetVariants;
        // if there were any new joins, set up the batch
        // (using recently generated variant IDs) and insert it
        PreparedStatement statement = null;
        if (values.isEmpty() == false) {
            statement = batchManager.insertTemplates.get("dataset_variants");
            if (statement == null)
                throw new IllegalStateException("Could not find SQL INSERT " +
                    "statement template for table \"dataset_variants\".");
            long checkpoint = System.nanoTime();
            for (ImmutablePair<String, Integer> variant : values) {
                Integer variantID = batchManager.variants.get(variant);
                if (variantID == null)
                    throw new IllegalStateException(String.format(
                        "ERROR setting up batch insert for dataset/variant join [%s/%s/%d]: " +
                        "No database ID has been determined yet for this peptide.",
                        mzTabRecord.datasetID, variant.getLeft(), variant.getRight()));
                statement.setString(1, variant.getLeft());
                statement.setInt(2, variantID);
                statement.addBatch();
                statement.clearParameters();
            }
            long end = System.nanoTime();
            batchManager.addTiming("dataset_variants_batch", end - checkpoint, values.size());
            checkpoint = end;
            int[] results = statement.executeBatch();
            end = System.nanoTime();
            batchManager.addTiming("dataset_variants_insert", end - checkpoint, results.length);
            // note each dataset/variant join pair as having been inserted with a dummy ID of 1 -
            // no need to actually query for these rows' IDs, just note that they're there
            for (ImmutablePair<String, Integer> variant : values)
                addElement("datasetVariant",
                    String.format("%s_%s_%d", mzTabRecord.datasetID,
                        variant.getLeft(), variant.getRight()), 1);
        }
        long end = System.nanoTime();
        batchManager.addTiming("dataset_variants", end - start, 1);
    }

    private void insertPSMsBatch() throws SQLException {
        long start = System.nanoTime();
        // get all new PSMs
        Map<String, PSM> values = new LinkedHashMap<String, PSM>(batchManager.psms.size());
        for (Entry<PSM, Integer> entry : batchManager.psms.entrySet()) {
            if (entry.getValue() == null) {
                PSM psm = entry.getKey();
                values.put(psm.getID().toString(), psm);
            }
        }
        // if there were any new PSMs, set up the batch
        // (using recently generated spectrumfile/peptide/variant IDs) and insert it
        PreparedStatement statement = null;
        ResultSet result = null;
        if (values.isEmpty() == false) {
            statement = batchManager.insertTemplates.get("psms");
            if (statement == null)
                throw new IllegalStateException("Could not find SQL INSERT " +
                    "statement template for table \"psms\".");
            long checkpoint = System.nanoTime();
            for (Entry<String, PSM> entry : values.entrySet()) {
                PSM psm = entry.getValue();
                ImmutableTriple<String, String, ImmutablePair<String, Integer>> properties =
                    batchManager.psmProperties.get(psm);
                if (properties == null)
                    throw new IllegalStateException(String.format(
                        "ERROR setting up batch insert for PSM [%s]: " +
                        "No properties tuple was registered for this PSM.", entry.getKey()));
                String spectrumFileDescriptor = properties.getLeft();
                if (spectrumFileDescriptor == null)
                    throw new IllegalStateException(String.format(
                        "ERROR setting up batch insert for PSM [%s]: " +
                        "Properties tuple for this PSM has null spectrum file descriptor.",
                        entry.getKey()));
                Integer spectrumFileID = batchManager.spectrumFiles.get(spectrumFileDescriptor);
                if (spectrumFileID == null)
                    throw new IllegalStateException(String.format(
                        "ERROR setting up batch insert for PSM [%s]: " +
                        "No database ID has been determined yet for associated spectrum file [%s].",
                        entry.getKey(), spectrumFileDescriptor));
                String peptideSequence = properties.getMiddle();
                if (peptideSequence == null)
                    throw new IllegalStateException(String.format(
                        "ERROR setting up batch insert for PSM [%s]: " +
                        "Properties tuple for this PSM has null peptide sequence.",
                        entry.getKey()));
                Integer peptideID = batchManager.peptides.get(peptideSequence);
                if (peptideID == null)
                    throw new IllegalStateException(String.format(
                        "ERROR setting up batch insert for PSM [%s]: " +
                        "No database ID has been determined yet for associated peptide [%s].",
                        entry.getKey(), peptideSequence));
                ImmutablePair<String, Integer> variant = properties.getRight();
                if (variant == null)
                    throw new IllegalStateException(String.format(
                        "ERROR setting up batch insert for PSM [%s]: " +
                        "Properties tuple for this PSM has null variant.",
                        entry.getKey()));
                Integer variantID = batchManager.variants.get(variant);
                if (variantID == null)
                    throw new IllegalStateException(String.format(
                        "ERROR setting up batch insert for PSM [%s]: " +
                        "No database ID has been determined yet for associated variant [%s/%d].",
                        entry.getKey(), variant.getLeft(), variant.getRight()));
                statement.setString(1, psm.getID());
                statement.setInt(2, psm.getIndex());
                statement.setString(3, psm.getNativeID());
                statement.setString(4, psm.getModifiedSequence());
                statement.setInt(5, psm.getCharge());
                Double massToCharge = psm.getMassToCharge();
                if (massToCharge == null)
                    statement.setNull(6, Types.DOUBLE);
                else statement.setDouble(6, massToCharge);
                statement.setInt(7, spectrumFileID);
                statement.setInt(8, peptideID);
                statement.setInt(9, variantID);
                statement.addBatch();
                statement.clearParameters();
                // write this PSM to the load file
                StringBuilder loadFileLine = new StringBuilder();
                loadFileLine.append(psm.getID());
                loadFileLine.append("\t").append(psm.getNativeID());
                loadFileLine.append("\t").append(psm.getModifiedSequence());
                loadFileLine.append("\t").append(psm.getCharge());
                loadFileLine.append("\t").append(massToCharge != null ? massToCharge : "NULL");
                loadFileLine.append("\t").append(mzTabRecord.id);
                loadFileLine.append("\t").append(spectrumFileID);
                loadFileLine.append("\t").append(peptideID);
                loadFileLine.append("\t").append(variantID);
                psmsLoadFileWriter.println(loadFileLine.toString());
            }
            long end = System.nanoTime();
            batchManager.addTiming("psms_batch", end - checkpoint, values.size());
            checkpoint = end;
            int[] results = statement.executeBatch();
            end = System.nanoTime();
            batchManager.addTiming("psms_insert", end - checkpoint, results.length);
            // query IDs of all PSM rows just inserted
            StringBuilder valueSet = new StringBuilder("(");
            for (int i=0; i<values.size(); i++)
                valueSet.append("?,");
            if (valueSet.charAt(valueSet.length() - 1) == ',')
                valueSet.setLength(valueSet.length() - 1);
            valueSet.append(")");
            statement = connection.prepareStatement(String.format(
                "SELECT id, id_in_file " +
                "FROM proxi.psms " +
                "WHERE resultfile_id=? " +
                "AND id_in_file IN %s", valueSet.toString()));
            statement.setInt(1, mzTabRecord.id);
            int valueCounter = 2;
            for (Entry<String, PSM> entry : values.entrySet())
                statement.setString(valueCounter++, entry.getValue().getID());
            checkpoint = System.nanoTime();
            result = statement.executeQuery();
            end = System.nanoTime();
            // store queried IDs for subsequent inserts to use
            int resultCount = 0;
            while (result.next()) {
                resultCount++;
                String psmID = Integer.toString(result.getInt("id_in_file"));
                int id = result.getInt("id");
                PSM psm = values.get(psmID);
                if (psm == null)
                    throw new IllegalStateException(String.format(
                        "ERROR inserting PSMs batch: batch insert completed " +
                        "successfully, but follow-up query returned PSM ID [%s] " +
                        "that could not be found in the new PSMs map.", psmID));
                batchManager.psms.put(psm, id);
                addElement("psm", psmID, id, true);
                values.remove(psmID);
            }
            batchManager.addTiming("psms_query", end - checkpoint, resultCount);
            result.close();
            statement.close();
            // IDs should now be stored for all new PSMs
            if (values.isEmpty() == false) {
                // verify missing IDs in the actual ID map
                Collection<PSM> missingIDs = new LinkedHashSet<PSM>(values.size());
                for (Entry<PSM, Integer> entry : batchManager.psms.entrySet())
                    if (entry.getValue() == null)
                        missingIDs.add(entry.getKey());
                if (missingIDs.isEmpty() == false) {
                    StringBuilder message = new StringBuilder(
                        "ERROR inserting PSMs batch: Even after inserting the " +
                        "latest batch, IDs are still missing for the following set ");
                    message.append("(size ").append(missingIDs.size()).append("):");
                    message.append("\n----------");
                    for (PSM psm : missingIDs)
                        message.append("\n").append(psm.getID());
                    message.append("\n----------");
                    throw new IllegalStateException(message.toString());
                } else {
                    StringBuilder message = new StringBuilder(
                        "WARNING: After inserting the latest batch of new " +
                        "psms rows, no IDs are missing, but follow-up query " +
                        "somehow did not return IDs for the following set ");
                    message.append("(size ").append(values.size()).append("):");
                    message.append("\n----------");
                    for (Entry<String, PSM> entry : values.entrySet()) {
                        message.append("\n").append(entry.getKey());
                        message.append(" : ").append(
                            batchManager.psms.get(entry.getValue()));
                    }
                    message.append("\n----------");
                    System.out.println(message.toString());
                }
            }
        }
        long end = System.nanoTime();
        batchManager.addTiming("psms", end - start, 1);
    }

    private void insertProteinsBatch() throws SQLException {
        long start = System.nanoTime();
        // get accessions for all new proteins
        List<String> values = new ArrayList<String>(batchManager.proteins.size());
        for (Entry<String, Integer> entry : batchManager.proteins.entrySet())
            if (entry.getValue() == null)
                values.add(entry.getKey());
        // if there were any new proteins, insert the batch
        PreparedStatement statement = null;
        ResultSet result = null;
        if (values.isEmpty() == false) {
            statement = batchManager.insertTemplates.get("proteins");
            if (statement == null)
                throw new IllegalStateException("Could not find SQL INSERT " +
                    "statement template for table \"proteins\".");
            long checkpoint = System.nanoTime();
            int[] results = statement.executeBatch();
            long end = System.nanoTime();
            batchManager.addTiming("proteins_insert", end - checkpoint, results.length);
            // query IDs of all protein rows just inserted
            StringBuilder valueSet = new StringBuilder("(");
            for (int i=0; i<values.size(); i++)
                valueSet.append("?,");
            if (valueSet.charAt(valueSet.length() - 1) == ',')
                valueSet.setLength(valueSet.length() - 1);
            valueSet.append(")");
            statement = connection.prepareStatement(String.format(
                "SELECT id, name " +
                "FROM proxi.proteins " +
                "WHERE name IN %s", valueSet.toString()));
            for (int i=0; i<values.size(); i++)
                statement.setString((i+1), values.get(i));
            checkpoint = System.nanoTime();
            result = statement.executeQuery();
            end = System.nanoTime();
            // store queried IDs for subsequent inserts to use
            int resultCount = 0;
            while (result.next()) {
                resultCount++;
                String accession = result.getString("name");
                int id = result.getInt("id");
                batchManager.proteins.put(accession, id);
                addElement("accession", accession, id);
                values.remove(accession);
            }
            batchManager.addTiming("proteins_query", end - checkpoint, resultCount);
            result.close();
            statement.close();
            // IDs should now be stored for all new proteins
            if (values.isEmpty() == false) {
                // verify missing IDs in the actual ID map
                Collection<String> missingIDs = new LinkedHashSet<String>(values.size());
                for (Entry<String, Integer> entry : batchManager.proteins.entrySet())
                    if (entry.getValue() == null)
                        missingIDs.add(entry.getKey());
                if (missingIDs.isEmpty() == false) {
                    StringBuilder message = new StringBuilder(
                        "ERROR inserting proteins batch: Even after inserting the " +
                        "latest batch, IDs are still missing for the following set ");
                    message.append("(size ").append(missingIDs.size()).append("):");
                    message.append("\n----------");
                    for (String accession : missingIDs)
                        message.append("\n").append(accession);
                    message.append("\n----------");
                    throw new IllegalStateException(message.toString());
                } else {
                    StringBuilder message = new StringBuilder(
                        "WARNING: After inserting the latest batch of new " +
                        "proteins rows, no IDs are missing, but follow-up query " +
                        "somehow did not return IDs for the following set ");
                    message.append("(size ").append(values.size()).append("):");
                    message.append("\n----------");
                    for (String accession : values) {
                        message.append("\n").append(accession);
                        message.append(" : ").append(batchManager.proteins.get(accession));
                    }
                    message.append("\n----------");
                    System.out.println(message.toString());
                }
            }
        }
        long end = System.nanoTime();
        batchManager.addTiming("proteins", end - start, 1);
    }

    private void insertResultFileProteinsBatch() throws SQLException {
        long start = System.nanoTime();
        // get all new resultfile/protein join pairs
        Set<String> values = batchManager.resultFileProteins;
        // if there were any new joins, set up the batch
        // (using recently generated protein IDs) and insert it
        PreparedStatement statement = null;
        if (values.isEmpty() == false) {
            statement = batchManager.insertTemplates.get("resultfile_proteins");
            if (statement == null)
                throw new IllegalStateException("Could not find SQL INSERT " +
                    "statement template for table \"resultfile_proteins\".");
            long checkpoint = System.nanoTime();
            for (String accession : values) {
                Integer proteinID = batchManager.proteins.get(accession);
                if (proteinID == null)
                    throw new IllegalStateException(String.format(
                        "ERROR setting up batch insert for resultfile/protein join [%d/%s]: " +
                        "No database ID has been determined yet for this protein.",
                        mzTabRecord.id, accession));
                statement.setInt(1, proteinID);
                statement.addBatch();
                statement.clearParameters();
            }
            long end = System.nanoTime();
            batchManager.addTiming("resultfile_proteins_batch", end - checkpoint, values.size());
            checkpoint = end;
            int[] results = statement.executeBatch();
            end = System.nanoTime();
            batchManager.addTiming("resultfile_proteins_insert", end - checkpoint, results.length);
            // note each resultfile/protein join pair as having been inserted with a dummy ID of 1 -
            // no need to actually query for these rows' IDs, just note that they're there
            for (String accession : values)
                addElement("resultFileProtein", accession, 1, true);
        }
        long end = System.nanoTime();
        batchManager.addTiming("resultfile_proteins", end - start, 1);
    }

    private void insertDatasetProteinsBatch() throws SQLException {
        // if this is not a dataset result, do nothing
        if (batchManager.isDatasetResult == false)
            return;
        long start = System.nanoTime();
        // get all new dataset/protein join pairs
        Set<String> values = batchManager.datasetProteins;
        // if there were any new joins, set up the batch
        // (using recently generated protein IDs) and insert it
        PreparedStatement statement = null;
        if (values.isEmpty() == false) {
            statement = batchManager.insertTemplates.get("dataset_proteins");
            if (statement == null)
                throw new IllegalStateException("Could not find SQL INSERT " +
                    "statement template for table \"dataset_proteins\".");
            long checkpoint = System.nanoTime();
            for (String accession : values) {
                Integer proteinID = batchManager.proteins.get(accession);
                if (proteinID == null)
                    throw new IllegalStateException(String.format(
                        "ERROR setting up batch insert for dataset/protein join [%s/%s]: " +
                        "No database ID has been determined yet for this protein.",
                        mzTabRecord.datasetID, accession));
                statement.setInt(1, proteinID);
                statement.addBatch();
                statement.clearParameters();
            }
            long end = System.nanoTime();
            batchManager.addTiming("dataset_proteins_batch", end - checkpoint, values.size());
            checkpoint = end;
            int[] results = statement.executeBatch();
            end = System.nanoTime();
            batchManager.addTiming("dataset_proteins_insert", end - checkpoint, results.length);
            // note each dataset/protein join pair as having been inserted with a dummy ID of 1 -
            // no need to actually query for these rows' IDs, just note that they're there
            for (String accession : values)
                addElement("datasetProtein",
                    String.format("%s_%s", mzTabRecord.datasetID, accession), 1);
        }
        long end = System.nanoTime();
        batchManager.addTiming("dataset_proteins", end - start, 1);
    }

    private void insertPeptideProteinsBatch() throws SQLException {
        long start = System.nanoTime();
        // get all new peptide/protein join pairs
        Map<String, Set<String>> values = batchManager.peptideProteins;
        // if there were any new joins, set up the batch
        // (using recently generated peptide and protein IDs) and insert it
        PreparedStatement statement = null;
        if (values.isEmpty() == false) {
            statement = batchManager.insertTemplates.get("peptide_proteins");
            if (statement == null)
                throw new IllegalStateException("Could not find SQL INSERT " +
                    "statement template for table \"peptide_proteins\".");
            long checkpoint = System.nanoTime();
            int insertCounter = 0;
            for (Entry<String, Set<String>> entry : values.entrySet()) {
                String sequence = entry.getKey();
                Integer peptideID = batchManager.peptides.get(sequence);
                if (peptideID == null)
                    throw new IllegalStateException(String.format(
                        "ERROR setting up batch insert for peptide/protein joins [%s]: " +
                        "No database ID has been determined yet for this peptide.", sequence));
                Set<String> accessions = entry.getValue();
                for (String accession : accessions) {
                    insertCounter++;
                    Integer proteinID = batchManager.proteins.get(accession);
                    if (proteinID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for peptide/protein join [%s/%s]: " +
                            "No database ID has been determined yet for this protein.",
                            sequence, accession));
                    statement.setInt(1, peptideID);
                    statement.setInt(2, proteinID);
                    statement.setString(3, sequence);
                    statement.addBatch();
                    statement.clearParameters();
                }
            }
            long end = System.nanoTime();
            batchManager.addTiming("peptide_proteins_batch", end - checkpoint, insertCounter);
            checkpoint = end;
            int[] results = statement.executeBatch();
            end = System.nanoTime();
            batchManager.addTiming("peptide_proteins_insert", end - checkpoint, results.length);
            // note each peptide/protein join pair as having been inserted with a dummy ID of 1 -
            // no need to actually query for these rows' IDs, just note that they're there
            for (Entry<String, Set<String>> entry : values.entrySet()) {
                for (String accession : entry.getValue()) {
                    addElement("peptideProtein",
                        String.format("%s_%s", entry.getKey(), accession), 1);
                }
            }
        }
        long end = System.nanoTime();
        batchManager.addTiming("peptide_proteins", end - start, 1);
    }

    private void insertVariantProteinsBatch() throws SQLException {
        long start = System.nanoTime();
        // get all new variant/protein join pairs
        Map<ImmutablePair<String, Integer>, Set<String>> values = batchManager.variantProteins;
        // if there were any new joins, set up the batch
        // (using recently generated variant and protein IDs) and insert it
        PreparedStatement statement = null;
        if (values.isEmpty() == false) {
            statement = batchManager.insertTemplates.get("variant_proteins");
            if (statement == null)
                throw new IllegalStateException("Could not find SQL INSERT " +
                    "statement template for table \"variant_proteins\".");
            long checkpoint = System.nanoTime();
            int insertCounter = 0;
            for (Entry<ImmutablePair<String, Integer>, Set<String>> entry : values.entrySet()) {
                ImmutablePair<String, Integer> variant = entry.getKey();
                Integer variantID = batchManager.variants.get(variant);
                if (variantID == null)
                    throw new IllegalStateException(String.format(
                        "ERROR setting up batch insert for variant/protein joins [%s/%d]: " +
                        "No database ID has been determined yet for this variant.",
                        variant.getLeft(), variant.getRight()));
                Set<String> accessions = entry.getValue();
                for (String accession : accessions) {
                    insertCounter++;
                    Integer proteinID = batchManager.proteins.get(accession);
                    if (proteinID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for variant/protein join [%s/%d/%s]: " +
                            "No database ID has been determined yet for this protein.",
                            variant.getLeft(), variant.getRight(), accession));
                    statement.setInt(1, variantID);
                    statement.setInt(2, proteinID);
                    statement.addBatch();
                    statement.clearParameters();
                }
            }
            long end = System.nanoTime();
            batchManager.addTiming("variant_proteins_batch", end - checkpoint, insertCounter);
            checkpoint = end;
            int[] results = statement.executeBatch();
            end = System.nanoTime();
            batchManager.addTiming("variant_proteins_insert", end - checkpoint, results.length);
            // note each variant/protein join pair as having been inserted with a dummy ID of 1 -
            // no need to actually query for these rows' IDs, just note that they're there
            for (Entry<ImmutablePair<String, Integer>, Set<String>> entry : values.entrySet()) {
                for (String accession : entry.getValue()) {
                    ImmutablePair<String, Integer> variant = entry.getKey();
                    addElement("variantProtein",
                        String.format("%s_%d_%s", variant.getLeft(), variant.getRight(), accession),
                        1);
                }
            }
        }
        long end = System.nanoTime();
        batchManager.addTiming("variant_proteins", end - start, 1);
    }

    private void insertPSMProteinsBatch() throws SQLException {
        long start = System.nanoTime();
        // get all new PSM/protein join pairs
        Map<PSM, Set<String>> values = batchManager.psmProteins;
        // if there were any new joins, set up the batch
        // (using recently generated PSM and protein IDs) and insert it
        PreparedStatement statement = null;
        if (values.isEmpty() == false) {
            statement = batchManager.insertTemplates.get("psm_proteins");
            if (statement == null)
                throw new IllegalStateException("Could not find SQL INSERT " +
                    "statement template for table \"psm_proteins\".");
            long checkpoint = System.nanoTime();
            int insertCounter = 0;
            for (Entry<PSM, Set<String>> entry : values.entrySet()) {
                PSM psm = entry.getKey();
                Integer psmID = batchManager.psms.get(psm);
                if (psmID == null)
                    throw new IllegalStateException(String.format(
                        "ERROR setting up batch insert for PSM/protein joins [%s]: " +
                        "No database ID has been determined yet for this PSM.",
                        psm.getID().toString()));
                Set<String> accessions = entry.getValue();
                for (String accession : accessions) {
                    insertCounter++;
                    Integer proteinID = batchManager.proteins.get(accession);
                    if (proteinID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for PSM/protein join [%s/%s]: " +
                            "No database ID has been determined yet for this protein.",
                            psm.getID().toString(), accession));
                    statement.setInt(1, psmID);
                    statement.setInt(2, proteinID);
                    statement.addBatch();
                    statement.clearParameters();
                }
            }
            long end = System.nanoTime();
            batchManager.addTiming("psm_proteins_batch", end - checkpoint, insertCounter);
            checkpoint = end;
            int[] results = statement.executeBatch();
            end = System.nanoTime();
            batchManager.addTiming("psm_proteins_insert", end - checkpoint, results.length);
            // note each PSM/protein join pair as having been inserted with a dummy ID of 1 -
            // no need to actually query for these rows' IDs, just note that they're there
            for (Entry<PSM, Set<String>> entry : values.entrySet()) {
                for (String accession : entry.getValue()) {
                    addElement("psmProtein",
                        String.format("%s_%s", entry.getKey().getID().toString(), accession),
                        1, true);
                }
            }
        }
        long end = System.nanoTime();
        batchManager.addTiming("psm_proteins", end - start, 1);
    }

    private void insertModificationsBatch() throws SQLException {
        long start = System.nanoTime();
        // get all new modifications
        List<String> values = new ArrayList<String>(batchManager.modifications.size());
        for (Entry<String, Integer> entry : batchManager.modifications.entrySet())
            if (entry.getValue() == null)
                values.add(entry.getKey());
        // if there were any new modifications, insert the batch
        PreparedStatement statement = null;
        ResultSet result = null;
        if (values.isEmpty() == false) {
            statement = batchManager.insertTemplates.get("modifications");
            if (statement == null)
                throw new IllegalStateException("Could not find SQL INSERT " +
                    "statement template for table \"modifications\".");
            long checkpoint = System.nanoTime();
            int[] results = statement.executeBatch();
            long end = System.nanoTime();
            batchManager.addTiming("modifications_insert", end - checkpoint, results.length);
            // query IDs of all modification rows just inserted
            StringBuilder valueSet = new StringBuilder("(");
            for (int i=0; i<values.size(); i++)
                valueSet.append("?,");
            if (valueSet.charAt(valueSet.length() - 1) == ',')
                valueSet.setLength(valueSet.length() - 1);
            valueSet.append(")");
            statement = connection.prepareStatement(String.format(
                "SELECT id, name " +
                "FROM proxi.modifications " +
                "WHERE name IN %s", valueSet.toString()));
            for (int i=0; i<values.size(); i++)
                statement.setString((i+1), values.get(i));
            checkpoint = System.nanoTime();
            result = statement.executeQuery();
            end = System.nanoTime();
            // store queried IDs for subsequent inserts to use
            int resultCount = 0;
            while (result.next()) {
                resultCount++;
                String name = result.getString("name");
                int id = result.getInt("id");
                batchManager.modifications.put(name, id);
                addElement("modification", name, id);
                values.remove(name);
            }
            batchManager.addTiming("modifications_query", end - checkpoint, resultCount);
            result.close();
            statement.close();
            // IDs should now be stored for all new modifications
            if (values.isEmpty() == false) {
                // verify missing IDs in the actual ID map
                Collection<String> missingIDs = new LinkedHashSet<String>(values.size());
                for (Entry<String, Integer> entry : batchManager.modifications.entrySet())
                    if (entry.getValue() == null)
                        missingIDs.add(entry.getKey());
                if (missingIDs.isEmpty() == false) {
                    StringBuilder message = new StringBuilder(
                        "ERROR inserting modifications batch: Even after inserting the " +
                        "latest batch, IDs are still missing for the following set ");
                    message.append("(size ").append(missingIDs.size()).append("):");
                    message.append("\n----------");
                    for (String modification : missingIDs)
                        message.append("\n").append(modification);
                    message.append("\n----------");
                    throw new IllegalStateException(message.toString());
                } else {
                    StringBuilder message = new StringBuilder(
                        "WARNING: After inserting the latest batch of new " +
                        "modifications rows, no IDs are missing, but follow-up query " +
                        "somehow did not return IDs for the following set ");
                    message.append("(size ").append(values.size()).append("):");
                    message.append("\n----------");
                    for (String modification : values) {
                        message.append("\n").append(modification);
                        message.append(" : ").append(batchManager.modifications.get(modification));
                    }
                    message.append("\n----------");
                    System.out.println(message.toString());
                }
            }
        }
        long end = System.nanoTime();
        batchManager.addTiming("proteins", end - start, 1);
    }

    private void insertResultFileModificationsBatch() throws SQLException {
        long start = System.nanoTime();
        // get all new resultfile/modification join pairs
        Set<String> values = batchManager.resultFileModifications;
        // if there were any new joins, set up the batch
        // (using recently generated modification IDs) and insert it
        PreparedStatement statement = null;
        if (values.isEmpty() == false) {
            statement = batchManager.insertTemplates.get("resultfile_modifications");
            if (statement == null)
                throw new IllegalStateException("Could not find SQL INSERT " +
                    "statement template for table \"resultfile_modifications\".");
            long checkpoint = System.nanoTime();
            for (String modification : values) {
                Integer modificationID = batchManager.modifications.get(modification);
                if (modificationID == null)
                    throw new IllegalStateException(String.format(
                        "ERROR setting up batch insert for resultfile/modification join [%d/%s]: " +
                        "No database ID has been determined yet for this modification.",
                        mzTabRecord.id, modification));
                statement.setInt(1, modificationID);
                statement.addBatch();
                statement.clearParameters();
            }
            long end = System.nanoTime();
            batchManager.addTiming(
                "resultfile_modifications_batch", end - checkpoint, values.size());
            checkpoint = end;
            int[] results = statement.executeBatch();
            end = System.nanoTime();
            batchManager.addTiming(
                "resultfile_modifications_insert", end - checkpoint, results.length);
            // note each resultfile/modification join pair as having been inserted with a dummy ID
            // of 1 - no need to actually query for these rows' IDs, just note that they're there
            for (String modification : values)
                addElement("resultFileModification", modification, 1, true);
        }
        long end = System.nanoTime();
        batchManager.addTiming("resultfile_modifications", end - start, 1);
    }

    private void insertDatasetModificationsBatch() throws SQLException {
        // if this is not a dataset result, do nothing
        if (batchManager.isDatasetResult == false)
            return;
        long start = System.nanoTime();
        // get all new dataset/modifications join pairs
        Set<String> values = batchManager.datasetModifications;
        // if there were any new joins, set up the batch
        // (using recently generated modifications IDs) and insert it
        PreparedStatement statement = null;
        if (values.isEmpty() == false) {
            statement = batchManager.insertTemplates.get("dataset_modifications");
            if (statement == null)
                throw new IllegalStateException("Could not find SQL INSERT " +
                    "statement template for table \"dataset_modifications\".");
            long checkpoint = System.nanoTime();
            for (String modification : values) {
                Integer modificationID = batchManager.modifications.get(modification);
                if (modificationID == null)
                    throw new IllegalStateException(String.format(
                        "ERROR setting up batch insert for dataset/modification join [%s/%s]: " +
                        "No database ID has been determined yet for this modification.",
                        mzTabRecord.datasetID, modification));
                statement.setInt(1, modificationID);
                statement.addBatch();
                statement.clearParameters();
            }
            long end = System.nanoTime();
            batchManager.addTiming("dataset_modifications_batch", end - checkpoint, values.size());
            checkpoint = end;
            int[] results = statement.executeBatch();
            end = System.nanoTime();
            batchManager.addTiming("dataset_modifications_insert", end - checkpoint, results.length);
            // note each dataset/modification join pair as having been inserted with a dummy ID
            // of 1 - no need to actually query for these rows' IDs, just note that they're there
            for (String modification : values)
                addElement("datasetModification",
                    String.format("%s_%s", mzTabRecord.datasetID, modification), 1);
        }
        long end = System.nanoTime();
        batchManager.addTiming("dataset_modifications", end - start, 1);
    }

    private void insertPeptideModificationsBatch() throws SQLException {
        long start = System.nanoTime();
        // get all new peptide/modification join pairs
        Map<String, Set<String>> values = batchManager.peptideModifications;
        // if there were any new joins, set up the batch
        // (using recently generated peptide and modification IDs) and insert it
        PreparedStatement statement = null;
        if (values.isEmpty() == false) {
            statement = batchManager.insertTemplates.get("peptide_modifications");
            if (statement == null)
                throw new IllegalStateException("Could not find SQL INSERT " +
                    "statement template for table \"peptide_modifications\".");
            long checkpoint = System.nanoTime();
            int insertCounter = 0;
            for (Entry<String, Set<String>> entry : values.entrySet()) {
                String sequence = entry.getKey();
                Integer peptideID = batchManager.peptides.get(sequence);
                if (peptideID == null)
                    throw new IllegalStateException(String.format(
                        "ERROR setting up batch insert for peptide/modification joins [%s]: " +
                        "No database ID has been determined yet for this peptide.", sequence));
                Set<String> modifications = entry.getValue();
                for (String modification : modifications) {
                    insertCounter++;
                    Integer modificationID = batchManager.modifications.get(modification);
                    if (modificationID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for peptide/modification join [%s/%s]: " +
                            "No database ID has been determined yet for this modification.",
                            sequence, modification));
                    statement.setInt(1, peptideID);
                    statement.setInt(2, modificationID);
                    statement.addBatch();
                    statement.clearParameters();
                }
            }
            long end = System.nanoTime();
            batchManager.addTiming("peptide_modifications_batch", end - checkpoint, insertCounter);
            checkpoint = end;
            int[] results = statement.executeBatch();
            end = System.nanoTime();
            batchManager.addTiming("peptide_modifications_insert", end - checkpoint, results.length);
            // note each peptide/modification join pair as having been inserted with a dummy ID of 1 -
            // no need to actually query for these rows' IDs, just note that they're there
            for (Entry<String, Set<String>> entry : values.entrySet()) {
                for (String modification : entry.getValue()) {
                    addElement("peptideModification",
                        String.format("%s_%s", entry.getKey(), modification), 1);
                }
            }
        }
        long end = System.nanoTime();
        batchManager.addTiming("peptide_modifications", end - start, 1);
    }

    private void insertVariantModificationsBatch() throws SQLException {
        long start = System.nanoTime();
        // get all new variant/modification join pairs
        Map<ImmutablePair<String, Integer>, Set<Modification>> values =
            batchManager.variantModifications;
        // if there were any new joins, set up the batch
        // (using recently generated variant and modification IDs) and insert it
        PreparedStatement statement = null;
        if (values.isEmpty() == false) {
            statement = batchManager.insertTemplates.get("variant_modifications");
            if (statement == null)
                throw new IllegalStateException("Could not find SQL INSERT " +
                    "statement template for table \"variant_modifications\".");
            long checkpoint = System.nanoTime();
            int insertCounter = 0;
            for (Entry<ImmutablePair<String, Integer>, Set<Modification>> entry :
                values.entrySet()) {
                ImmutablePair<String, Integer> variant = entry.getKey();
                Integer variantID = batchManager.variants.get(variant);
                if (variantID == null)
                    throw new IllegalStateException(String.format(
                        "ERROR setting up batch insert for variant/modification joins [%s/%d]: " +
                        "No database ID has been determined yet for this variant.",
                        variant.getLeft(), variant.getRight()));
                Set<Modification> modifications = entry.getValue();
                for (Modification modification : modifications) {
                    insertCounter++;
                    Integer modificationID = batchManager.modifications.get(modification.getName());
                    if (modificationID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for variant/modification join " +
                            "[%s/%d/%s/%d]: No database ID has been determined yet for this " +
                            "modification.", variant.getLeft(), variant.getRight(),
                            modification.getName(), modification.getPosition()));
                    statement.setInt(1, variantID);
                    statement.setInt(2, modificationID);
                    statement.setInt(3, modification.getPosition());
                    statement.addBatch();
                    statement.clearParameters();
                }
            }
            long end = System.nanoTime();
            batchManager.addTiming("variant_modifications_batch", end - checkpoint, insertCounter);
            checkpoint = end;
            int[] results = statement.executeBatch();
            end = System.nanoTime();
            batchManager.addTiming("variant_modifications_insert", end - checkpoint, results.length);
            // note each variant/modification join pair as having been inserted with a dummy ID
            // of 1 - no need to actually query for these rows' IDs, just note that they're there
            for (Entry<ImmutablePair<String, Integer>, Set<Modification>> entry : values.entrySet()) {
                ImmutablePair<String, Integer> variant = entry.getKey();
                for (Modification modification : entry.getValue()) {
                    // if the location is not known, then this
                    // variant/modification join cannot be inserted
                    Integer position = modification.getPosition();
                    if (position == null)
                        continue;
                    addElement("variantModification", String.format("%s_%d_%s_%d",
                        variant.getLeft(), variant.getRight(), modification.getName(), position), 1);
                }
            }
        }
        long end = System.nanoTime();
        batchManager.addTiming("variant_modifications", end - start, 1);
    }

    private void insertProteinModificationsBatch() throws SQLException {
        long start = System.nanoTime();
        // get all new protein/modification join pairs
        Map<String, Set<String>> values = batchManager.proteinModifications;
        // if there were any new joins, set up the batch
        // (using recently generated protein and modification IDs) and insert it
        PreparedStatement statement = null;
        if (values.isEmpty() == false) {
            statement = batchManager.insertTemplates.get("protein_modifications");
            if (statement == null)
                throw new IllegalStateException("Could not find SQL INSERT " +
                    "statement template for table \"protein_modifications\".");
            long checkpoint = System.nanoTime();
            int insertCounter = 0;
            for (Entry<String, Set<String>> entry : values.entrySet()) {
                String accession = entry.getKey();
                Integer proteinID = batchManager.proteins.get(accession);
                if (proteinID == null)
                    throw new IllegalStateException(String.format(
                        "ERROR setting up batch insert for protein/modification joins [%s]: " +
                        "No database ID has been determined yet for this protein.", accession));
                Set<String> modifications = entry.getValue();
                for (String modification : modifications) {
                    insertCounter++;
                    Integer modificationID = batchManager.modifications.get(modification);
                    if (modificationID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for protein/modification join [%s/%s]: " +
                            "No database ID has been determined yet for this modification.",
                            accession, modification));
                    statement.setInt(1, proteinID);
                    statement.setInt(2, modificationID);
                    statement.addBatch();
                    statement.clearParameters();
                }
            }
            long end = System.nanoTime();
            batchManager.addTiming("protein_modifications_batch", end - checkpoint, insertCounter);
            checkpoint = end;
            int[] results = statement.executeBatch();
            end = System.nanoTime();
            batchManager.addTiming("protein_modifications_insert", end - checkpoint, results.length);
            // note each protein/modification join pair as having been inserted with a dummy ID of 1 -
            // no need to actually query for these rows' IDs, just note that they're there
            for (Entry<String, Set<String>> entry : values.entrySet()) {
                for (String modification : entry.getValue()) {
                    addElement("proteinModification",
                        String.format("%s_%s", entry.getKey(), modification), 1);
                }
            }
        }
        long end = System.nanoTime();
        batchManager.addTiming("protein_modifications", end - start, 1);
    }

    private void insertPSMModificationsBatch() throws SQLException {
        long start = System.nanoTime();
        // get all new PSM/modification join pairs
        Map<PSM, Set<String>> values = batchManager.psmModifications;
        // if there were any new joins, set up the batch
        // (using recently generated PSM and modification IDs) and insert it
        PreparedStatement statement = null;
        if (values.isEmpty() == false) {
            statement = batchManager.insertTemplates.get("psm_modifications");
            if (statement == null)
                throw new IllegalStateException("Could not find SQL INSERT " +
                    "statement template for table \"psm_modifications\".");
            long checkpoint = System.nanoTime();
            int insertCounter = 0;
            for (Entry<PSM, Set<String>> entry : values.entrySet()) {
                PSM psm = entry.getKey();
                Integer psmID = batchManager.psms.get(psm);
                if (psmID == null)
                    throw new IllegalStateException(String.format(
                        "ERROR setting up batch insert for PSM/modification joins [%s]: " +
                        "No database ID has been determined yet for this PSM.",
                        psm.getID().toString()));
                Set<String> modifications = entry.getValue();
                for (String modification : modifications) {
                    insertCounter++;
                    Integer modificationID = batchManager.modifications.get(modification);
                    if (modificationID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for PSM/modification join [%s/%s]: " +
                            "No database ID has been determined yet for this modification.",
                            psm.getID().toString(), modification));
                    statement.setInt(1, psmID);
                    statement.setInt(2, modificationID);
                    statement.addBatch();
                    statement.clearParameters();
                }
            }
            long end = System.nanoTime();
            batchManager.addTiming("psm_modifications_batch", end - checkpoint, insertCounter);
            checkpoint = end;
            int[] results = statement.executeBatch();
            end = System.nanoTime();
            batchManager.addTiming("psm_modifications_insert", end - checkpoint, results.length);
            // note each protein/modification join pair as having been inserted with a dummy ID of 1 -
            // no need to actually query for these rows' IDs, just note that they're there
            for (Entry<PSM, Set<String>> entry : values.entrySet()) {
                for (String modification : entry.getValue()) {
                    addElement("psmModification", String.format("%s_%s",
                        entry.getKey().getID().toString(), modification), 1, true);
                }
            }
        }
        long end = System.nanoTime();
        batchManager.addTiming("psm_modifications", end - start, 1);
    }

	private Collection<Modification> cleanModificationsForSearch(
		Collection<Modification> modifications
	) {
		if (modifications == null)
			return null;
		Collection<Modification> cleaned =
			new LinkedHashSet<Modification>(modifications.size());
		for (Modification modification : modifications) {
			// clean CHEMMODs to use 1 digit of precision
			Matcher matcher = MzTabConstants.MZTAB_CHEMMOD_PATTERN.matcher(
				modification.getName());
			if (matcher.matches()) try {
				int mass = (int)Math.rint(Double.parseDouble(matcher.group(1)));
				String name = String.format("CHEMMOD:%s%d",
					mass >= 0 ? "+" : "", mass);
				cleaned.add(
					new Modification(name, modification.getPositions()));
			} catch (NumberFormatException error) {}
			// just pass through any other kind of mod
			else cleaned.add(modification);
		}
		return cleaned;
	}
	
	private Integer getElementID(String type, String value) {
		if (type == null || value == null ||
			value.trim().equalsIgnoreCase("null"))
			return 0;
		Map<String, Integer> values = null;
		// get element from global map first, if present
		if (globalElements != null)
			values = globalElements.get(type);
		// then try to get it from the local map
		if (values == null || values.containsKey(value) == false)
			values = uniqueElements.get(type);
		if (values == null || values.containsKey(value) == false)
			return null;
		else return values.get(value);
	}
	
	private void addElement(String type, String value, int id) {
		addElement(type, value, id, false);
	}
	
	private void addElement(
		String type, String value, int id, boolean localOnly
	) {
		if (type == null || value == null ||
			value.trim().equalsIgnoreCase("null"))
			return;
		// add to local map
		Map<String, Integer> values = uniqueElements.get(type);
		if (values == null)
			values = new HashMap<String, Integer>();
		values.put(value, id);
		uniqueElements.put(type, values);
		// add to global map, if requested and present
		if (localOnly == false && globalElements != null) {
			values = globalElements.get(type);
			if (values == null)
				values = new HashMap<String, Integer>();
			values.put(value, id);
			globalElements.put(type, values);
		}
	}
	
	private void incrementRowCount(String type) {
		if (type == null)
			return;
		Integer count = rowCounts.get(type);
		if (count == null)
			count = 0;
		rowCounts.put(type, count + 1);
	}
	
	private String formatRowCount(
		int elements, int rows, int invalid, int unimported, double seconds
	) {
		StringBuilder count = new StringBuilder().append(elements);
		if (rows > 0) {
			count.append(" (").append(rows).append(" ");
			count.append(CommonUtils.pluralize("row", rows));
			if (invalid > 0) {
				count.append(", ").append(invalid).append(" invalid ");
				count.append(CommonUtils.pluralize("row", invalid));
			}
			if (unimported > 0) {
				count.append(", ").append(unimported).append(" unimported ");
				count.append(CommonUtils.pluralize("row", unimported));
			}
			if (seconds > 0.0) {
				count.append(", ");
				count.append(String.format("%.2f", rows / seconds));
				count.append(" rows/second)");
			}
		}
		return count.toString();
	}

    private void dumpTiming() {
        // write full timing details to debug log file, if specified
        PrintWriter writer = null;
        if (debugLogFile != null) try {
            writer = new PrintWriter(new FileWriter(debugLogFile));
            writer.println(DEBUG_LOG_HEADER);
            for (Entry<String, PROXITimingRecord> entry : batchManager.timingRecords.entrySet())
                writer.println(String.format(
                    "%s\t%s", entry.getKey(), entry.getValue().toString()));
        } catch (Throwable error) {
            System.err.println(String.format(
                "Could not write MassIVE search import timing details to debug log file [%s].",
                debugLogFile.getAbsolutePath()));
            error.printStackTrace();
        } finally {
            if (writer != null) try {
                writer.close();
            } catch (Throwable error) {}
        }
        // write timing summary to console
        StringBuilder message =
            new StringBuilder("MassIVE search import timing summary:\n----------");
        PROXITimingRecord record = batchManager.timingRecords.get("psm_row_preprocessing");
        if (record != null) {
            message.append("\nPSM row pre-processing:")
                .append("\t").append(CommonUtils.formatNanoseconds(record.total.getLeft()));
            message.append("\n  Rows:")
                .append("\t").append(String.format("%,d", record.count));
            message.append("\n  Slowest row:")
                .append("\t").append(CommonUtils.formatNanoseconds(record.max.getLeft()));
            message.append("\n  Fastest row:")
                .append("\t").append(CommonUtils.formatNanoseconds(record.min.getLeft()));
            ImmutablePair<Double, Double> average = record.getAverage();
            message.append("\n  Average row:")
                .append("\t").append(
                    CommonUtils.formatNanoseconds(Math.round(average.getLeft())));
        }
        record = batchManager.timingRecords.get("psm_row_protein_clean");
        if (record != null) {
            message.append("\nProtein string formatting:")
                .append("\t").append(CommonUtils.formatNanoseconds(record.total.getLeft()));
            message.append("\n  Proteins:")
                .append("\t").append(String.format("%,d", record.count));
            message.append("\n  Slowest protein:")
                .append("\t").append(CommonUtils.formatNanoseconds(record.max.getLeft()));
            message.append("\n  Fastest protein:")
                .append("\t").append(CommonUtils.formatNanoseconds(record.min.getLeft()));
            ImmutablePair<Double, Double> average = record.getAverage();
            message.append("\n  Average protein:")
                .append("\t").append(
                    CommonUtils.formatNanoseconds(Math.round(average.getLeft())));
        }
        record = batchManager.timingRecords.get("psm_row_cascade");
        if (record != null) {
            message.append("\nPSM row batch processing:")
                .append("\t").append(CommonUtils.formatNanoseconds(record.total.getLeft()));
            message.append("\n  PSM/protein pairs:")
                .append("\t").append(String.format("%,d", record.count));
            message.append("\n  Slowest PSM/protein pair:")
                .append("\t").append(CommonUtils.formatNanoseconds(record.max.getLeft()));
            message.append("\n  Fastest PSM/protein pair:")
                .append("\t").append(CommonUtils.formatNanoseconds(record.min.getLeft()));
            ImmutablePair<Double, Double> average = record.getAverage();
            message.append("\n  Average PSM/protein pair:")
                .append("\t").append(
                    CommonUtils.formatNanoseconds(Math.round(average.getLeft())));
        }
        record = batchManager.timingRecords.get("psm_row_trigger");
        if (record != null) {
            message.append("\nPSM row insert-all-batches operations:")
                .append("\t").append(CommonUtils.formatNanoseconds(record.total.getLeft()));
            message.append("\n  Triggered batch inserts:")
                .append("\t").append(String.format("%,d", record.count));
            message.append("\n  Slowest batch insert:")
                .append("\t").append(CommonUtils.formatNanoseconds(record.max.getLeft()));
            message.append("\n  Fastest batch insert:")
                .append("\t").append(CommonUtils.formatNanoseconds(record.min.getLeft()));
            ImmutablePair<Double, Double> average = record.getAverage();
            message.append("\n  Average batch insert:")
                .append("\t").append(
                    CommonUtils.formatNanoseconds(Math.round(average.getLeft())));
        }
        // organize database operation timing records
        long totalBatchTime = 0L;
        long totalInsertTime = 0L;
        long totalQueryTime = 0L;
        Map<String, Map<String, PROXITimingRecord>> operationRecords =
            new LinkedHashMap<String, Map<String, PROXITimingRecord>>();
        for (Entry<String, PROXITimingRecord> entry : batchManager.timingRecords.entrySet()) {
            String type = entry.getKey();
            String operation = null;
            if (type.endsWith("_batch") || type.endsWith("_query"))
                operation = type.substring(0, type.length() - 6);
            else if (type.endsWith("_insert"))
                operation = type.substring(0, type.length() - 7);
            else continue;
            Map<String, PROXITimingRecord> operationRecord = operationRecords.get(operation);
            if (operationRecord == null)
                operationRecord = new HashMap<String, PROXITimingRecord>(4);
            record = entry.getValue();
            if (type.endsWith("_batch")) {
                operationRecord.put("batch", record);
                totalBatchTime += record.total.getLeft();
            } else if (type.endsWith("_insert")) {
                operationRecord.put("insert", record);
                totalInsertTime += record.total.getLeft();
            } else if (type.endsWith("_query")) {
                operationRecord.put("query", record);
                totalQueryTime += record.total.getLeft();
            }
            operationRecords.put(operation, operationRecord);
        }
        // write timing summary for each database operation
        message.append("\nTotal batch setup time:\t").append(
            CommonUtils.formatNanoseconds(totalBatchTime));
        message.append("\nTotal batch insert time:\t").append(
            CommonUtils.formatNanoseconds(totalInsertTime));
        message.append("\nTotal follow-up query time:\t").append(
            CommonUtils.formatNanoseconds(totalQueryTime));
        for (Entry<String, Map<String, PROXITimingRecord>> entry : operationRecords.entrySet()) {
            message.append("\nMassIVE search table [").append(entry.getKey()).append("]:");
            Map<String, PROXITimingRecord> operationRecord = entry.getValue();
            record = operationRecord.get("batch");
            if (record != null) {
                message.append("\n  Batch setup:")
                    .append("\t").append(CommonUtils.formatNanoseconds(record.total.getLeft()));
                message.append("\n    Batches:")
                    .append("\t").append(String.format("%,d", record.count));
                message.append("\n    Inserts:")
                    .append("\t").append(String.format("%,d", record.total.getRight()));
                message.append("\n    Slowest batch:")
                    .append("\t").append(String.format("%,d", record.max.getRight()))
                    .append("\t").append(CommonUtils.formatNanoseconds(record.max.getLeft()));
                message.append("\n    Fastest batch:")
                    .append("\t").append(String.format("%,d", record.min.getRight()))
                    .append("\t").append(CommonUtils.formatNanoseconds(record.min.getLeft()));
                ImmutablePair<Double, Double> average = record.getAverage();
                message.append("\n    Average batch:")
                    .append("\t").append(String.format("%,.2f", average.getRight()))
                    .append("\t").append(
                        CommonUtils.formatNanoseconds(Math.round(average.getLeft())));
            }
            record = operationRecord.get("insert");
            if (record != null) {
                message.append("\n  Batch inserts:")
                    .append("\t").append(CommonUtils.formatNanoseconds(record.total.getLeft()));
                message.append("\n    Batches:")
                    .append("\t").append(String.format("%,d", record.count));
                message.append("\n    Inserts:")
                    .append("\t").append(String.format("%,d", record.total.getRight()));
                message.append("\n    Slowest batch:")
                    .append("\t").append(String.format("%,d", record.max.getRight()))
                    .append("\t").append(CommonUtils.formatNanoseconds(record.max.getLeft()));
                message.append("\n    Fastest batch:")
                    .append("\t").append(String.format("%,d", record.min.getRight()))
                    .append("\t").append(CommonUtils.formatNanoseconds(record.min.getLeft()));
                ImmutablePair<Double, Double> average = record.getAverage();
                message.append("\n    Average batch:")
                    .append("\t").append(String.format("%,.2f", average.getRight()))
                    .append("\t").append(
                        CommonUtils.formatNanoseconds(Math.round(average.getLeft())));
            }
            record = operationRecord.get("query");
            if (record != null) {
                message.append("\n  Follow-up queries:")
                    .append("\t").append(CommonUtils.formatNanoseconds(record.total.getLeft()));
                message.append("\n    Queries:")
                    .append("\t").append(String.format("%,d", record.count));
                message.append("\n    Query set values:")
                    .append("\t").append(String.format("%,d", record.total.getRight()));
                message.append("\n    Slowest query:")
                    .append("\t").append(String.format("%,d", record.max.getRight()))
                    .append("\t").append(CommonUtils.formatNanoseconds(record.max.getLeft()));
                message.append("\n    Fastest query:")
                    .append("\t").append(String.format("%,d", record.min.getRight()))
                    .append("\t").append(CommonUtils.formatNanoseconds(record.min.getLeft()));
                ImmutablePair<Double, Double> average = record.getAverage();
                message.append("\n    Average query:")
                    .append("\t").append(String.format("%,.2f", average.getRight()))
                    .append("\t").append(
                        CommonUtils.formatNanoseconds(Math.round(average.getLeft())));
            }
        }
        message.append("\n----------");
        System.out.println(message.toString());
    }
}
