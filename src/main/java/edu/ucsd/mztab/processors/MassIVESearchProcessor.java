package edu.ucsd.mztab.processors;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
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
import edu.ucsd.mztab.model.TimingRecord;
import edu.ucsd.mztab.model.MzTabMsRun;
import edu.ucsd.mztab.ui.MzTabPROXIImporter;
import edu.ucsd.mztab.util.CommonUtils;
import edu.ucsd.mztab.util.ProteomicsUtils;

public class MassIVESearchProcessor implements MzTabProcessor
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
    private static final String DEBUG_LOG_HEADER =
        "category\ttotal_batch_count\ttotal_element_count\ttotal_time_ns" +
        "\tslowest_batch_element_count\tslowest_batch_time_ns" +
        "\tfastest_batch_element_count\tfastest_batch_time_ns" +
        "\taverage_batch_element_count\taverage_batch_time_ns";
    // MySQL LOAD DATA input files
    private static final String TEMP_DIRECTORY_NAME = "temp";
    private static final int WRITE_BUFFER_SIZE = 8 * 1024; // 8 KiB

    /*========================================================================
     * Properties
     *========================================================================*/
    private Connection                        connection;
    private Map<String, Map<String, Integer>> uniqueElements;
    private Map<String, Map<String, Integer>> globalElements;
    private Map<String, Integer>              rowCounts;
    private Map<String, File>                 dataLoadFiles;
    private Set<String>                       psmIDs;
    private Long[]                            psmDatabaseIDs;
    private Long                              lastPSMDatabaseID;
    private MassIVESearchMzTabRecord          mzTabRecord;
    private MassIVESearchDataLoadManager      dataLoadManager;
    private MzTabSectionHeader                prtHeader;
    private MzTabSectionHeader                pepHeader;
    private MzTabSectionHeader                psmHeader;
    private boolean                           importByQValue;
    private Integer                           lastPSMIndex;
    private Integer                           validColumn;
    private Integer                           qValueColumn;
    private File                              tempDirectory;
    private File                              debugLogFile;
    private Long                              start;

    /*========================================================================
     * Constructor
     *========================================================================*/
    public MassIVESearchProcessor(
        String taskID, String datasetID, boolean importByQValue, Long startingPSMID,
        Map<String, Map<String, Integer>> globalElements, Connection connection
    ) {
        // validate database connection
        if (connection == null)
            throw new NullPointerException(
                "Argument database connection is null.");
        else this.connection = connection;
        // initialize mzTab file record
        mzTabRecord = new MassIVESearchMzTabRecord();
        // validate task ID
        if (taskID == null)
            throw new NullPointerException(
                "Argument ProteoSAFe task ID is null.");
        else mzTabRecord.taskID = taskID;
        // dataset ID can be null
        mzTabRecord.datasetID = datasetID;
        // initialize counter maps
        uniqueElements = new LinkedHashMap<String, Map<String, Integer>>(21);
        if (globalElements != null)
            this.globalElements = globalElements;
        rowCounts = new LinkedHashMap<String, Integer>(3);
        // initialize mzTab file parameters
        prtHeader = null;
        pepHeader = null;
        psmHeader = null;
        qValueColumn = null;
        this.importByQValue = importByQValue;
        // initialize load files
        tempDirectory = new File(TEMP_DIRECTORY_NAME);
        tempDirectory.mkdirs();
        if (tempDirectory.isDirectory() == false || tempDirectory.canWrite() == false)
            throw new RuntimeException(String.format(
                "Could not create temp directory [%s].", tempDirectory.getAbsolutePath()));
        dataLoadFiles = new LinkedHashMap<String, File>(21);
        // level 1 - no foreign keys
        dataLoadFiles.put("resultfiles", new File(tempDirectory, "spectrumfiles.tsv"));
        dataLoadFiles.put("peptides", new File(tempDirectory, "peptides.tsv"));
        dataLoadFiles.put("proteins", new File(tempDirectory, "proteins.tsv"));
        dataLoadFiles.put("modifications", new File(tempDirectory, "modifications.tsv"));
        // level 2 - foreign keys to level 1
        dataLoadFiles.put("variants", new File(tempDirectory, "variants.tsv"));
        dataLoadFiles.put("resultfile_peptides", new File(tempDirectory, "resultfile_peptides.tsv"));
        dataLoadFiles.put("resultfile_proteins", new File(tempDirectory, "resultfile_proteins.tsv"));
        dataLoadFiles.put("resultfile_modifications",
            new File(tempDirectory, "resultfile_modifications.tsv"));
        dataLoadFiles.put("dataset_peptides", new File(tempDirectory, "dataset_peptides.tsv"));
        dataLoadFiles.put("dataset_proteins", new File(tempDirectory, "dataset_proteins.tsv"));
        dataLoadFiles.put("dataset_modifications",
            new File(tempDirectory, "dataset_modifications.tsv"));
        dataLoadFiles.put("peptide_proteins", new File(tempDirectory, "peptide_proteins.tsv"));
        dataLoadFiles.put("peptide_modifications",
            new File(tempDirectory, "peptide_modifications.tsv"));
        dataLoadFiles.put("protein_modifications",
            new File(tempDirectory, "protein_modifications.tsv"));
        // level 3 - foreign keys to level 2
        dataLoadFiles.put("psms", new File(tempDirectory, "psms.tsv"));
        dataLoadFiles.put("resultfile_variants", new File(tempDirectory, "resultfile_variants.tsv"));
        dataLoadFiles.put("dataset_variants", new File(tempDirectory, "dataset_variants.tsv"));
        dataLoadFiles.put("variant_proteins", new File(tempDirectory, "variant_proteins.tsv"));
        dataLoadFiles.put("variant_modifications",
            new File(tempDirectory, "variant_modifications.tsv"));
        // level 4 - foreign keys to level 3
        dataLoadFiles.put("psm_proteins", new File(tempDirectory, "psm_proteins.tsv"));
        dataLoadFiles.put("psm_modifications", new File(tempDirectory, "psm_modifications.tsv"));
        // initialize PSM ID state
        if (startingPSMID != null) {
            if (startingPSMID < 1)
                throw new IllegalArgumentException(String.format(
                    "Starting PSM database ID [%d] cannot be less than 1.", startingPSMID));
            else lastPSMDatabaseID = startingPSMID;
        } else lastPSMDatabaseID = 1L;
        psmIDs = null;
        psmDatabaseIDs = null;
        lastPSMIndex = null;
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
        dataLoadManager = new MassIVESearchDataLoadManager(mzTabRecord, connection);
        // record all of this mzTab file's referenced spectrum files
        for (Integer msRun : mzTabRecord.mzTabFile.getMsRuns().keySet())
            processSpectrumFile(mzTabRecord.mzTabFile.getMsRun(msRun).getDescriptor());
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
//          incrementRowCount("PRT");
//          if (prtHeader == null)
//              throw new IllegalArgumentException(String.format(
//                  "Line %d of mzTab file [%s] is invalid:" +
//                  "\n----------\n%s\n----------\n" +
//                  "A \"PRT\" row was found before any \"PRH\" row.",
//                  lineNumber, mzTabFilename, line));
//          else prtHeader.validateMzTabRow(line);
//          // extract insertable elements from this PRT row
//          String[] columns = line.split("\t");
//          // record this protein
//          Collection<Modification> modifications =
//              ProteomicsUtils.getModifications(
//                  columns[prtHeader.getColumnIndex("modifications")]);
//          try {
//              cascadeProtein(
//                  columns[prtHeader.getColumnIndex("accession")],
//                  modifications);
//              connection.commit();
//          } catch (Throwable error) {
//              try { connection.rollback(); } catch (Throwable innerError) {}
//              // log this insertion failure
//              incrementRowCount("invalid_PRT");
//              // print warning and continue
//              System.err.println(String.format(
//                  "Line %d of mzTab file [%s] is invalid:" +
//                  "\n----------\n%s\n----------\n%s",
//                  lineNumber, mzTabFilename, line,
//                  getRootCause(error).getMessage()));
//              //error.printStackTrace();
//          }
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
//      } else if (line.startsWith("PEP")) {
//          incrementRowCount("PEP");
//          if (pepHeader == null)
//              throw new IllegalArgumentException(String.format(
//                  "Line %d of mzTab file [%s] is invalid:" +
//                  "\n----------\n%s\n----------\n" +
//                  "A \"PEP\" row was found before any \"PEH\" row.",
//                  lineNumber, mzTabFilename, line));
//          else pepHeader.validateMzTabRow(line);
//          // extract insertable elements from this PEP row
//          String[] columns = line.split("\t");
//          // record this peptide
//          Collection<Modification> modifications =
//              ProteomicsUtils.getModifications(
//                  columns[pepHeader.getColumnIndex("modifications")]);
//          try {
//              cascadePeptide(
//                  columns[pepHeader.getColumnIndex("sequence")],
//                  columns[pepHeader.getColumnIndex("accession")],
//                  modifications);
//              connection.commit();
//          } catch (Throwable error) {
//              try { connection.rollback(); } catch (Throwable innerError) {}
//              // log this insertion failure
//              incrementRowCount("invalid_PEP");
//              // print warning and continue
//              System.err.println(String.format(
//                  "Line %d of mzTab file [%s] is invalid:\n" +
//                  "----------\n%s\n----------\n%s",
//                  lineNumber, mzTabFilename, line,
//                  getRootCause(error).getMessage()));
//              //error.printStackTrace();
//          }
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
//
System.out.println("PSM row:");
//
            long start = System.nanoTime();
            long checkpoint = start;
            long minorCheckpoint = checkpoint;
            incrementRowCount("PSM");
            long end = System.nanoTime();
//
System.out.println(String.format("  psm_row_increment_row_count\t%s",
    CommonUtils.formatNanoseconds(end - minorCheckpoint)));
//
            dataLoadManager.addTiming("psm_row_increment_row_count", end - minorCheckpoint, 1);
            minorCheckpoint = end;
            if (psmHeader == null)
                throw new IllegalArgumentException(String.format(
                    "Line %d of mzTab file [%s] is invalid:" +
                    "\n----------\n%s\n----------\n" +
                    "A \"PSM\" row was found before any \"PSH\" row.",
                    lineNumber, mzTabFilename, line));
            else psmHeader.validateMzTabRow(line);
            end = System.nanoTime();
//
System.out.println(String.format("  psm_row_validate\t%s",
    CommonUtils.formatNanoseconds(end - minorCheckpoint)));
//
            dataLoadManager.addTiming("psm_row_validate", end - minorCheckpoint, 1);
            minorCheckpoint = end;
            // extract insertable elements from this PSM row
            String[] columns = line.split("\t");
            // get this PSM's index
            String psmID = columns[psmHeader.getColumnIndex("PSM_ID")];
            Integer psmIndex = null;
            // if the PSM_ID set is initialized, then we've already determined that PSM_ID
            // values differ from 1-based index, so just use the set
            if (psmIDs != null) {
                if (psmIDs.contains(psmID) == false) {
                    lastPSMIndex++;
                    psmIndex = lastPSMIndex;
                    psmIDs.add(psmID);
                }
            }
            // otherwise assume that PSM_ID and index are the same;
            // i.e. consecutive integers starting at 1
            else try {
                int psmIDValue = Integer.parseInt(psmID);
                // handle first PSM row
                if (lastPSMIndex == null) {
                    // expected case - first PSM_ID is 1
                    if (psmIDValue == 1)
                        lastPSMIndex = psmIDValue;
                    // abnormal case - first PSM_ID is an int, but not 1
                    else throw new NumberFormatException();
                }
                // handle subsequent PSM rows
                else {
                    // expected case - this PSM_ID has been seen before
                    if (psmIDValue <= lastPSMIndex) {
                        // do nothing
                    }
                    // expected case - this PSM_ID is the next consecutive index
                    else if (psmIDValue == (lastPSMIndex + 1))
                        lastPSMIndex = psmIDValue;
                    // abnormal case - this PSM_ID is an int, but not the next consecutive index
                    else throw new NumberFormatException();
                }
                psmIndex = psmIDValue;
            }
            // only if this row's PSM_ID differs from its 1-based index do we
            // need to worry about tracking all PSM_ID values seen so far
            catch (NumberFormatException error) {
                // initialize PSM_ID set and add this PSM_ID
                psmIDs = new TreeSet<String>();
                psmIDs.add(psmID);
                // handle first PSM row
                if (lastPSMIndex == null)
                    lastPSMIndex = 1;
                // handle subsequent PSM rows
                else {
                    // add all PSM_ID values seen so far
                    for (int i=1; i<=lastPSMIndex; i++)
                        psmIDs.add(Integer.toString(i));
                    // increment index for this new PSM_ID
                    lastPSMIndex++;
                }
                psmIndex = lastPSMIndex;
            }
            end = System.nanoTime();
//
System.out.println(String.format("  psm_row_check_index (%d)\t%s",
    psmIndex, CommonUtils.formatNanoseconds(end - minorCheckpoint)));
//
            dataLoadManager.addTiming("psm_row_check_index", end - minorCheckpoint, 1);
            minorCheckpoint = end;
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
            end = System.nanoTime();
//
System.out.println(String.format("  psm_row_check_importable\t%s",
    CommonUtils.formatNanoseconds(end - minorCheckpoint)));
//
            dataLoadManager.addTiming("psm_row_check_importable", end - minorCheckpoint, 1);
            minorCheckpoint = end;
            // instantiate and validate the PSM
            Collection<Modification> modifications = null;
            PSM psm = null;
            if (importable) try {
                modifications = cleanModificationsForSearch(
                    ProteomicsUtils.getModifications(
                        columns[psmHeader.getColumnIndex("modifications")]));
                end = System.nanoTime();
//
System.out.println(String.format("  psm_row_clean_mods\t%s",
    CommonUtils.formatNanoseconds(end - minorCheckpoint)));
//
                dataLoadManager.addTiming("psm_row_clean_mods", end - minorCheckpoint, 1);
                minorCheckpoint = end;
                psm = new PSM(
                    psmID, psmIndex,
                    columns[psmHeader.getColumnIndex("spectra_ref")],
                    columns[psmHeader.getColumnIndex("sequence")],
                    columns[psmHeader.getColumnIndex("charge")],
                    columns[psmHeader.getColumnIndex("exp_mass_to_charge")],
                    modifications
                );
                end = System.nanoTime();
//
System.out.println(String.format("  psm_row_instantiate_psm\t%s",
    CommonUtils.formatNanoseconds(end - minorCheckpoint)));
//
                dataLoadManager.addTiming("psm_row_instantiate_psm", end - minorCheckpoint, 1);
                minorCheckpoint = end;
                // if this PSM doesn't pass basic validation, do not import
                if (MzTabPROXIImporter.isImportable(psm) == false)
                    importable = false;
                end = System.nanoTime();
//
System.out.println(String.format("  psm_row_check_importable_2\t%s",
    CommonUtils.formatNanoseconds(end - minorCheckpoint)));
//
                dataLoadManager.addTiming("psm_row_check_importable_2", end - minorCheckpoint, 1);
                minorCheckpoint = end;
            } catch (Throwable error) {
                importable = false;
            }
            end = System.nanoTime();
//
System.out.println(String.format("  psm_row_preprocessing\t%s",
    CommonUtils.formatNanoseconds(end - checkpoint)));
//
            dataLoadManager.addTiming("psm_row_preprocessing", end - checkpoint, 1);
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
//
System.out.println(String.format("  psm_row_protein_clean\t%s",
    CommonUtils.formatNanoseconds(end - checkpoint)));
//
                    dataLoadManager.addTiming("psm_row_protein_clean", end - checkpoint, 1);
                    checkpoint = end;
                    // process this PSM into the current batch
                    cascadePSM(psm, cleanedAccession, modifications);
                    end = System.nanoTime();
//
System.out.println(String.format("  psm_row_cascade\t%s",
    CommonUtils.formatNanoseconds(end - checkpoint)));
//
                    dataLoadManager.addTiming("psm_row_cascade", end - checkpoint, 1);
                    checkpoint = end;
                }
            } else incrementRowCount("unimportable_PSM");
        }
        return line;
    }

    public void tearDown() {
        // allocate PSM database IDs array
        if (lastPSMIndex == null)
            throw new IllegalStateException(
                "mzTab file is done being read, yet lastPSMIndex is still null.");
        psmDatabaseIDs = new Long[lastPSMIndex + 1];
        // build and submit all data
        try {
            loadData();
        } catch (RuntimeException error) {
            throw error;
        } catch (Throwable error) {
            throw new RuntimeException(error);
        }
        // report import results
        StringBuilder success = new StringBuilder("Imported file [");
        success.append(mzTabRecord.mzTabFile.getMzTabFilename());
        success.append("] (");
        success.append(
            CommonUtils.formatBytes(mzTabRecord.mzTabFile.getFile().length()));
        success.append(", ");
        int lines = getRowCount("lines_in_file");
        success.append(String.format("%,d", lines)).append(" ");
        success.append(CommonUtils.pluralize("line", lines));
        success.append(")");
        double seconds = 0.0;
        if (start != null) {
            long elapsed = System.nanoTime() - start;
            seconds = elapsed / 1000.0;
            success.append(" in ");
            success.append(CommonUtils.formatNanoseconds(elapsed));
            success.append(" (");
            success.append(String.format("%.2f", lines / seconds));
            success.append(" lines/second)");
        }
        success.append(".");
        success.append("\n\tPSMs:     ");
        success.append(formatRowCount(getImportedPSMCount(),
            getRowCount("PSM"), getRowCount("invalid_PSM"),
            getRowCount("unimportable_PSM"), seconds));
        success.append("\n\tPeptides: ");
        success.append(formatRowCount(getElementCount("sequence"),
            getRowCount("PEP"), getRowCount("invalid_PEP"),
            getRowCount("unimportable_PEP"), seconds));
        success.append("\n\tVariants: ").append(String.format("%,d", getElementCount("variant")));
        success.append("\n\tProteins: ");
        success.append(formatRowCount(getElementCount("accession"),
            getRowCount("PRT"), getRowCount("invalid_PRT"),
            getRowCount("unimportable_PRT"), seconds));
        success.append("\n\tPTMs:     ")
            .append(String.format("%,d", getElementCount("modification")));
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

    public int getImportedPSMCount() {
        int importedPSMs = 0;
        for (Long id : psmDatabaseIDs)
            if (id != null)
                importedPSMs++;
        return importedPSMs;
    }

    public long getLastPSMDatabaseID() {
        return lastPSMDatabaseID;
    }

    public void setDebugLogFile(File debugLogFile) {
        this.debugLogFile = debugLogFile;
    }

    /*========================================================================
     * Convenience classes
     *========================================================================*/
    private static class MassIVESearchMzTabRecord {
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
        public MassIVESearchMzTabRecord() {
            // initialize properties
            mzTabFile = null;
            id = null;
            taskID = null;
            datasetID = null;
        }
    }

    private static class MassIVESearchDataLoadManager {
        /*====================================================================
         * Properties
         *====================================================================*/
        // top-level tables
        // spectrumfiles : file_descriptor -> id
        private Map<String, Integer>                         spectrumFiles;
        // peptides : sequence -> id
        private Map<String, Integer>                         peptides;
        // proteins : accession -> id
        private Map<String, Integer>                         proteins;
        // modifications : name -> id
        private Map<String, Integer>                         modifications;
        // modification masses : name -> mass
        private Map<String, Double>                          modificationMasses;
        // variants : sequence/charge -> id
        private Map<ImmutablePair<String, Integer>, Integer> variants;
        // variant peptides : variant sequence/charge -> peptide sequence
        private Map<ImmutablePair<String, Integer>, String>  variantPeptides;
        // psms : PSM -> spectrumfile/peptide/variant
        private Map<PSM, ImmutableTriple<String, String, ImmutablePair<String, Integer>>>
                                                             psms;

        // join tables - one key fixed
        // resultfile_peptides : peptide sequence
        private Collection<String>                           resultFilePeptides;
        // resultfile_proteins : protein accession
        private Collection<String>                           resultFileProteins;
        // resultfile_modifications : modification name
        private Collection<String>                           resultFileModifications;
        // resultfile_variants : variant sequence/charge
        private Collection<ImmutablePair<String, Integer>>   resultFileVariants;
        // dataset_peptides : peptide sequence
        private Collection<String>                           datasetPeptides;
        // dataset_proteins : protein accession
        private Collection<String>                           datasetProteins;
        // dataset_modifications : modification name
        private Collection<String>                           datasetModifications;
        // dataset_variants : variant sequence/charge
        private Collection<ImmutablePair<String, Integer>>   datasetVariants;

        // join tables - two keys
        // peptide_proteins : peptide sequence -> protein accession
        private Collection<ImmutablePair<String, String>>    peptideProteins;
        // peptide_modifications : peptide sequence -> modification name
        private Collection<ImmutablePair<String, String>>    peptideModifications;
        // protein_modifications : protein accession -> modification name
        private Collection<ImmutablePair<String, String>>    proteinModifications;
        // variant_proteins : variant sequence/charge -> protein accession
        private Collection<ImmutablePair<ImmutablePair<String, Integer>, String>>
                                                             variantProteins;
        // variant_modifications : variant sequence/charge -> modification name/position
        private Collection<ImmutablePair<
            ImmutablePair<String, Integer>, ImmutablePair<String, Integer>>>
                                                             variantModifications;
        // psm_proteins : PSM -> protein accession
        private Collection<ImmutablePair<PSM, String>>       psmProteins;
        // psm_modifications : PSM -> modification name
        private Collection<ImmutablePair<PSM, String>>       psmModifications;

        // data load properties
        private boolean                                      isDatasetResult;
        private Map<String, MassIVESearchTimingRecord>       timingRecords;

        /*====================================================================
         * Constructor
         *====================================================================*/
        public MassIVESearchDataLoadManager(
            MassIVESearchMzTabRecord mzTabRecord, Connection connection
        ) {
            if (mzTabRecord == null)
                throw new NullPointerException("mzTab record cannot be null.");
            else if (mzTabRecord.id == null)
                throw new NullPointerException("mzTab record resultfile ID cannot be null.");
            else if (connection == null)
                throw new NullPointerException("Connection cannot be null.");
            // initialize data load state
            isDatasetResult = mzTabRecord.datasetID != null;
            timingRecords = new LinkedHashMap<String, MassIVESearchTimingRecord>();
            clear();
        }

        /*====================================================================
         * Public interface methods
         *====================================================================*/
        public void clear() {
            // initialize all new element collections
            // top-level tables
            spectrumFiles = new TreeMap<String, Integer>();
            peptides = new TreeMap<String, Integer>();
            proteins = new TreeMap<String, Integer>();
            modifications = new TreeMap<String, Integer>();
            modificationMasses = new TreeMap<String, Double>();
            variants = new TreeMap<ImmutablePair<String, Integer>, Integer>();
            variantPeptides = new TreeMap<ImmutablePair<String, Integer>, String>();
            psms =
                new TreeMap<PSM, ImmutableTriple<String, String, ImmutablePair<String, Integer>>>();
            // join tables - one key fixed
            resultFilePeptides = new ArrayList<String>();
            resultFileProteins = new ArrayList<String>();
            resultFileModifications = new ArrayList<String>();
            resultFileVariants = new ArrayList<ImmutablePair<String, Integer>>();
            datasetPeptides = new ArrayList<String>();
            datasetProteins = new ArrayList<String>();
            datasetModifications = new ArrayList<String>();
            datasetVariants = new ArrayList<ImmutablePair<String, Integer>>();
            // join tables - two keys
            peptideProteins = new ArrayList<ImmutablePair<String, String>>();
            peptideModifications = new ArrayList<ImmutablePair<String, String>>();
            proteinModifications = new ArrayList<ImmutablePair<String, String>>();
            variantProteins = new ArrayList<ImmutablePair<ImmutablePair<String, Integer>, String>>();
            variantModifications = new ArrayList<ImmutablePair<
                ImmutablePair<String, Integer>, ImmutablePair<String, Integer>>>();
            psmProteins = new ArrayList<ImmutablePair<PSM, String>>();
            psmModifications = new ArrayList<ImmutablePair<PSM, String>>();
        }

        // level 1 - no foreign keys
        public void processSpectrumFile(String fileDescriptor, Integer id) {
            if (fileDescriptor == null)
                throw new NullPointerException("File descriptor cannot be null.");
            // if this spectrum file has already been processed in this batch, skip
            else if (spectrumFiles.containsKey(fileDescriptor))
                return;
            // update the proper batch state ID map with this spectrum file
            spectrumFiles.put(fileDescriptor, id);
        }

        public void processPeptide(String sequence, Integer id) {
            if (sequence == null)
                throw new NullPointerException("Peptide sequence cannot be null.");
            // if this peptide has already been processed in this batch, skip
            else if (peptides.containsKey(sequence))
                return;
            // add this peptide to the proper batch state ID map
            peptides.put(sequence, id);
        }

        public void processProtein(String accession, Integer id) {
            if (accession == null)
                throw new NullPointerException("Protein accession cannot be null.");
            // if this protein has already been processed in this batch, skip
            else if (proteins.containsKey(accession))
                return;
            // add this protein to the proper batch state ID map
            proteins.put(accession, id);
        }

        public void processModification(Modification modification, Integer id) {
            if (modification == null)
                throw new NullPointerException("Modification cannot be null.");
            // if this modification has already been processed in this batch, skip
            String name = modification.getName();
            if (modifications.containsKey(name))
                return;
            // add this modification to the proper batch state ID maps
            modifications.put(name, id);
            modificationMasses.put(name, modification.getMass());
        }

        // level 2 - foreign keys to level 1
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
            // add this variant to the proper batch state ID maps
            variants.put(variant, id);
            variantPeptides.put(variant, peptideSequence);
        }

        public void processResultFilePeptide(String sequence) {
            if (sequence == null)
                throw new NullPointerException("Peptide sequence cannot be null.");
            // add this peptide to the proper batch state join set
            resultFilePeptides.add(sequence);
        }

        public void processResultFileProtein(String accession) {
            if (accession == null)
                throw new NullPointerException("Protein accession cannot be null.");
            // add this protein to the proper batch state join set
            resultFileProteins.add(accession);
        }

        public void processResultFileModification(Modification modification) {
            if (modification == null)
                throw new NullPointerException("Modification cannot be null.");
            // add this modification to the proper batch state join set
            resultFileModifications.add(modification.getName());
        }

        public void processDatasetPeptide(String sequence) {
            if (sequence == null)
                throw new NullPointerException("Peptide sequence cannot be null.");
            // if this is not a dataset result, do nothing
            else if (isDatasetResult == false)
                return;
            // add this peptide to the proper batch state join set
            datasetPeptides.add(sequence);
        }

        public void processDatasetProtein(String accession) {
            if (accession == null)
                throw new NullPointerException("Protein accession cannot be null.");
            // if this is not a dataset result, do nothing
            else if (isDatasetResult == false)
                return;
            // add this protein to the proper batch state join set
            datasetProteins.add(accession);
        }

        public void processDatasetModification(Modification modification) {
            if (modification == null)
                throw new NullPointerException("Modification cannot be null.");
            // if this is not a dataset result, do nothing
            else if (isDatasetResult == false)
                return;
            // add this modification to the proper batch state join set
            datasetModifications.add(modification.getName());
        }

        public void processPeptideProtein(String sequence, String accession) {
            if (sequence == null)
                throw new NullPointerException("Peptide sequence cannot be null.");
            else if (accession == null)
                throw new NullPointerException("Protein accession cannot be null.");
            // add this peptide/protein pair to the proper batch state join map
            peptideProteins.add(new ImmutablePair<String, String>(sequence, accession));
        }

        public void processPeptideModification(String sequence, Modification modification) {
            if (sequence == null)
                throw new NullPointerException("Peptide sequence cannot be null.");
            else if (modification == null)
                throw new NullPointerException("Modification cannot be null.");
            // add this peptide/modification pair to the proper batch state join map
            peptideModifications.add(
                new ImmutablePair<String, String>(sequence, modification.getName()));
        }

        public void processProteinModification(String accession, Modification modification) {
            if (accession == null)
                throw new NullPointerException("Protein accession cannot be null.");
            else if (modification == null)
                throw new NullPointerException("Modification cannot be null.");
            // add this protein/modification pair to the proper batch state join map
            proteinModifications.add(
                new ImmutablePair<String, String>(accession, modification.getName()));
        }

        // level 3 - foreign keys to level 2
        public void processPSM(
            PSM psm, String fileDescriptor, String peptideSequence,
            ImmutablePair<String, Integer> variant
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
            // add this PSM to the proper batch state ID maps
            psms.put(psm, new ImmutableTriple<String, String, ImmutablePair<String, Integer>>(
                 fileDescriptor, peptideSequence, variant));
        }

        public void processResultFileVariant(ImmutablePair<String, Integer> variant) {
            if (variant == null)
                throw new NullPointerException("Variant cannot be null.");
            // add this variant to the proper batch state join set
            resultFileVariants.add(variant);
        }

        public void processDatasetVariant(ImmutablePair<String, Integer> variant) {
            if (variant == null)
                throw new NullPointerException("Variant cannot be null.");
            // if this is not a dataset result, do nothing
            else if (isDatasetResult == false)
                return;
            // add this variant to the proper batch state join set
            datasetVariants.add(variant);
        }

        public void processVariantProtein(
            ImmutablePair<String, Integer> variant, String accession
        ) {
            if (variant == null)
                throw new NullPointerException("Variant cannot be null.");
            else if (variant.getLeft() == null)
                throw new NullPointerException("Variant sequence cannot be null.");
            else if (variant.getRight() == null)
                throw new NullPointerException("Variant charge cannot be null.");
            else if (accession == null)
                throw new NullPointerException("Protein accession cannot be null.");
            // add this variant/modification pair to the proper batch state join map
            variantProteins.add(
                new ImmutablePair<ImmutablePair<String, Integer>, String>(variant, accession));
        }

        public void processVariantModification(
            ImmutablePair<String, Integer> variant, Modification modification
        ) {
            if (variant == null)
                throw new NullPointerException("Variant cannot be null.");
            else if (variant.getLeft() == null)
                throw new NullPointerException("Variant sequence cannot be null.");
            else if (variant.getRight() == null)
                throw new NullPointerException("Variant charge cannot be null.");
            else if (modification == null)
                throw new NullPointerException("Modification cannot be null.");
            // if the location is not known, then this variant/modification join cannot be inserted
            Integer position = modification.getPosition();
            if (position == null)
                return;
            // add this variant/modification pair to the proper batch state join map
            variantModifications.add(
                new ImmutablePair<ImmutablePair<String, Integer>, ImmutablePair<String, Integer>>(
                    variant, new ImmutablePair<String, Integer>(modification.getName(), position)));
        }

        // level 3 - foreign keys to level 2
        public void processPSMProtein(PSM psm, String accession) {
            if (psm == null)
                throw new NullPointerException("PSM cannot be null.");
            else if (accession == null)
                throw new NullPointerException("Protein accession cannot be null.");
            // add this PSM/protein pair to the proper batch state join map
            psmProteins.add(new ImmutablePair<PSM, String>(psm, accession));
        }

        public void processPSMModification(PSM psm, Modification modification) {
            if (psm == null)
                throw new NullPointerException("PSM cannot be null.");
            else if (modification == null)
                throw new NullPointerException("Modification cannot be null.");
            // add this PSM/modification pair to the proper batch state join map
            psmModifications.add(new ImmutablePair<PSM, String>(psm, modification.getName()));
        }

        /*====================================================================
         * Convenience methods
         *====================================================================*/
        private void addTiming(String type, long time, int batchSize) {
            if (type == null)
                return;
            MassIVESearchTimingRecord timingRecord = timingRecords.get(type);
            if (timingRecord == null)
                timingRecord = new MassIVESearchTimingRecord();
            timingRecord.add(time, batchSize);
            timingRecords.put(type, timingRecord);
        }
    }

    @SuppressWarnings("unused")
    private static class MassIVESearchTimingRecord extends TimingRecord {
        /*====================================================================
         * Properties
         *====================================================================*/
        private Integer totalBatchSize;
        private Integer slowestBatchSize;
        private Integer fastestBatchSize;

        /*====================================================================
         * Constructor
         *====================================================================*/
        public MassIVESearchTimingRecord() {
            super();
            totalBatchSize = null;
            slowestBatchSize = null;
            fastestBatchSize = null;
        }

        /*====================================================================
         * Property accessor methods
         *====================================================================*/
        public Integer getTotalBatchSize() {
            return totalBatchSize;
        }

        public Integer getSlowestBatchSize() {
            return slowestBatchSize;
        }

        public Integer getFastestBatchSize() {
            return fastestBatchSize;
        }

        /*====================================================================
         * Public interface methods
         *====================================================================*/
        public void add(long elapsed, int batchSize) {
            // compare this timing to current max and min, set batch size accordingly
            if (totalBatchSize == null)
                totalBatchSize = batchSize;
            else totalBatchSize += batchSize;
            Long maxTime = getMax();
            if (slowestBatchSize == null || maxTime == null || maxTime < elapsed)
                slowestBatchSize = batchSize;
            Long minTime = getMin();
            if (fastestBatchSize == null || minTime == null || minTime > elapsed)
                fastestBatchSize = batchSize;
            // set timing
            super.add(elapsed);
        }

        public ImmutablePair<Double, Double> getCombinedAverage() {
            Integer count = getCount();
            if (count < 1 || getTotal() == null || totalBatchSize == null)
                return null;
            else return new ImmutablePair<Double, Double>(
                super.getAverage(), (double)totalBatchSize / count);
        }

        @Override
        public String toString() {
            StringBuilder record = new StringBuilder();
            record.append(getCount());
            if (totalBatchSize != null)
                record.append("\t").append(totalBatchSize);
            Long totalTime = getTotal();
            if (totalTime != null)
                record.append("\t").append(totalTime);
            Integer count = getCount();
            if (count > 1) {
                record.append("\t").append(slowestBatchSize).append("\t").append(getMax());
                record.append("\t").append(fastestBatchSize).append("\t").append(getMin());
                ImmutablePair<Double, Double> average = getCombinedAverage();
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

    // level 1 - no foreign keys
    private void processSpectrumFile(String fileDescriptor) {
        if (fileDescriptor == null)
            return;
        // process this spectrum file into the current batch
        dataLoadManager.processSpectrumFile(
            fileDescriptor, getElementID("spectrumFile", fileDescriptor));
    }

    private void processPeptide(String sequence) {
        if (sequence == null)
            return;
        // process this peptide into the current batch
        dataLoadManager.processPeptide(sequence, getElementID("sequence", sequence));
        // process this resultfile/peptide join into the current batch
        dataLoadManager.processResultFilePeptide(sequence);
        // if this is a dataset result, process this dataset/peptide join into the current batch
        if (dataLoadManager.isDatasetResult)
            dataLoadManager.processDatasetPeptide(sequence);
    }

    private void processProtein(String accession) {
        if (accession == null)
            return;
        // process this protein into the current batch
        dataLoadManager.processProtein(accession, getElementID("accession", accession));
        // process this resultfile/protein join into the current batch
        dataLoadManager.processResultFileProtein(accession);
        // if this is a dataset result, process this dataset/protein join into the current batch
        if (dataLoadManager.isDatasetResult)
            dataLoadManager.processDatasetProtein(accession);
    }

    private void processModification(Modification modification) {
        if (modification == null)
            return;
        // process this modification into the current batch
        dataLoadManager.processModification(
            modification, getElementID("modification", modification.getName()));
        // process this resultfile/modification join into the current batch
        dataLoadManager.processResultFileModification(modification);
        // if this is a dataset result, process this dataset/modification join into the current batch
        if (dataLoadManager.isDatasetResult)
            dataLoadManager.processDatasetModification(modification);
    }

    // level 2 - foreign keys to level 1
    private void processVariant(ImmutablePair<String, Integer> variant, String peptideSequence) {
        if (variant == null || peptideSequence == null)
            return;
        // process this variant into the current batch
        String variantID = String.format("%s_%d", variant.getLeft(), variant.getRight());
        dataLoadManager.processVariant(variant, peptideSequence, getElementID("variant", variantID));
        // process this resultfile/variant join into the current batch
        dataLoadManager.processResultFileVariant(variant);
        // if this is a dataset result, process this dataset/variant join into the current batch
        if (dataLoadManager.isDatasetResult)
            dataLoadManager.processDatasetVariant(variant);
    }

    private void processPeptideProtein(String sequence, String accession) {
        if (sequence == null || accession == null)
            return;
        // process this peptide/protein join into the current batch
        dataLoadManager.processPeptideProtein(sequence, accession);
    }

    private void processPeptideModification(String sequence, Modification modification) {
        if (sequence == null || modification == null)
            return;
        // process this peptide/modification join into the current batch
        dataLoadManager.processPeptideModification(sequence, modification);
    }

    private void processProteinModification(String accession, Modification modification) {
        if (accession == null || modification == null)
            return;
        // process this protein/modification join into the current batch
        dataLoadManager.processProteinModification(accession, modification);
    }

    // level 3 - foreign keys to level 2
    private void processPSM(
        PSM psm, String spectrumFileDescriptor, String peptideSequence,
        ImmutablePair<String, Integer> variant
    ) {
        if (psm == null || spectrumFileDescriptor == null || peptideSequence == null ||
            variant == null)
            return;
        // process this PSM into the current batch
        dataLoadManager.processPSM(psm, spectrumFileDescriptor, peptideSequence, variant);
    }

    private void processVariantProtein(ImmutablePair<String, Integer> variant, String accession) {
        if (variant == null || accession == null)
            return;
        // process this variant/protein join into the current batch
        dataLoadManager.processVariantProtein(variant, accession);
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
        dataLoadManager.processVariantModification(variant, modification);
    }

    // level 3 - foreign keys to level 2
    private void processPSMProtein(PSM psm, String accession) {
        if (psm == null || accession == null)
            return;
        // process this PSM/protein join into the current batch
        dataLoadManager.processPSMProtein(psm, accession);
    }

    private void processPSMModification(PSM psm, Modification modification) {
        if (psm == null || modification == null)
            return;
        // process this PSM/modification join into the current batch
        dataLoadManager.processPSMModification(psm, modification);
    }

    private void loadData() throws SQLException {
        // build and submit level 1 load files
        loadSpectrumFiles();
        loadPeptides();
        loadProteins();
        loadModifications();
        // build and submit level 2 load files
        loadVariants();
        loadResultFilePeptides();
        loadResultFileProteins();
        loadResultFileModifications();
        if (dataLoadManager.isDatasetResult) {
            loadDatasetPeptides();
            loadDatasetProteins();
            loadDatasetModifications();
        }
        loadPeptideProteins();
        loadPeptideModifications();
        loadProteinModifications();
        // build and submit level 3 load files
        loadPSMs();
        loadResultFileVariants();
        if (dataLoadManager.isDatasetResult)
            loadDatasetVariants();
        loadVariantProteins();
        loadVariantModifications();
        // build and submit level 4 load files
        loadPSMProteins();
        loadPSMModifications();
    }

    // level 1 - no foreign keys
    private void loadSpectrumFiles() throws SQLException {
        long start = System.nanoTime();
        // get file descriptors for all new spectrum files
        List<String> values = new ArrayList<String>(dataLoadManager.spectrumFiles.size());
        for (Entry<String, Integer> entry : dataLoadManager.spectrumFiles.entrySet())
            if (entry.getValue() == null)
                values.add(entry.getKey());
        // if there were any new spectrum files, insert all
        if (values.isEmpty() == false) {
            // build spectrumfiles load file
            long checkpoint = System.nanoTime();
            File loadFile = new File(tempDirectory, "spectrumfiles.tsv");
            PrintWriter writer = null;
            int loadRows = 0;
            try {
                writer = new PrintWriter(
                    new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(loadFile)),
                        WRITE_BUFFER_SIZE)
                );
                for (String fileDescriptor : values) {
                    // write this spectrumfiles row to the load file
                    StringBuilder loadFileLine = new StringBuilder();
                    loadFileLine.append(fileDescriptor);
                    loadFileLine.append("\t").append(mzTabRecord.taskID);
                    if (dataLoadManager.isDatasetResult)
                        loadFileLine.append("\t").append(mzTabRecord.datasetID);
                    writer.println(loadFileLine.toString());
                    loadRows++;
                }
            } catch (RuntimeException error) {
                throw error;
            } catch (Throwable error) {
                throw new RuntimeException(error);
            } finally {
                if (writer != null) {
                    try { writer.flush(); } catch (Throwable error) {}
                    try { writer.close(); } catch (Throwable error) {}
                }
            }
            long end = System.nanoTime();
            dataLoadManager.addTiming("spectrumfiles_batch", end - checkpoint, loadRows);
            // import spectrumfiles load file
            checkpoint = end;
            PreparedStatement statement = null;
            Integer loadResult = null;
            try {
                statement = connection.prepareStatement(
                    "LOAD DATA CONCURRENT INFILE ? IGNORE " +
                    "INTO TABLE proxi.spectrumfiles " +
                    "FIELDS TERMINATED BY '\\t' " +
                    "(file_descriptor, task_id, dataset_id)");
                statement.setString(1, loadFile.getAbsolutePath());
                loadResult = statement.executeUpdate();
            } finally {
                try { statement.close(); }
                catch (Throwable error) {}
            }
            // after import is complete, delete load file
            loadFile.delete();
            end = System.nanoTime();
            dataLoadManager.addTiming("spectrumfiles_insert", end - checkpoint, loadResult);
            // query database for newly inserted spectrumfiles IDs
            checkpoint = end;
            StringBuilder valueSet = new StringBuilder("(");
            for (int i=0; i<values.size(); i++)
                valueSet.append("?,");
            if (valueSet.charAt(valueSet.length() - 1) == ',')
                valueSet.setLength(valueSet.length() - 1);
            valueSet.append(")");
            ResultSet result = null;
            int resultCount = 0;
            try {
                statement = connection.prepareStatement(String.format(
                    "SELECT id, file_descriptor " +
                    "FROM proxi.spectrumfiles " +
                    "WHERE file_descriptor IN %s", valueSet.toString()));
                for (int i=0; i<values.size(); i++)
                    statement.setString((i+1), values.get(i));
                result = statement.executeQuery();
                // store queried IDs for subsequent inserts to use
                while (result.next()) {
                    resultCount++;
                    String fileDescriptor = result.getString("file_descriptor");
                    int id = result.getInt("id");
                    dataLoadManager.spectrumFiles.put(fileDescriptor, id);
                    addElement("spectrumFile", fileDescriptor, id);
                    values.remove(fileDescriptor);
                }
            } finally {
                try { result.close(); }
                catch (Throwable error) {}
                try { statement.close(); }
                catch (Throwable error) {}
            }
            end = System.nanoTime();
            dataLoadManager.addTiming("spectrumfiles_query", end - checkpoint, resultCount);
            // IDs should now be stored for all new spectrum files
            if (values.isEmpty() == false) {
                // verify missing IDs in the actual ID map
                Collection<String> missingIDs = new LinkedHashSet<String>(values.size());
                for (Entry<String, Integer> entry : dataLoadManager.spectrumFiles.entrySet())
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
                        message.append(" : ").append(
                            dataLoadManager.spectrumFiles.get(fileDescriptor));
                    }
                    message.append("\n----------");
                    System.out.println(message.toString());
                }
            }
        } 
        long end = System.nanoTime();
        dataLoadManager.addTiming("spectrumfiles", end - start, 1);
    }

    private void loadPeptides() throws SQLException {
        long start = System.nanoTime();
        // get sequences for all new peptides
        List<String> values = new ArrayList<String>(dataLoadManager.peptides.size());
        for (Entry<String, Integer> entry : dataLoadManager.peptides.entrySet())
            if (entry.getValue() == null)
                values.add(entry.getKey());
        // if there were any new peptides, insert the batch
        if (values.isEmpty() == false) {
            // build peptides load file
            long checkpoint = System.nanoTime();
            File loadFile = new File(tempDirectory, "peptides.tsv");
            PrintWriter writer = null;
            int loadRows = 0;
            try {
                writer = new PrintWriter(
                    new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(loadFile)),
                        WRITE_BUFFER_SIZE)
                );
                for (String sequence : values) {
                    // write this peptides row to the load file
                    writer.println(sequence);
                    loadRows++;
                }
            } catch (RuntimeException error) {
                throw error;
            } catch (Throwable error) {
                throw new RuntimeException(error);
            } finally {
                if (writer != null) {
                    try { writer.flush(); } catch (Throwable error) {}
                    try { writer.close(); } catch (Throwable error) {}
                }
            }
            long end = System.nanoTime();
            dataLoadManager.addTiming("peptides_batch", end - checkpoint, loadRows);
            // import peptides load file
            checkpoint = end;
            PreparedStatement statement = null;
            Integer loadResult = null;
            try {
                statement = connection.prepareStatement(
                    "LOAD DATA CONCURRENT INFILE ? IGNORE " +
                    "INTO TABLE proxi.peptides " +
                    "FIELDS TERMINATED BY '\\t' " +
                    "(sequence)");
                statement.setString(1, loadFile.getAbsolutePath());
                loadResult = statement.executeUpdate();
            } finally {
                try { statement.close(); }
                catch (Throwable error) {}
            }
            // after import is complete, delete load file
            loadFile.delete();
            end = System.nanoTime();
            dataLoadManager.addTiming("peptides_insert", end - checkpoint, loadResult);
            // query database for newly inserted peptides IDs
            checkpoint = end;
            StringBuilder valueSet = new StringBuilder("(");
            for (int i=0; i<values.size(); i++)
                valueSet.append("?,");
            if (valueSet.charAt(valueSet.length() - 1) == ',')
                valueSet.setLength(valueSet.length() - 1);
            valueSet.append(")");
            ResultSet result = null;
            int resultCount = 0;
            try {
                statement = connection.prepareStatement(String.format(
                    "SELECT id, sequence " +
                    "FROM proxi.peptides " +
                    "WHERE sequence IN %s", valueSet.toString()));
                for (int i=0; i<values.size(); i++)
                    statement.setString((i+1), values.get(i));
                result = statement.executeQuery();
                // store queried IDs for subsequent inserts to use
                while (result.next()) {
                    resultCount++;
                    String sequence = result.getString("sequence");
                    int id = result.getInt("id");
                    dataLoadManager.peptides.put(sequence, id);
                    addElement("sequence", sequence, id);
                    values.remove(sequence);
                }
            } finally {
                try { result.close(); }
                catch (Throwable error) {}
                try { statement.close(); }
                catch (Throwable error) {}
            }
            end = System.nanoTime();
            dataLoadManager.addTiming("peptides_query", end - checkpoint, resultCount);
            // IDs should now be stored for all new peptides
            if (values.isEmpty() == false) {
                // verify missing IDs in the actual ID map
                Collection<String> missingIDs = new LinkedHashSet<String>(values.size());
                for (Entry<String, Integer> entry : dataLoadManager.peptides.entrySet())
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
                        message.append(" : ").append(dataLoadManager.peptides.get(sequence));
                    }
                    message.append("\n----------");
                    System.out.println(message.toString());
                }
            }
        }
        long end = System.nanoTime();
        dataLoadManager.addTiming("peptides", end - start, 1);
    }

    private void loadProteins() throws SQLException {
        long start = System.nanoTime();
        // get accessions for all new proteins
        List<String> values = new ArrayList<String>(dataLoadManager.proteins.size());
        for (Entry<String, Integer> entry : dataLoadManager.proteins.entrySet())
            if (entry.getValue() == null)
                values.add(entry.getKey());
        // if there were any new proteins, insert the batch
        if (values.isEmpty() == false) {
            // build proteins load file
            long checkpoint = System.nanoTime();
            File loadFile = new File(tempDirectory, "proteins.tsv");
            PrintWriter writer = null;
            int loadRows = 0;
            try {
                writer = new PrintWriter(
                    new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(loadFile)),
                        WRITE_BUFFER_SIZE)
                );
                for (String accession : values) {
                    // write this proteins row to the load file
                    writer.println(accession);
                    loadRows++;
                }
            } catch (RuntimeException error) {
                throw error;
            } catch (Throwable error) {
                throw new RuntimeException(error);
            } finally {
                if (writer != null) {
                    try { writer.flush(); } catch (Throwable error) {}
                    try { writer.close(); } catch (Throwable error) {}
                }
            }
            long end = System.nanoTime();
            dataLoadManager.addTiming("proteins_batch", end - checkpoint, loadRows);
            // import proteins load file
            checkpoint = end;
            PreparedStatement statement = null;
            Integer loadResult = null;
            try {
                statement = connection.prepareStatement(
                    "LOAD DATA CONCURRENT INFILE ? IGNORE " +
                    "INTO TABLE proxi.proteins " +
                    "FIELDS TERMINATED BY '\\t' " +
                    "(name)");
                statement.setString(1, loadFile.getAbsolutePath());
                loadResult = statement.executeUpdate();
            } finally {
                try { statement.close(); }
                catch (Throwable error) {}
            }
            // after import is complete, delete load file
            loadFile.delete();
            end = System.nanoTime();
            dataLoadManager.addTiming("proteins_insert", end - checkpoint, loadResult);
            // query database for newly inserted proteins IDs
            checkpoint = end;
            StringBuilder valueSet = new StringBuilder("(");
            for (int i=0; i<values.size(); i++)
                valueSet.append("?,");
            if (valueSet.charAt(valueSet.length() - 1) == ',')
                valueSet.setLength(valueSet.length() - 1);
            valueSet.append(")");
            ResultSet result = null;
            int resultCount = 0;
            try {
                statement = connection.prepareStatement(String.format(
                    "SELECT id, name " +
                    "FROM proxi.proteins " +
                    "WHERE name IN %s", valueSet.toString()));
                for (int i=0; i<values.size(); i++)
                    statement.setString((i+1), values.get(i));
                result = statement.executeQuery();
                // store queried IDs for subsequent inserts to use
                while (result.next()) {
                    resultCount++;
                    String accession = result.getString("name");
                    int id = result.getInt("id");
                    dataLoadManager.proteins.put(accession, id);
                    addElement("accession", accession, id);
                    values.remove(accession);
                }
            } finally {
                try { result.close(); }
                catch (Throwable error) {}
                try { statement.close(); }
                catch (Throwable error) {}
            }
            end = System.nanoTime();
            dataLoadManager.addTiming("proteins_query", end - checkpoint, resultCount);
            // IDs should now be stored for all new proteins
            if (values.isEmpty() == false) {
                // verify missing IDs in the actual ID map
                Collection<String> missingIDs = new LinkedHashSet<String>(values.size());
                for (Entry<String, Integer> entry : dataLoadManager.proteins.entrySet())
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
                        message.append(" : ").append(dataLoadManager.proteins.get(accession));
                    }
                    message.append("\n----------");
                    System.out.println(message.toString());
                }
            }
        }
        long end = System.nanoTime();
        dataLoadManager.addTiming("proteins", end - start, 1);
    }

    private void loadModifications() throws SQLException {
        long start = System.nanoTime();
        // get all new modifications
        Map<String, Double> values = new LinkedHashMap<String, Double>();
        for (Entry<String, Integer> entry : dataLoadManager.modifications.entrySet()) {
            if (entry.getValue() == null) {
                String name = entry.getKey();
                values.put(name, dataLoadManager.modificationMasses.get(name));
            }
        }
        // if there were any new modifications, insert the batch
        if (values.isEmpty() == false) {
            // build modifications load file
            long checkpoint = System.nanoTime();
            File loadFile = new File(tempDirectory, "modifications.tsv");
            PrintWriter writer = null;
            int loadRows = 0;
            try {
                writer = new PrintWriter(
                    new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(loadFile)),
                        WRITE_BUFFER_SIZE)
                );
                for (Entry<String, Double> entry : values.entrySet()) {
                    // write this modifications row to the load file
                    StringBuilder loadFileLine = new StringBuilder();
                    loadFileLine.append(entry.getKey());
                    loadFileLine.append("\t");
                    Double mass = entry.getValue();
                    if (mass != null)
                        loadFileLine.append(mass);
                    else loadFileLine.append("\\N");
                    writer.println(loadFileLine.toString());
                    loadRows++;
                }
            } catch (RuntimeException error) {
                throw error;
            } catch (Throwable error) {
                throw new RuntimeException(error);
            } finally {
                if (writer != null) {
                    try { writer.flush(); } catch (Throwable error) {}
                    try { writer.close(); } catch (Throwable error) {}
                }
            }
            long end = System.nanoTime();
            dataLoadManager.addTiming("modifications_batch", end - checkpoint, loadRows);
            // import modifications load file
            checkpoint = end;
            PreparedStatement statement = null;
            Integer loadResult = null;
            try {
                statement = connection.prepareStatement(
                    "LOAD DATA CONCURRENT INFILE ? IGNORE " +
                    "INTO TABLE proxi.modifications " +
                    "FIELDS TERMINATED BY '\\t' " +
                    "(name, mass)");
                statement.setString(1, loadFile.getAbsolutePath());
                loadResult = statement.executeUpdate();
            } finally {
                try { statement.close(); }
                catch (Throwable error) {}
            }
            // after import is complete, delete load file
            loadFile.delete();
            end = System.nanoTime();
            dataLoadManager.addTiming("modifications_insert", end - checkpoint, loadResult);
            // query database for newly inserted modifications IDs
            checkpoint = end;
            StringBuilder valueSet = new StringBuilder("(");
            for (int i=0; i<values.size(); i++)
                valueSet.append("?,");
            if (valueSet.charAt(valueSet.length() - 1) == ',')
                valueSet.setLength(valueSet.length() - 1);
            valueSet.append(")");
            ResultSet result = null;
            int resultCount = 0;
            try {
                statement = connection.prepareStatement(String.format(
                    "SELECT id, name " +
                    "FROM proxi.modifications " +
                    "WHERE name IN %s", valueSet.toString()));
                int valueCounter = 1;
                for (Entry<String, Double> entry : values.entrySet())
                    statement.setString(valueCounter++, entry.getKey());
                result = statement.executeQuery();
                // store queried IDs for subsequent inserts to use
                while (result.next()) {
                    resultCount++;
                    String name = result.getString("name");
                    int id = result.getInt("id");
                    dataLoadManager.modifications.put(name, id);
                    addElement("modification", name, id);
                    values.remove(name);
                }
            } finally {
                try { result.close(); }
                catch (Throwable error) {}
                try { statement.close(); }
                catch (Throwable error) {}
            }
            end = System.nanoTime();
            dataLoadManager.addTiming("modifications_query", end - checkpoint, resultCount);
            // IDs should now be stored for all new modifications
            if (values.isEmpty() == false) {
                // verify missing IDs in the actual ID map
                Collection<String> missingIDs = new LinkedHashSet<String>();
                for (Entry<String, Integer> entry : dataLoadManager.modifications.entrySet())
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
                    for (Entry<String, Double> entry : values.entrySet()) {
                        String name = entry.getKey();
                        message.append("\n").append(name);
                        message.append(" : ").append(dataLoadManager.modifications.get(name));
                    }
                    message.append("\n----------");
                    System.out.println(message.toString());
                }
            }
        }
        long end = System.nanoTime();
        dataLoadManager.addTiming("modifications", end - start, 1);
    }

    // level 2 - foreign keys to level 1
    private void loadVariants() throws SQLException {
        long start = System.nanoTime();
        // get sequence/charge for all new variants
        List<ImmutablePair<String, Integer>> values =
            new ArrayList<ImmutablePair<String, Integer>>(dataLoadManager.variants.size());
        for (Entry<ImmutablePair<String, Integer>, Integer> entry :
            dataLoadManager.variants.entrySet())
            if (entry.getValue() == null)
                values.add(entry.getKey());
        // if there were any new variants, set up the batch
        // (using recently generated peptide IDs) and insert it
        if (values.isEmpty() == false) {
            // build variants load file
            long checkpoint = System.nanoTime();
            File loadFile = new File(tempDirectory, "variants.tsv");
            PrintWriter writer = null;
            int loadRows = 0;
            try {
                writer = new PrintWriter(
                    new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(loadFile)),
                        WRITE_BUFFER_SIZE)
                );
                for (ImmutablePair<String, Integer> variant : values) {
                    // get this variant's peptide ID
                    String peptideSequence = dataLoadManager.variantPeptides.get(variant);
                    if (peptideSequence == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for variant [%s/%d]: " +
                            "No peptide sequence was registered for this variant.",
                            variant.getLeft(), variant.getRight()));
                    Integer peptideID = dataLoadManager.peptides.get(peptideSequence);
                    if (peptideID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for variant [%s/%d]: " +
                            "No database ID has been determined yet for associated peptide [%s].",
                            variant.getLeft(), variant.getRight(), peptideSequence));
                    // write this variants row to the load file
                    StringBuilder loadFileLine = new StringBuilder();
                    loadFileLine.append(variant.getLeft());
                    loadFileLine.append("\t").append(variant.getRight());
                    loadFileLine.append("\t").append(peptideID);
                    writer.println(loadFileLine.toString());
                    loadRows++;
                }
            } catch (RuntimeException error) {
                throw error;
            } catch (Throwable error) {
                throw new RuntimeException(error);
            } finally {
                if (writer != null) {
                    try { writer.flush(); } catch (Throwable error) {}
                    try { writer.close(); } catch (Throwable error) {}
                }
            }
            long end = System.nanoTime();
            dataLoadManager.addTiming("variants_batch", end - checkpoint, loadRows);
            // import variants load file
            checkpoint = end;
            PreparedStatement statement = null;
            Integer loadResult = null;
            try {
                statement = connection.prepareStatement(
                    "LOAD DATA CONCURRENT INFILE ? IGNORE " +
                    "INTO TABLE proxi.variants " +
                    "FIELDS TERMINATED BY '\\t' " +
                    "(sequence, charge, peptide_id)");
                statement.setString(1, loadFile.getAbsolutePath());
                loadResult = statement.executeUpdate();
            } finally {
                try { statement.close(); }
                catch (Throwable error) {}
            }
            // after import is complete, delete load file
            loadFile.delete();
            end = System.nanoTime();
            dataLoadManager.addTiming("variants_insert", end - checkpoint, loadResult);
            // query database for newly inserted variants IDs
            checkpoint = end;
            StringBuilder valueSet = new StringBuilder("(");
            for (int i=0; i<values.size(); i++)
                valueSet.append("(?,?),");
            if (valueSet.charAt(valueSet.length() - 1) == ',')
                valueSet.setLength(valueSet.length() - 1);
            valueSet.append(")");
            ResultSet result = null;
            int resultCount = 0;
            try {
                statement = connection.prepareStatement(String.format(
                    "SELECT id, sequence, charge " +
                    "FROM proxi.variants " +
                    "WHERE (sequence, charge) IN %s", valueSet.toString()));
                int valueCounter = 1;
                for (ImmutablePair<String, Integer> variant : values) {
                    statement.setString(valueCounter++, variant.getLeft());
                    statement.setInt(valueCounter++, variant.getRight());
                }
                result = statement.executeQuery();
                // store queried IDs for subsequent inserts to use
                while (result.next()) {
                    resultCount++;
                    String sequence = result.getString("sequence");
                    int charge = result.getInt("charge");
                    int id = result.getInt("id");
                    ImmutablePair<String, Integer> variant =
                        new ImmutablePair<String, Integer>(sequence, charge);
                    dataLoadManager.variants.put(variant, id);
                    addElement("variant", String.format("%s_%d", sequence, charge), id);
                    values.remove(variant);
                }
            } finally {
                try { result.close(); }
                catch (Throwable error) {}
                try { statement.close(); }
                catch (Throwable error) {}
            }
            end = System.nanoTime();
            dataLoadManager.addTiming("variants_query", end - checkpoint, resultCount);
            // IDs should now be stored for all new variants
            if (values.isEmpty() == false) {
                // verify missing IDs in the actual ID map
                Collection<ImmutablePair<String, Integer>> missingIDs =
                    new LinkedHashSet<ImmutablePair<String, Integer>>(values.size());
                for (Entry<ImmutablePair<String, Integer>, Integer> entry :
                    dataLoadManager.variants.entrySet())
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
                        message.append(" : ").append(dataLoadManager.variants.get(variant));
                    }
                    message.append("\n----------");
                    System.out.println(message.toString());
                }
            }
        }
        long end = System.nanoTime();
        dataLoadManager.addTiming("variants", end - start, 1);
    }

    private void loadResultFilePeptides() throws SQLException {
        long start = System.nanoTime();
        // get all new resultfile/peptide join pairs
        Collection<String> values = dataLoadManager.resultFilePeptides;
        // if there were any new joins, set up the batch
        // (using recently generated peptide IDs) and insert it
        if (values.isEmpty() == false) {
            // build resultfile_peptides load file
            long checkpoint = System.nanoTime();
            File loadFile = new File(tempDirectory, "resultfile_peptides.tsv");
            PrintWriter writer = null;
            int loadRows = 0;
            try {
                writer = new PrintWriter(
                    new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(loadFile)),
                        WRITE_BUFFER_SIZE)
                );
                for (String sequence : values) {
                    // get this peptide's ID
                    Integer peptideID = dataLoadManager.peptides.get(sequence);
                    if (peptideID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for resultfile/peptide join [%d/%s]: " +
                            "No database ID has been determined yet for this peptide.",
                            mzTabRecord.id, sequence));
                    // write this resultfile_peptides row to the load file
                    StringBuilder loadFileLine = new StringBuilder();
                    loadFileLine.append(sequence);
                    loadFileLine.append("\t").append(mzTabRecord.id);
                    loadFileLine.append("\t").append(peptideID);
                    writer.println(loadFileLine.toString());
                    loadRows++;
                }
            } catch (RuntimeException error) {
                throw error;
            } catch (Throwable error) {
                throw new RuntimeException(error);
            } finally {
                if (writer != null) {
                    try { writer.flush(); } catch (Throwable error) {}
                    try { writer.close(); } catch (Throwable error) {}
                }
            }
            long end = System.nanoTime();
            dataLoadManager.addTiming("resultfile_peptides_batch", end - checkpoint, loadRows);
            // import resultfile_peptides load file
            checkpoint = end;
            PreparedStatement statement = null;
            Integer loadResult = null;
            try {
                statement = connection.prepareStatement(
                    "LOAD DATA CONCURRENT INFILE ? IGNORE " +
                    "INTO TABLE proxi.resultfile_peptides " +
                    "FIELDS TERMINATED BY '\\t' " +
                    "(sequence, resultfile_id, peptide_id)");
                statement.setString(1, loadFile.getAbsolutePath());
                loadResult = statement.executeUpdate();
            } finally {
                try { statement.close(); }
                catch (Throwable error) {}
            }
            // after import is complete, delete load file
            loadFile.delete();
            end = System.nanoTime();
            dataLoadManager.addTiming("resultfile_peptides_insert", end - checkpoint, loadResult);
        }
        long end = System.nanoTime();
        dataLoadManager.addTiming("resultfile_peptides", end - start, 1);
    }

    private void loadResultFileProteins() throws SQLException {
        long start = System.nanoTime();
        // get all new resultfile/protein join pairs
        Collection<String> values = dataLoadManager.resultFileProteins;
        // if there were any new joins, set up the batch
        // (using recently generated protein IDs) and insert it
        if (values.isEmpty() == false) {
            // build resultfile_proteins load file
            long checkpoint = System.nanoTime();
            File loadFile = new File(tempDirectory, "resultfile_proteins.tsv");
            PrintWriter writer = null;
            int loadRows = 0;
            try {
                writer = new PrintWriter(
                    new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(loadFile)),
                        WRITE_BUFFER_SIZE)
                );
                for (String accession : values) {
                    // get this protein's ID
                    Integer proteinID = dataLoadManager.proteins.get(accession);
                    if (proteinID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for resultfile/protein join [%d/%s]: " +
                            "No database ID has been determined yet for this protein.",
                            mzTabRecord.id, accession));
                    // write this resultfile_proteins row to the load file
                    StringBuilder loadFileLine = new StringBuilder();
                    loadFileLine.append(mzTabRecord.id);
                    loadFileLine.append("\t").append(proteinID);
                    writer.println(loadFileLine.toString());
                    loadRows++;
                }
            } catch (RuntimeException error) {
                throw error;
            } catch (Throwable error) {
                throw new RuntimeException(error);
            } finally {
                if (writer != null) {
                    try { writer.flush(); } catch (Throwable error) {}
                    try { writer.close(); } catch (Throwable error) {}
                }
            }
            long end = System.nanoTime();
            dataLoadManager.addTiming("resultfile_proteins_batch", end - checkpoint, loadRows);
            // import resultfile_proteins load file
            checkpoint = end;
            PreparedStatement statement = null;
            Integer loadResult = null;
            try {
                statement = connection.prepareStatement(
                    "LOAD DATA CONCURRENT INFILE ? IGNORE " +
                    "INTO TABLE proxi.resultfile_proteins " +
                    "FIELDS TERMINATED BY '\\t' " +
                    "(resultfile_id, protein_id)");
                statement.setString(1, loadFile.getAbsolutePath());
                loadResult = statement.executeUpdate();
            } finally {
                try { statement.close(); }
                catch (Throwable error) {}
            }
            // after import is complete, delete load file
            loadFile.delete();
            end = System.nanoTime();
            dataLoadManager.addTiming("resultfile_proteins_insert", end - checkpoint, loadResult);
        }
        long end = System.nanoTime();
        dataLoadManager.addTiming("resultfile_proteins", end - start, 1);
    }

    private void loadResultFileModifications() throws SQLException {
        long start = System.nanoTime();
        // get all new resultfile/modification join pairs
        Collection<String> values = dataLoadManager.resultFileModifications;
        // if there were any new joins, set up the batch
        // (using recently generated modification IDs) and insert it
        if (values.isEmpty() == false) {
            // build resultfile_modifications load file
            long checkpoint = System.nanoTime();
            File loadFile = new File(tempDirectory, "resultfile_modifications.tsv");
            PrintWriter writer = null;
            int loadRows = 0;
            try {
                writer = new PrintWriter(
                    new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(loadFile)),
                        WRITE_BUFFER_SIZE)
                );
                for (String modification : values) {
                    // get this modification's ID
                    Integer modificationID = dataLoadManager.modifications.get(modification);
                    if (modificationID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for resultfile/modification join " +
                            "[%d/%s]: No database ID has been determined yet for this modification.",
                            mzTabRecord.id, modification));
                    // write this resultfile_modifications row to the load file
                    StringBuilder loadFileLine = new StringBuilder();
                    loadFileLine.append(mzTabRecord.id);
                    loadFileLine.append("\t").append(modificationID);
                    writer.println(loadFileLine.toString());
                    loadRows++;
                }
            } catch (RuntimeException error) {
                throw error;
            } catch (Throwable error) {
                throw new RuntimeException(error);
            } finally {
                if (writer != null) {
                    try { writer.flush(); } catch (Throwable error) {}
                    try { writer.close(); } catch (Throwable error) {}
                }
            }
            long end = System.nanoTime();
            dataLoadManager.addTiming("resultfile_modifications_batch", end - checkpoint, loadRows);
            // import resultfile_modifications load file
            checkpoint = end;
            PreparedStatement statement = null;
            Integer loadResult = null;
            try {
                statement = connection.prepareStatement(
                    "LOAD DATA CONCURRENT INFILE ? IGNORE " +
                    "INTO TABLE proxi.resultfile_modifications " +
                    "FIELDS TERMINATED BY '\\t' " +
                    "(resultfile_id, modification_id)");
                statement.setString(1, loadFile.getAbsolutePath());
                loadResult = statement.executeUpdate();
            } finally {
                try { statement.close(); }
                catch (Throwable error) {}
            }
            // after import is complete, delete load file
            loadFile.delete();
            end = System.nanoTime();
            dataLoadManager.addTiming(
                "resultfile_modifications_insert", end - checkpoint, loadResult);
        }
        long end = System.nanoTime();
        dataLoadManager.addTiming("resultfile_modifications", end - start, 1);
    }

    private void loadDatasetPeptides() throws SQLException {
        // if this is not a dataset result, do nothing
        if (dataLoadManager.isDatasetResult == false)
            return;
        long start = System.nanoTime();
        // get all new dataset/peptide join pairs
        Collection<String> values = dataLoadManager.datasetPeptides;
        // if there were any new joins, set up the batch
        // (using recently generated peptide IDs) and insert it
        if (values.isEmpty() == false) {
            // build dataset_peptides load file
            long checkpoint = System.nanoTime();
            File loadFile = new File(tempDirectory, "dataset_peptides.tsv");
            PrintWriter writer = null;
            int loadRows = 0;
            try {
                writer = new PrintWriter(
                    new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(loadFile)),
                        WRITE_BUFFER_SIZE)
                );
                for (String sequence : values) {
                    // get this peptide's ID
                    Integer peptideID = dataLoadManager.peptides.get(sequence);
                    if (peptideID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for dataset/peptide join [%d/%s]: " +
                            "No database ID has been determined yet for this peptide.",
                            mzTabRecord.id, sequence));
                    // write this dataset_peptides row to the load file
                    StringBuilder loadFileLine = new StringBuilder();
                    loadFileLine.append(sequence);
                    loadFileLine.append("\t").append(mzTabRecord.datasetID);
                    loadFileLine.append("\t").append(peptideID);
                    writer.println(loadFileLine.toString());
                    loadRows++;
                }
            } catch (RuntimeException error) {
                throw error;
            } catch (Throwable error) {
                throw new RuntimeException(error);
            } finally {
                if (writer != null) {
                    try { writer.flush(); } catch (Throwable error) {}
                    try { writer.close(); } catch (Throwable error) {}
                }
            }
            long end = System.nanoTime();
            dataLoadManager.addTiming("dataset_peptides_batch", end - checkpoint, loadRows);
            // import dataset_peptides load file
            checkpoint = end;
            PreparedStatement statement = null;
            Integer loadResult = null;
            try {
                statement = connection.prepareStatement(
                    "LOAD DATA CONCURRENT INFILE ? IGNORE " +
                    "INTO TABLE proxi.dataset_peptides " +
                    "FIELDS TERMINATED BY '\\t' " +
                    "(sequence, dataset_id, peptide_id)");
                statement.setString(1, loadFile.getAbsolutePath());
                loadResult = statement.executeUpdate();
            } finally {
                try { statement.close(); }
                catch (Throwable error) {}
            }
            // after import is complete, delete load file
            loadFile.delete();
            end = System.nanoTime();
            dataLoadManager.addTiming("dataset_peptides_insert", end - checkpoint, loadResult);
        }
        long end = System.nanoTime();
        dataLoadManager.addTiming("dataset_peptides", end - start, 1);
    }

    private void loadDatasetProteins() throws SQLException {
        // if this is not a dataset result, do nothing
        if (dataLoadManager.isDatasetResult == false)
            return;
        long start = System.nanoTime();
        // get all new dataset/protein join pairs
        Collection<String> values = dataLoadManager.datasetProteins;
        // if there were any new joins, set up the batch
        // (using recently generated protein IDs) and insert it
        if (values.isEmpty() == false) {
            // build dataset_proteins load file
            long checkpoint = System.nanoTime();
            File loadFile = new File(tempDirectory, "dataset_proteins.tsv");
            PrintWriter writer = null;
            int loadRows = 0;
            try {
                writer = new PrintWriter(
                    new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(loadFile)),
                        WRITE_BUFFER_SIZE)
                );
                for (String accession : values) {
                    // get this protein's ID
                    Integer proteinID = dataLoadManager.proteins.get(accession);
                    if (proteinID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for dataset/protein join [%d/%s]: " +
                            "No database ID has been determined yet for this protein.",
                            mzTabRecord.id, accession));
                    // write this dataset_proteins row to the load file
                    StringBuilder loadFileLine = new StringBuilder();
                    loadFileLine.append(mzTabRecord.datasetID);
                    loadFileLine.append("\t").append(proteinID);
                    writer.println(loadFileLine.toString());
                    loadRows++;
                }
            } catch (RuntimeException error) {
                throw error;
            } catch (Throwable error) {
                throw new RuntimeException(error);
            } finally {
                if (writer != null) {
                    try { writer.flush(); } catch (Throwable error) {}
                    try { writer.close(); } catch (Throwable error) {}
                }
            }
            long end = System.nanoTime();
            dataLoadManager.addTiming("dataset_proteins_batch", end - checkpoint, loadRows);
            // import dataset_proteins load file
            checkpoint = end;
            PreparedStatement statement = null;
            Integer loadResult = null;
            try {
                statement = connection.prepareStatement(
                    "LOAD DATA CONCURRENT INFILE ? IGNORE " +
                    "INTO TABLE proxi.dataset_proteins " +
                    "FIELDS TERMINATED BY '\\t' " +
                    "(dataset_id, protein_id)");
                statement.setString(1, loadFile.getAbsolutePath());
                loadResult = statement.executeUpdate();
            } finally {
                try { statement.close(); }
                catch (Throwable error) {}
            }
            // after import is complete, delete load file
            loadFile.delete();
            end = System.nanoTime();
            dataLoadManager.addTiming("dataset_proteins_insert", end - checkpoint, loadResult);
        }
        long end = System.nanoTime();
        dataLoadManager.addTiming("dataset_proteins", end - start, 1);
    }

    private void loadDatasetModifications() throws SQLException {
        // if this is not a dataset result, do nothing
        if (dataLoadManager.isDatasetResult == false)
            return;
        long start = System.nanoTime();
        // get all new dataset/modifications join pairs
        Collection<String> values = dataLoadManager.datasetModifications;
        // if there were any new joins, set up the batch
        // (using recently generated modifications IDs) and insert it
        if (values.isEmpty() == false) {
            // build dataset_modifications load file
            long checkpoint = System.nanoTime();
            File loadFile = new File(tempDirectory, "dataset_modifications.tsv");
            PrintWriter writer = null;
            int loadRows = 0;
            try {
                writer = new PrintWriter(
                    new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(loadFile)),
                        WRITE_BUFFER_SIZE)
                );
                for (String modification : values) {
                    // get this modification's ID
                    Integer modificationID = dataLoadManager.modifications.get(modification);
                    if (modificationID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for dataset/modification join [%d/%s]: " +
                            "No database ID has been determined yet for this modification.",
                            mzTabRecord.id, modification));
                    // write this dataset_modifications row to the load file
                    StringBuilder loadFileLine = new StringBuilder();
                    loadFileLine.append(mzTabRecord.datasetID);
                    loadFileLine.append("\t").append(modificationID);
                    writer.println(loadFileLine.toString());
                    loadRows++;
                }
            } catch (RuntimeException error) {
                throw error;
            } catch (Throwable error) {
                throw new RuntimeException(error);
            } finally {
                if (writer != null) {
                    try { writer.flush(); } catch (Throwable error) {}
                    try { writer.close(); } catch (Throwable error) {}
                }
            }
            long end = System.nanoTime();
            dataLoadManager.addTiming("dataset_modifications_batch", end - checkpoint, loadRows);
            // import dataset_modifications load file
            checkpoint = end;
            PreparedStatement statement = null;
            Integer loadResult = null;
            try {
                statement = connection.prepareStatement(
                    "LOAD DATA CONCURRENT INFILE ? IGNORE " +
                    "INTO TABLE proxi.dataset_modifications " +
                    "FIELDS TERMINATED BY '\\t' " +
                    "(dataset_id, modification_id)");
                statement.setString(1, loadFile.getAbsolutePath());
                loadResult = statement.executeUpdate();
            } finally {
                try { statement.close(); }
                catch (Throwable error) {}
            }
            // after import is complete, delete load file
            loadFile.delete();
            end = System.nanoTime();
            dataLoadManager.addTiming("dataset_modifications_insert", end - checkpoint, loadResult);
        }
        long end = System.nanoTime();
        dataLoadManager.addTiming("dataset_modifications", end - start, 1);
    }

    private void loadPeptideProteins() throws SQLException {
        long start = System.nanoTime();
        // get all new peptide/protein join pairs
        Collection<ImmutablePair<String, String>> values = dataLoadManager.peptideProteins;
        // if there were any new joins, set up the batch
        // (using recently generated peptide and protein IDs) and insert it
        if (values.isEmpty() == false) {
            // build peptide_proteins load file
            long checkpoint = System.nanoTime();
            File loadFile = new File(tempDirectory, "peptide_proteins.tsv");
            PrintWriter writer = null;
            int loadRows = 0;
            try {
                writer = new PrintWriter(
                    new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(loadFile)),
                        WRITE_BUFFER_SIZE)
                );
                for (ImmutablePair<String, String> value : values) {
                    // get this peptide's ID
                    String sequence = value.getLeft();
                    Integer peptideID = dataLoadManager.peptides.get(sequence);
                    if (peptideID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for peptide/protein joins [%s]: " +
                            "No database ID has been determined yet for this peptide.", sequence));
                    // get this protein's ID
                    String accession = value.getRight();
                    Integer proteinID = dataLoadManager.proteins.get(accession);
                    if (proteinID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for peptide/protein join [%s/%s]: " +
                            "No database ID has been determined yet for this protein.",
                            sequence, accession));
                    // write this peptide_proteins row to the load file
                    StringBuilder loadFileLine = new StringBuilder();
                    loadFileLine.append(sequence);
                    loadFileLine.append("\t").append(peptideID);
                    loadFileLine.append("\t").append(proteinID);
                    writer.println(loadFileLine.toString());
                    loadRows++;
                }
            } catch (RuntimeException error) {
                throw error;
            } catch (Throwable error) {
                throw new RuntimeException(error);
            } finally {
                if (writer != null) {
                    try { writer.flush(); } catch (Throwable error) {}
                    try { writer.close(); } catch (Throwable error) {}
                }
            }
            long end = System.nanoTime();
            dataLoadManager.addTiming("peptide_proteins_batch", end - checkpoint, loadRows);
            // import peptide_proteins load file
            checkpoint = end;
            PreparedStatement statement = null;
            Integer loadResult = null;
            try {
                statement = connection.prepareStatement(
                    "LOAD DATA CONCURRENT INFILE ? IGNORE " +
                    "INTO TABLE proxi.peptide_proteins " +
                    "FIELDS TERMINATED BY '\\t' " +
                    "(sequence, peptide_id, protein_id)");
                statement.setString(1, loadFile.getAbsolutePath());
                loadResult = statement.executeUpdate();
            } finally {
                try { statement.close(); }
                catch (Throwable error) {}
            }
            // after import is complete, delete load file
            loadFile.delete();
            end = System.nanoTime();
            dataLoadManager.addTiming("peptide_proteins_insert", end - checkpoint, loadResult);
        }
        long end = System.nanoTime();
        dataLoadManager.addTiming("peptide_proteins", end - start, 1);
    }

    private void loadPeptideModifications() throws SQLException {
        long start = System.nanoTime();
        // get all new peptide/modification join pairs
        Collection<ImmutablePair<String, String>> values = dataLoadManager.peptideModifications;
        // if there were any new joins, set up the batch
        // (using recently generated peptide and modification IDs) and insert it
        if (values.isEmpty() == false) {
            // build peptide_modifications load file
            long checkpoint = System.nanoTime();
            File loadFile = new File(tempDirectory, "peptide_modifications.tsv");
            PrintWriter writer = null;
            int loadRows = 0;
            try {
                writer = new PrintWriter(
                    new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(loadFile)),
                        WRITE_BUFFER_SIZE)
                );
                for (ImmutablePair<String, String> value : values) {
                    // get this peptide's ID
                    String sequence = value.getLeft();
                    Integer peptideID = dataLoadManager.peptides.get(sequence);
                    if (peptideID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for peptide/modification joins [%s]: " +
                            "No database ID has been determined yet for this peptide.", sequence));
                    // get this modification's ID
                    String modification = value.getRight();
                    Integer modificationID = dataLoadManager.modifications.get(modification);
                    if (modificationID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for peptide/modification join " +
                            "[%s/%s]: No database ID has been determined yet for this " +
                            "modification.", sequence, modification));
                    // write this peptide_modifications row to the load file
                    StringBuilder loadFileLine = new StringBuilder();
                    loadFileLine.append(peptideID);
                    loadFileLine.append("\t").append(modificationID);
                    writer.println(loadFileLine.toString());
                    loadRows++;
                }
            } catch (RuntimeException error) {
                throw error;
            } catch (Throwable error) {
                throw new RuntimeException(error);
            } finally {
                if (writer != null) {
                    try { writer.flush(); } catch (Throwable error) {}
                    try { writer.close(); } catch (Throwable error) {}
                }
            }
            long end = System.nanoTime();
            dataLoadManager.addTiming("peptide_modifications_batch", end - checkpoint, loadRows);
            // import peptide_modifications load file
            checkpoint = end;
            PreparedStatement statement = null;
            Integer loadResult = null;
            try {
                statement = connection.prepareStatement(
                    "LOAD DATA CONCURRENT INFILE ? IGNORE " +
                    "INTO TABLE proxi.peptide_modifications " +
                    "FIELDS TERMINATED BY '\\t' " +
                    "(peptide_id, modification_id)");
                statement.setString(1, loadFile.getAbsolutePath());
                loadResult = statement.executeUpdate();
            } finally {
                try { statement.close(); }
                catch (Throwable error) {}
            }
            // after import is complete, delete load file
            loadFile.delete();
            end = System.nanoTime();
            dataLoadManager.addTiming("peptide_modifications_insert", end - checkpoint, loadResult);
        }
        long end = System.nanoTime();
        dataLoadManager.addTiming("peptide_modifications", end - start, 1);
    }

    private void loadProteinModifications() throws SQLException {
        long start = System.nanoTime();
        // get all new protein/modification join pairs
        Collection<ImmutablePair<String, String>> values = dataLoadManager.proteinModifications;
        // if there were any new joins, set up the batch
        // (using recently generated protein and modification IDs) and insert it
        if (values.isEmpty() == false) {
            // build protein_modifications load file
            long checkpoint = System.nanoTime();
            File loadFile = new File(tempDirectory, "protein_modifications.tsv");
            PrintWriter writer = null;
            int loadRows = 0;
            try {
                writer = new PrintWriter(
                    new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(loadFile)),
                        WRITE_BUFFER_SIZE)
                );
                for (ImmutablePair<String, String> value : values) {
                    // get this protein's ID
                    String accession = value.getLeft();
                    Integer proteinID = dataLoadManager.proteins.get(accession);
                    if (proteinID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for protein/modification joins [%s]: " +
                            "No database ID has been determined yet for this protein.", accession));
                    // get this modification's ID
                    String modification = value.getRight();
                    Integer modificationID = dataLoadManager.modifications.get(modification);
                    if (modificationID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for protein/modification join " +
                            "[%s/%s]: No database ID has been determined yet for this " +
                            "modification.", accession, modification));
                    // write this protein_modifications row to the load file
                    StringBuilder loadFileLine = new StringBuilder();
                    loadFileLine.append(proteinID);
                    loadFileLine.append("\t").append(modificationID);
                    writer.println(loadFileLine.toString());
                    loadRows++;
                }
            } catch (RuntimeException error) {
                throw error;
            } catch (Throwable error) {
                throw new RuntimeException(error);
            } finally {
                if (writer != null) {
                    try { writer.flush(); } catch (Throwable error) {}
                    try { writer.close(); } catch (Throwable error) {}
                }
            }
            long end = System.nanoTime();
            dataLoadManager.addTiming("protein_modifications_batch", end - checkpoint, loadRows);
            // import protein_modifications load file
            checkpoint = end;
            PreparedStatement statement = null;
            Integer loadResult = null;
            try {
                statement = connection.prepareStatement(
                    "LOAD DATA CONCURRENT INFILE ? IGNORE " +
                    "INTO TABLE proxi.protein_modifications " +
                    "FIELDS TERMINATED BY '\\t' " +
                    "(protein_id, modification_id)");
                statement.setString(1, loadFile.getAbsolutePath());
                loadResult = statement.executeUpdate();
            } finally {
                try { statement.close(); }
                catch (Throwable error) {}
            }
            // after import is complete, delete load file
            loadFile.delete();
            end = System.nanoTime();
            dataLoadManager.addTiming("protein_modifications_insert", end - checkpoint, loadResult);
        }
        long end = System.nanoTime();
        dataLoadManager.addTiming("protein_modifications", end - start, 1);
    }

    // level 3 - foreign keys to level 2
    private void loadPSMs() throws SQLException {
        long start = System.nanoTime();
        // get all new PSMs
        Map<Integer, PSM> values = new TreeMap<Integer, PSM>();
        for (PSM psm : dataLoadManager.psms.keySet())
            values.put(psm.getIndex(), psm);
        long end = System.nanoTime();
        // if there were any new PSMs, set up the batch
        // (using recently generated spectrumfile/peptide/variant IDs) and insert it
        if (values.isEmpty() == false) {
            // build psms load file
            long checkpoint = System.nanoTime();
            File loadFile = new File(tempDirectory, "psms.tsv");
            PrintWriter writer = null;
            int loadRows = 0;
            try {
                writer = new PrintWriter(
                    new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(loadFile)),
                        WRITE_BUFFER_SIZE)
                );
                for (Entry<Integer, PSM> entry : values.entrySet()) {
                    PSM psm = entry.getValue();
                    // get this PSM's spectrumfile, peptide, and variant IDs
                    ImmutableTriple<String, String, ImmutablePair<String, Integer>> properties =
                        dataLoadManager.psms.get(psm);
                    if (properties == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for PSM %,d: " +
                            "No properties tuple was registered for this PSM.", entry.getKey()));
                    String spectrumFileDescriptor = properties.getLeft();
                    if (spectrumFileDescriptor == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for PSM %,d: " +
                            "Properties tuple for this PSM has null spectrum file descriptor.",
                            entry.getKey()));
                    Integer spectrumFileID =
                        dataLoadManager.spectrumFiles.get(spectrumFileDescriptor);
                    if (spectrumFileID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for PSM %,d: " +
                            "No database ID has been determined yet for associated " +
                            "spectrum file [%s].", entry.getKey(), spectrumFileDescriptor));
                    String peptideSequence = properties.getMiddle();
                    if (peptideSequence == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for PSM %,d: " +
                            "Properties tuple for this PSM has null peptide sequence.",
                            entry.getKey()));
                    Integer peptideID = dataLoadManager.peptides.get(peptideSequence);
                    if (peptideID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for PSM %,d: " +
                            "No database ID has been determined yet for associated " +
                            "peptide [%s].", entry.getKey(), peptideSequence));
                    ImmutablePair<String, Integer> variant = properties.getRight();
                    if (variant == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for PSM %,d: " +
                            "Properties tuple for this PSM has null variant.",
                            entry.getKey()));
                    Integer variantID = dataLoadManager.variants.get(variant);
                    if (variantID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for PSM %,d: " +
                            "No database ID has been determined yet for associated " +
                            "variant [%s/%d].", entry.getKey(),
                            variant.getLeft(), variant.getRight()));
                    // note this psm's database ID
                    int index = psm.getIndex();
                    psmDatabaseIDs[index] = lastPSMDatabaseID;
                    // write this psms row to the load file
                    StringBuilder loadFileLine = new StringBuilder();
                    loadFileLine.append(lastPSMDatabaseID++);
                    loadFileLine.append("\t").append(psm.getID());
                    loadFileLine.append("\t").append(index);
                    loadFileLine.append("\t").append(psm.getNativeID());
                    loadFileLine.append("\t").append(psm.getModifiedSequence());
                    loadFileLine.append("\t").append(psm.getCharge());
                    Double massToCharge = psm.getMassToCharge();
                    if (massToCharge != null)
                        loadFileLine.append("\t").append(massToCharge);
                    else loadFileLine.append("\t\\N");
                    loadFileLine.append("\t").append(mzTabRecord.id);
                    loadFileLine.append("\t").append(spectrumFileID);
                    loadFileLine.append("\t").append(peptideID);
                    loadFileLine.append("\t").append(variantID);
                    if (dataLoadManager.isDatasetResult)
                        loadFileLine.append("\t").append(mzTabRecord.datasetID);
                    writer.println(loadFileLine.toString());
                    loadRows++;
                }
            } catch (RuntimeException error) {
                throw error;
            } catch (Throwable error) {
                throw new RuntimeException(error);
            } finally {
                if (writer != null) {
                    try { writer.flush(); } catch (Throwable error) {}
                    try { writer.close(); } catch (Throwable error) {}
                }
            }
            end = System.nanoTime();
            dataLoadManager.addTiming("psms_batch", end - checkpoint, loadRows);
            // import psms load file
            checkpoint = end;
            PreparedStatement statement = null;
            Integer loadResult = null;
            try {
                StringBuilder sql = new StringBuilder(
                    "LOAD DATA CONCURRENT INFILE ? IGNORE " +
                    "INTO TABLE proxi.psms " +
                    "FIELDS TERMINATED BY '\\t' " +
                    "(id, id_in_file, index_in_file, nativeid, variant_sequence, charge, " +
                    "exp_mass_to_charge, resultfile_id, spectrumfile_id, peptide_id, variant_id");
                if (dataLoadManager.isDatasetResult)
                    sql.append(", dataset_id");
                sql.append(")");
                statement = connection.prepareStatement(sql.toString());
                statement.setString(1, loadFile.getAbsolutePath());
                loadResult = statement.executeUpdate();
            } finally {
                try { statement.close(); }
                catch (Throwable error) {}
            }
            // after import is complete, delete load file
            loadFile.delete();
            end = System.nanoTime();
            dataLoadManager.addTiming("psms_insert", end - checkpoint, loadResult);
        }
        dataLoadManager.addTiming("psms", end - start, 1);
    }

    private void loadResultFileVariants() throws SQLException {
        long start = System.nanoTime();
        // get all new resultfile/variant join pairs
        Collection<ImmutablePair<String, Integer>> values = dataLoadManager.resultFileVariants;
        // if there were any new joins, set up the batch
        // (using recently generated variant IDs) and insert it
        if (values.isEmpty() == false) {
            // build resultfile_variants load file
            long checkpoint = System.nanoTime();
            File loadFile = new File(tempDirectory, "resultfile_variants.tsv");
            PrintWriter writer = null;
            int loadRows = 0;
            try {
                writer = new PrintWriter(
                    new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(loadFile)),
                        WRITE_BUFFER_SIZE)
                );
                for (ImmutablePair<String, Integer> variant : values) {
                    // get this variant's ID
                    Integer variantID = dataLoadManager.variants.get(variant);
                    if (variantID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for resultfile/variant join " +
                            "[%d/%s/%d]: No database ID has been determined yet for this variant.",
                            mzTabRecord.id, variant.getLeft(), variant.getRight()));
                    // write this resultfile_variants row to the load file
                    StringBuilder loadFileLine = new StringBuilder();
                    loadFileLine.append(variant.getLeft());
                    loadFileLine.append("\t").append(mzTabRecord.id);
                    loadFileLine.append("\t").append(variantID);
                    writer.println(loadFileLine.toString());
                    loadRows++;
                }
            } catch (RuntimeException error) {
                throw error;
            } catch (Throwable error) {
                throw new RuntimeException(error);
            } finally {
                if (writer != null) {
                    try { writer.flush(); } catch (Throwable error) {}
                    try { writer.close(); } catch (Throwable error) {}
                }
            }
            long end = System.nanoTime();
            dataLoadManager.addTiming("resultfile_variants_batch", end - checkpoint, loadRows);
            // import resultfile_variants load file
            checkpoint = end;
            PreparedStatement statement = null;
            Integer loadResult = null;
            try {
                statement = connection.prepareStatement(
                    "LOAD DATA CONCURRENT INFILE ? IGNORE " +
                    "INTO TABLE proxi.resultfile_variants " +
                    "FIELDS TERMINATED BY '\\t' " +
                    "(sequence, resultfile_id, variant_id)");
                statement.setString(1, loadFile.getAbsolutePath());
                loadResult = statement.executeUpdate();
            } finally {
                try { statement.close(); }
                catch (Throwable error) {}
            }
            // after import is complete, delete load file
            loadFile.delete();
            end = System.nanoTime();
            dataLoadManager.addTiming("resultfile_variants_insert", end - checkpoint, loadResult);
        }
        long end = System.nanoTime();
        dataLoadManager.addTiming("resultfile_variants", end - start, 1);
    }

    private void loadDatasetVariants() throws SQLException {
        // if this is not a dataset result, do nothing
        if (dataLoadManager.isDatasetResult == false)
            return;
        long start = System.nanoTime();
        // get all new dataset/variant join pairs
        Collection<ImmutablePair<String, Integer>> values = dataLoadManager.datasetVariants;
        // if there were any new joins, set up the batch
        // (using recently generated variant IDs) and insert it
        if (values.isEmpty() == false) {
            // build dataset_variants load file
            long checkpoint = System.nanoTime();
            File loadFile = new File(tempDirectory, "dataset_variants.tsv");
            PrintWriter writer = null;
            int loadRows = 0;
            try {
                writer = new PrintWriter(
                    new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(loadFile)),
                        WRITE_BUFFER_SIZE)
                );
                for (ImmutablePair<String, Integer> variant : values) {
                    // get this variant's ID
                    Integer variantID = dataLoadManager.variants.get(variant);
                    if (variantID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for dataset/variant join [%d/%s/%d]: " +
                            "No database ID has been determined yet for this variant.",
                            mzTabRecord.id, variant.getLeft(), variant.getRight()));
                    // write this dataset_variants row to the load file
                    StringBuilder loadFileLine = new StringBuilder();
                    loadFileLine.append(variant.getLeft());
                    loadFileLine.append("\t").append(mzTabRecord.datasetID);
                    loadFileLine.append("\t").append(variantID);
                    writer.println(loadFileLine.toString());
                    loadRows++;
                }
            } catch (RuntimeException error) {
                throw error;
            } catch (Throwable error) {
                throw new RuntimeException(error);
            } finally {
                if (writer != null) {
                    try { writer.flush(); } catch (Throwable error) {}
                    try { writer.close(); } catch (Throwable error) {}
                }
            }
            long end = System.nanoTime();
            dataLoadManager.addTiming("dataset_variants_batch", end - checkpoint, loadRows);
            // import dataset_variants load file
            checkpoint = end;
            PreparedStatement statement = null;
            Integer loadResult = null;
            try {
                statement = connection.prepareStatement(
                    "LOAD DATA CONCURRENT INFILE ? IGNORE " +
                    "INTO TABLE proxi.dataset_variants " +
                    "FIELDS TERMINATED BY '\\t' " +
                    "(sequence, dataset_id, variant_id)");
                statement.setString(1, loadFile.getAbsolutePath());
                loadResult = statement.executeUpdate();
            } finally {
                try { statement.close(); }
                catch (Throwable error) {}
            }
            // after import is complete, delete load file
            loadFile.delete();
            end = System.nanoTime();
            dataLoadManager.addTiming("dataset_variants_insert", end - checkpoint, loadResult);
        }
        long end = System.nanoTime();
        dataLoadManager.addTiming("dataset_variants", end - start, 1);
    }

    private void loadVariantProteins() throws SQLException {
        long start = System.nanoTime();
        // get all new variant/protein join pairs
        Collection<ImmutablePair<ImmutablePair<String, Integer>, String>> values =
            dataLoadManager.variantProteins;
        // if there were any new joins, set up the batch
        // (using recently generated variant and protein IDs) and insert it
        if (values.isEmpty() == false) {
            // build variant_proteins load file
            long checkpoint = System.nanoTime();
            File loadFile = new File(tempDirectory, "variant_proteins.tsv");
            PrintWriter writer = null;
            int loadRows = 0;
            try {
                writer = new PrintWriter(
                    new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(loadFile)),
                        WRITE_BUFFER_SIZE)
                );
                for (ImmutablePair<ImmutablePair<String, Integer>, String> value : values) {
                    // get this variant's ID
                    ImmutablePair<String, Integer> variant = value.getLeft();
                    Integer variantID = dataLoadManager.variants.get(variant);
                    if (variantID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for variant/protein joins [%s/%d]: " +
                            "No database ID has been determined yet for this variant.",
                            variant.getLeft(), variant.getRight()));
                    // get this protein's ID
                    String accession = value.getRight();
                    Integer proteinID = dataLoadManager.proteins.get(accession);
                    if (proteinID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for variant/protein join " +
                            "[%s/%d/%s]: No database ID has been determined yet for this " +
                            "protein.", variant.getLeft(), variant.getRight(), accession));
                    // write this variant_proteins row to the load file
                    StringBuilder loadFileLine = new StringBuilder();
                    loadFileLine.append(variantID);
                    loadFileLine.append("\t").append(proteinID);
                    writer.println(loadFileLine.toString());
                    loadRows++;
                }
            } catch (RuntimeException error) {
                throw error;
            } catch (Throwable error) {
                throw new RuntimeException(error);
            } finally {
                if (writer != null) {
                    try { writer.flush(); } catch (Throwable error) {}
                    try { writer.close(); } catch (Throwable error) {}
                }
            }
            long end = System.nanoTime();
            dataLoadManager.addTiming("variant_proteins_batch", end - checkpoint, loadRows);
            // import variant_proteins load file
            checkpoint = end;
            PreparedStatement statement = null;
            Integer loadResult = null;
            try {
                statement = connection.prepareStatement(
                    "LOAD DATA CONCURRENT INFILE ? IGNORE " +
                    "INTO TABLE proxi.variant_proteins " +
                    "FIELDS TERMINATED BY '\\t' " +
                    "(variant_id, protein_id)");
                statement.setString(1, loadFile.getAbsolutePath());
                loadResult = statement.executeUpdate();
            } finally {
                try { statement.close(); }
                catch (Throwable error) {}
            }
            // after import is complete, delete load file
            loadFile.delete();
            end = System.nanoTime();
            dataLoadManager.addTiming("variant_proteins_insert", end - checkpoint, loadResult);
        }
        long end = System.nanoTime();
        dataLoadManager.addTiming("variant_proteins", end - start, 1);
    }

    private void loadVariantModifications() throws SQLException {
        long start = System.nanoTime();
        // get all new variant/modification join pairs
        Collection<ImmutablePair<
            ImmutablePair<String, Integer>, ImmutablePair<String, Integer>>> values =
                dataLoadManager.variantModifications;
        // if there were any new joins, set up the batch
        // (using recently generated variant and modification IDs) and insert it
        if (values.isEmpty() == false) {
            // build variant_modifications load file
            long checkpoint = System.nanoTime();
            File loadFile = new File(tempDirectory, "variant_modifications.tsv");
            PrintWriter writer = null;
            int loadRows = 0;
            try {
                writer = new PrintWriter(
                    new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(loadFile)),
                        WRITE_BUFFER_SIZE)
                );
                for (ImmutablePair<
                    ImmutablePair<String, Integer>, ImmutablePair<String, Integer>> value : values) {
                    // get this variant's ID
                    ImmutablePair<String, Integer> variant = value.getLeft();
                    Integer variantID = dataLoadManager.variants.get(variant);
                    if (variantID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for variant/modification joins " +
                            "[%s/%d]: No database ID has been determined yet for this variant.",
                            variant.getLeft(), variant.getRight()));
                    // modification should have non-null position
                    // to have been registered for a variant join
                    ImmutablePair<String, Integer> modification = value.getRight();
                    Integer position = modification.getRight();
                    if (position == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for variant/modification join " +
                            "[%s/%d/%s]: Modification has null position - how did it get " +
                            "registered for a variant join?",
                            variant.getLeft(), variant.getRight(), modification.getLeft()));
                    // get this modification's ID
                    String name = modification.getLeft();
                    Integer modificationID = dataLoadManager.modifications.get(name);
                    if (modificationID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for variant/modification join " +
                            "[%s/%d/%s/%d]: No database ID has been determined yet for this " +
                            "modification.", variant.getLeft(), variant.getRight(),
                            name, position));
                    // write this variant_modifications row to the load file
                    StringBuilder loadFileLine = new StringBuilder();
                    loadFileLine.append(position);
                    loadFileLine.append("\t").append(variantID);
                    loadFileLine.append("\t").append(modificationID);
                    writer.println(loadFileLine.toString());
                    loadRows++;
                }
            } catch (RuntimeException error) {
                throw error;
            } catch (Throwable error) {
                throw new RuntimeException(error);
            } finally {
                if (writer != null) {
                    try { writer.flush(); } catch (Throwable error) {}
                    try { writer.close(); } catch (Throwable error) {}
                }
            }
            long end = System.nanoTime();
            dataLoadManager.addTiming("variant_modifications_batch", end - checkpoint, loadRows);
            // import variant_modifications load file
            checkpoint = end;
            PreparedStatement statement = null;
            Integer loadResult = null;
            try {
                statement = connection.prepareStatement(
                    "LOAD DATA CONCURRENT INFILE ? IGNORE " +
                    "INTO TABLE proxi.variant_modifications " +
                    "FIELDS TERMINATED BY '\\t' " +
                    "(location, variant_id, modification_id)");
                statement.setString(1, loadFile.getAbsolutePath());
                loadResult = statement.executeUpdate();
            } finally {
                try { statement.close(); }
                catch (Throwable error) {}
            }
            // after import is complete, delete load file
            loadFile.delete();
            end = System.nanoTime();
            dataLoadManager.addTiming("variant_modifications_insert", end - checkpoint, loadResult);
        }
        long end = System.nanoTime();
        dataLoadManager.addTiming("variant_modifications", end - start, 1);
    }

    // level 3 - foreign keys to level 2
    private void loadPSMProteins() throws SQLException {
        long start = System.nanoTime();
        // get all new PSM/protein join pairs
        Collection<ImmutablePair<PSM, String>> values = dataLoadManager.psmProteins;
        // if there were any new joins, set up the batch
        // (using recently generated PSM and protein IDs) and insert it
        if (values.isEmpty() == false) {
            // build psm_proteins load file
            long checkpoint = System.nanoTime();
            File loadFile = new File(tempDirectory, "psm_proteins.tsv");
            PrintWriter writer = null;
            int loadRows = 0;
            try {
                writer = new PrintWriter(
                    new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(loadFile)),
                        WRITE_BUFFER_SIZE)
                );
                for (ImmutablePair<PSM, String> value : values) {
                    // get this PSM's ID
                    PSM psm = value.getLeft();
                    Long psmID = psmDatabaseIDs[psm.getIndex()];
                    if (psmID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for PSM/protein joins [%d]: " +
                            "No database ID has been determined yet for this PSM.",
                            psm.getIndex()));
                    // get this protein's ID
                    String accession = value.getRight();
                    Integer proteinID = dataLoadManager.proteins.get(accession);
                    if (proteinID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for PSM/protein join [%d/%s]: " +
                            "No database ID has been determined yet for this protein.",
                            psm.getIndex(), accession));
                    // write this psm_proteins row to the load file
                    StringBuilder loadFileLine = new StringBuilder();
                    loadFileLine.append(psmID);
                    loadFileLine.append("\t").append(proteinID);
                    writer.println(loadFileLine.toString());
                    loadRows++;
                }
            } catch (RuntimeException error) {
                throw error;
            } catch (Throwable error) {
                throw new RuntimeException(error);
            } finally {
                if (writer != null) {
                    try { writer.flush(); } catch (Throwable error) {}
                    try { writer.close(); } catch (Throwable error) {}
                }
            }
            long end = System.nanoTime();
            dataLoadManager.addTiming("psm_proteins_batch", end - checkpoint, loadRows);
            // import psm_proteins load file
            checkpoint = end;
            PreparedStatement statement = null;
            Integer loadResult = null;
            try {
                statement = connection.prepareStatement(
                    "LOAD DATA CONCURRENT INFILE ? IGNORE " +
                    "INTO TABLE proxi.psm_proteins " +
                    "FIELDS TERMINATED BY '\\t' " +
                    "(psm_id, protein_id)");
                statement.setString(1, loadFile.getAbsolutePath());
                loadResult = statement.executeUpdate();
            } finally {
                try { statement.close(); }
                catch (Throwable error) {}
            }
            // after import is complete, delete load file
            loadFile.delete();
            end = System.nanoTime();
            dataLoadManager.addTiming("psm_proteins_insert", end - checkpoint, loadResult);
        }
        long end = System.nanoTime();
        dataLoadManager.addTiming("psm_proteins", end - start, 1);
    }

    private void loadPSMModifications() throws SQLException {
        long start = System.nanoTime();
        // get all new PSM/modification join pairs
        Collection<ImmutablePair<PSM, String>> values = dataLoadManager.psmModifications;
        // if there were any new joins, set up the batch
        // (using recently generated PSM and modification IDs) and insert it
        if (values.isEmpty() == false) {
            // build psm_modifications load file
            long checkpoint = System.nanoTime();
            File loadFile = new File(tempDirectory, "psm_modifications.tsv");
            PrintWriter writer = null;
            int loadRows = 0;
            try {
                writer = new PrintWriter(
                    new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(loadFile)),
                        WRITE_BUFFER_SIZE)
                );
                for (ImmutablePair<PSM, String> value : values) {
                    // get this PSM's ID
                    PSM psm = value.getLeft();
                    Long psmID = psmDatabaseIDs[psm.getIndex()];
                    if (psmID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for PSM/modification joins [%d]: " +
                            "No database ID has been determined yet for this PSM.",
                            psm.getIndex()));
                    // get this modification's ID
                    String modification = value.getRight();
                    Integer modificationID = dataLoadManager.modifications.get(modification);
                    if (modificationID == null)
                        throw new IllegalStateException(String.format(
                            "ERROR setting up batch insert for PSM/modification join " +
                            "[%d/%s]: No database ID has been determined yet for this " +
                            "modification.", psm.getIndex(), modification));
                    // write this psm_modifications row to the load file
                    StringBuilder loadFileLine = new StringBuilder();
                    loadFileLine.append(psmID);
                    loadFileLine.append("\t").append(modificationID);
                    writer.println(loadFileLine.toString());
                    loadRows++;
                }
            } catch (RuntimeException error) {
                throw error;
            } catch (Throwable error) {
                throw new RuntimeException(error);
            } finally {
                if (writer != null) {
                    try { writer.flush(); } catch (Throwable error) {}
                    try { writer.close(); } catch (Throwable error) {}
                }
            }
            long end = System.nanoTime();
            dataLoadManager.addTiming("psm_modifications_batch", end - checkpoint, loadRows);
            // import psm_modifications load file
            checkpoint = end;
            PreparedStatement statement = null;
            Integer loadResult = null;
            try {
                statement = connection.prepareStatement(
                    "LOAD DATA CONCURRENT INFILE ? IGNORE " +
                    "INTO TABLE proxi.psm_modifications " +
                    "FIELDS TERMINATED BY '\\t' " +
                    "(psm_id, modification_id)");
                statement.setString(1, loadFile.getAbsolutePath());
                loadResult = statement.executeUpdate();
            } finally {
                try { statement.close(); }
                catch (Throwable error) {}
            }
            // after import is complete, delete load file
            loadFile.delete();
            end = System.nanoTime();
            dataLoadManager.addTiming("psm_modifications_insert", end - checkpoint, loadResult);
        }
        long end = System.nanoTime();
        dataLoadManager.addTiming("psm_modifications", end - start, 1);
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
            values = new TreeMap<String, Integer>();
        values.put(value, id);
        uniqueElements.put(type, values);
        // add to global map, if requested and present
        if (localOnly == false && globalElements != null) {
            values = globalElements.get(type);
            if (values == null)
                values = new TreeMap<String, Integer>();
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
        StringBuilder count = new StringBuilder().append(String.format("%,d", elements));
        if (rows > 0) {
            count.append(" (").append(String.format("%,d", rows)).append(" ");
            count.append(CommonUtils.pluralize("row", rows));
            if (invalid > 0) {
                count.append(", ").append(String.format("%,d", invalid)).append(" invalid ");
                count.append(CommonUtils.pluralize("row", invalid));
            }
            if (unimported > 0) {
                count.append(", ").append(String.format("%,d", unimported)).append(" unimported ");
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
            for (Entry<String, MassIVESearchTimingRecord> entry :
                dataLoadManager.timingRecords.entrySet())
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
        String[] summary = new String[14];
        MassIVESearchTimingRecord record =
            dataLoadManager.timingRecords.get("psm_row_preprocessing");
        String total = null;
        Long preProcessingTotal = null;
        if (record != null) {
            preProcessingTotal = record.getTotal();
            total = CommonUtils.formatNanoseconds(preProcessingTotal);
            message.append("\nPSM row pre-processing:").append("\t").append(total);
            message.append("\n  Rows:")
                .append("\t").append(String.format("%,d", record.getCount()));
            message.append("\n  Slowest row:")
                .append("\t").append(CommonUtils.formatNanoseconds(record.getMax()));
            message.append("\n  Fastest row:")
                .append("\t").append(CommonUtils.formatNanoseconds(record.getMin()));
            message.append("\n  Average row:")
                .append("\t").append(CommonUtils.formatNanoseconds(Math.round(record.getAverage())));
        }
        record = dataLoadManager.timingRecords.get("psm_row_increment_row_count");
        if (record != null) {
            total = CommonUtils.formatNanoseconds(record.getTotal());
            message.append("\n\t\tIncrement PSM row count:").append("\t").append(total);
            summary[6] = total;
        } else summary[6] = "null";
        record = dataLoadManager.timingRecords.get("psm_row_validate");
        if (record != null) {
            total = CommonUtils.formatNanoseconds(record.getTotal());
            message.append("\n\t\tValidate PSM row:").append("\t").append(total);
            summary[7] = total;
        } else summary[7] = "null";
        record = dataLoadManager.timingRecords.get("psm_row_check_index");
        if (record != null) {
            total = CommonUtils.formatNanoseconds(record.getTotal());
            message.append("\n\t\tProcess PSM index:").append("\t").append(total);
            summary[8] = total;
        } else summary[8] = "null";
        record = dataLoadManager.timingRecords.get("psm_row_check_importable");
        if (record != null) {
            total = CommonUtils.formatNanoseconds(record.getTotal());
            message.append("\n\t\tFirst check that PSM row is importable:")
                .append("\t").append(total);
            summary[9] = total;
        } else summary[9] = "null";
        record = dataLoadManager.timingRecords.get("psm_row_clean_mods");
        if (record != null) {
            total = CommonUtils.formatNanoseconds(record.getTotal());
            message.append("\n\t\tModification formatting:").append("\t").append(total);
            summary[10] = total;
        } else summary[10] = "null";
        record = dataLoadManager.timingRecords.get("psm_row_instantiate_psm");
        if (record != null) {
            total = CommonUtils.formatNanoseconds(record.getTotal());
            message.append("\n\t\tInstantiate PSM object:").append("\t").append(total);
            summary[11] = total;
        } else summary[11] = "null";
        record = dataLoadManager.timingRecords.get("psm_row_check_importable_2");
        if (record != null) {
            total = CommonUtils.formatNanoseconds(record.getTotal());
            message.append("\n\t\tSecond check that PSM row is importable:")
                .append("\t").append(total);
            summary[12] = total;
        } else summary[12] = "null";
        record = dataLoadManager.timingRecords.get("psm_row_protein_clean");
        if (record != null) {
            long proteinCleanTotal = record.getTotal();
            total = CommonUtils.formatNanoseconds(proteinCleanTotal);
            message.append("\nProtein string formatting:").append("\t").append(total);
            message.append("\n  Proteins:")
                .append("\t").append(String.format("%,d", record.getCount()));
            message.append("\n  Slowest protein:")
                .append("\t").append(CommonUtils.formatNanoseconds(record.getMax()));
            message.append("\n  Fastest protein:")
                .append("\t").append(CommonUtils.formatNanoseconds(record.getMin()));
            message.append("\n  Average protein:")
                .append("\t").append(CommonUtils.formatNanoseconds(Math.round(record.getAverage())));
            summary[13] = total;
            if (preProcessingTotal == null)
                preProcessingTotal = proteinCleanTotal;
            else preProcessingTotal += proteinCleanTotal;
        } else summary[13] = "null";
        if (preProcessingTotal != null)
            summary[0] = CommonUtils.formatNanoseconds(preProcessingTotal);
        else summary[0] = "null";
        record = dataLoadManager.timingRecords.get("psm_row_cascade");
        if (record != null) {
            total = CommonUtils.formatNanoseconds(record.getTotal());
            message.append("\nPSM row batch processing:").append("\t").append(total);
            message.append("\n  PSM/protein pairs:")
                .append("\t").append(String.format("%,d", record.getCount()));
            message.append("\n  Slowest PSM/protein pair:")
                .append("\t").append(CommonUtils.formatNanoseconds(record.getMax()));
            message.append("\n  Fastest PSM/protein pair:")
                .append("\t").append(CommonUtils.formatNanoseconds(record.getMin()));
            message.append("\n  Average PSM/protein pair:")
                .append("\t").append(CommonUtils.formatNanoseconds(Math.round(record.getAverage())));
            summary[1] = total;
        } else summary[1] = "null";
        // organize database operation timing records
        long totalBatchTime = 0L;
        long totalInsertTime = 0L;
        long totalQueryTime = 0L;
        Map<String, Map<String, MassIVESearchTimingRecord>> operationRecords =
            new LinkedHashMap<String, Map<String, MassIVESearchTimingRecord>>();
        for (Entry<String, MassIVESearchTimingRecord> entry :
            dataLoadManager.timingRecords.entrySet()) {
            String type = entry.getKey();
            String operation = null;
            if (type.endsWith("_batch") || type.endsWith("_query"))
                operation = type.substring(0, type.length() - 6);
            else if (type.endsWith("_insert"))
                operation = type.substring(0, type.length() - 7);
            else continue;
            Map<String, MassIVESearchTimingRecord> operationRecord = operationRecords.get(operation);
            if (operationRecord == null)
                operationRecord = new LinkedHashMap<String, MassIVESearchTimingRecord>(4);
            record = entry.getValue();
            if (type.endsWith("_batch")) {
                operationRecord.put("batch", record);
                totalBatchTime += record.getTotal();
            } else if (type.endsWith("_insert")) {
                operationRecord.put("insert", record);
                totalInsertTime += record.getTotal();
            } else if (type.endsWith("_query")) {
                operationRecord.put("query", record);
                totalQueryTime += record.getTotal();
            }
            operationRecords.put(operation, operationRecord);
        }
        // write timing summary for each database operation
        total = CommonUtils.formatNanoseconds(totalBatchTime);
        message.append("\nTotal load file write time:\t").append(total);
        summary[2] = total;
        total = CommonUtils.formatNanoseconds(totalInsertTime);
        message.append("\nTotal data load time:\t").append(total);
        summary[3] = total;
        total = CommonUtils.formatNanoseconds(totalQueryTime);
        message.append("\nTotal follow-up query time:\t").append(total);
        summary[4] = total;
        for (Entry<String, Map<String, MassIVESearchTimingRecord>> entry :
            operationRecords.entrySet()) {
            message.append("\nMassIVE search table [").append(entry.getKey()).append("]:");
            Map<String, MassIVESearchTimingRecord> operationRecord = entry.getValue();
            record = operationRecord.get("batch");
            if (record != null) {
                message.append("\n  Load file write:")
                    .append("\t").append(CommonUtils.formatNanoseconds(record.getTotal()));
                message.append("\t").append(String.format("%,d", record.getTotalBatchSize()))
                    .append(" rows");
            }
            record = operationRecord.get("insert");
            if (record != null) {
                message.append("\n  Data load:")
                    .append("\t").append(CommonUtils.formatNanoseconds(record.getTotal()));
                message.append("\t").append(String.format("%,d", record.getTotalBatchSize()))
                    .append(" rows");
            }
            record = operationRecord.get("query");
            if (record != null) {
                message.append("\n  Follow-up queries:")
                    .append("\t").append(CommonUtils.formatNanoseconds(record.getTotal()));
                message.append("\t").append(String.format("%,d", record.getTotalBatchSize()))
                    .append(" query set values");
            }
        }
        for (int i=0; i<summary.length; i++) {
            if (i == 0)
                message.append("\n");
            else message.append("\t");
            message.append(summary[i]);
        }
        message.append("\n----------");
        System.out.println(message.toString());
    }
}
