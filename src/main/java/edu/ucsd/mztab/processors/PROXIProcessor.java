package edu.ucsd.mztab.processors;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import edu.ucsd.mztab.model.Modification;
import edu.ucsd.mztab.model.MzTabConstants;
import edu.ucsd.mztab.model.MzTabFile;
import edu.ucsd.mztab.model.MzTabProcessor;
import edu.ucsd.mztab.model.MzTabSectionHeader;
import edu.ucsd.mztab.model.MzTabConstants.MzTabSection;
import edu.ucsd.mztab.model.PSM;
import edu.ucsd.mztab.model.MzTabMsRun;
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
	private static final int IMPORT_ATTEMPTS_PER_PSM = 8;
	
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private Connection                        connection;
	private Map<String, Map<String, Integer>> uniqueElements;
	private Map<String, Integer>              rowCounts;
	private PROXIMzTabRecord                  mzTabRecord;
	private MzTabSectionHeader                prtHeader;
	private MzTabSectionHeader                pepHeader;
	private MzTabSectionHeader                psmHeader;
	private boolean                           importByQValue;
	private Integer                           validColumn;
	private Integer                           qValueColumn;
	private Long                              start;
	
	/*========================================================================
	 * Constructor
	 *========================================================================*/
	public PROXIProcessor(
		String taskID, Integer datasetID, boolean importByQValue,
		Connection connection
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
		rowCounts = new HashMap<String, Integer>(3);
		// initialize mzTab file parameters
		prtHeader = null;
		pepHeader = null;
		psmHeader = null;
		qValueColumn = null;
		this.importByQValue = importByQValue;
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
		start = System.currentTimeMillis();
		// initialize database connection for transactions
		try {
			connection.setAutoCommit(false);
		} catch (SQLException error) {
			throw new RuntimeException(error);
		}
		// insert mzTab file into database,
		// populate mzTabFile object with column values
		insertMzTabFile();
		// record all of this mzTab file's referenced spectrum files
		for (Integer msRun : mzTabRecord.mzTabFile.getMsRuns().keySet())
			recordSpectrumFile(mzTabRecord.mzTabFile.getMsRun(msRun));
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
			if (importByQValue) try {
				double qValue = Double.parseDouble(columns[qValueColumn]);
				if (qValue > MzTabConstants.DEFAULT_IMPORT_Q_VALUE_THRESHOLD)
					importable = false;
			} catch (Throwable error) {
				importable = false;
			}
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
					// get clean protein accession
					String cleanedAccession =
						ProteomicsUtils.cleanProteinAccession(protein);
					// get modifications
					Collection<Modification> modifications =
						ProteomicsUtils.getModifications(
							columns[psmHeader.getColumnIndex("modifications")]);
					// retry this PSM import the specified number of times,
					// ignoring early errors since they are assumed to be
					// caused by parallel import race conditions
					Throwable importError = null;
					for (int tries=1; tries<=IMPORT_ATTEMPTS_PER_PSM; tries++) {
						try {
							cascadePSM(
								columns[psmHeader.getColumnIndex("PSM_ID")],
								columns[psmHeader.getColumnIndex(
									"spectra_ref")],
								columns[psmHeader.getColumnIndex("sequence")],
								cleanedAccession,
								columns[psmHeader.getColumnIndex("charge")],
								columns[psmHeader.getColumnIndex(
									"exp_mass_to_charge")],
								modifications);
							connection.commit();
							importError = null;
							break;
						} catch (Throwable error) {
							importError = error;
							try { connection.rollback(); }
							catch (Throwable innerError) {}
							// wait a random amount of time (up to one second)
							// before trying to import this PSM row again
							try { Thread.sleep((long)(Math.random() * 1000)); }
							catch (Throwable innerError) {}
						}
					}
					// if after all retries the PSM import still
					// failed, report the error and move on
					if (importError != null) {
						// log this insertion failure
						incrementRowCount("invalid_PSM");
						// print warning and continue
						System.err.println(String.format(
							"Line %d of mzTab file [%s] is invalid:" +
							"\n----------\n%s\n----------\n%s",
							lineNumber, mzTabFilename, line,
							getRootCause(importError).getMessage()));
						importError.printStackTrace();
						break;
					}
				}
			} else incrementRowCount("unimportable_PSM");
		}
		return line;
	}
	
	public void tearDown() {
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
			long elapsed = System.currentTimeMillis() - start;
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
		private Integer   datasetID;
		
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
				statement.setInt(3, mzTabRecord.datasetID);
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
			// commit result file insertion
			connection.commit();
		} catch (Throwable error) {
			throw new RuntimeException("Error recording resultfile: There " +
				"was an error inserting the resultfile row into the database.",
				error);
		} finally {
			try { statement.close(); } catch (Throwable error) {}
			try { result.close(); } catch (Throwable error) {}
		}
	}
	
	private Integer cascadeProtein(
		String accession, Collection<Modification> modifications
	) {
		Integer proteinID = recordProtein(accession);
		// only process further if this is a legitimate protein
		if (proteinID != null && proteinID > 0) {
			// record all modifications associated with this protein
			if (modifications != null && modifications.isEmpty() == false) {
				for (Modification modification : modifications) {
					Integer modificationID =
						cascadeModification(modification);
					if (modificationID != null && modificationID > 0)
						recordProteinModification(proteinID, modificationID);
				}
			}
		}
		return proteinID;
	}
	
	private Integer cascadePeptide(
		String sequence, String accession,
		Collection<Modification> modifications
	) {
		Integer peptideID = recordPeptide(sequence);
		// only process further if this is a legitimate peptide
		if (peptideID != null && peptideID > 0) {
			// record the protein associated with this peptide
			Integer proteinID = cascadeProtein(accession, modifications);
			if (proteinID != null && proteinID > 0)
				recordPeptideProtein(peptideID, proteinID, sequence);
			// record all modifications associated with this peptide
			if (modifications != null && modifications.isEmpty() == false) {
				for (Modification modification : modifications) {
					Integer modificationID = cascadeModification(modification);
					if (modificationID != null && modificationID > 0)
						recordPeptideModification(peptideID, modificationID);
				}
			}
		}
		return peptideID;
	}
	
	private Integer cascadeVariant(
		String modifiedSequence, String accession, int charge, int peptideID,
		Collection<Modification> modifications
	) {
		Integer variantID = recordVariant(modifiedSequence, charge, peptideID);
		// only process further if this is a legitimate variant
		if (variantID != null && variantID > 0) {
			// record the protein associated with this variant
			Integer proteinID =
				cascadeProtein(accession, modifications);
			if (proteinID != null && proteinID > 0)
				recordVariantProtein(variantID, proteinID);
			// record all modifications associated with this variant
			if (modifications != null && modifications.isEmpty() == false) {
				for (Modification modification : modifications) {
					Integer modificationID = cascadeModification(modification);
					Integer location = modification.getPosition();
					if (modificationID != null && modificationID > 0 &&
						location != null)
						recordVariantModification(
							variantID, modificationID, location);
				}
			}
		}
		return variantID;
	}
	
	private Integer cascadeModification(Modification modification) {
		Integer modificationID =
			recordModification(modification.getName(), modification.getMass());
		return modificationID;
	}
	
	private Integer cascadePSM(
		String psmFileID, String spectraRef, String sequence,
		String accession, String charge, String massToCharge,
		Collection<Modification> modifications
	) {
		PSM psm = new PSM(psmFileID, spectraRef, sequence,
			charge, massToCharge, modifications);
		Integer spectrumFileID = null;
		Integer peptideID = null;
		Integer variantID = null;
		Integer psmID = null;
		Integer proteinID = null;
		Integer modificationID = null;
		try {
			// retrieve and record this PSM's spectrum file
			int msRun = psm.getMsRun();
			MzTabMsRun spectrumFile =
				mzTabRecord.mzTabFile.getMsRun(msRun);
			if (spectrumFile == null)
				throw new NullPointerException(String.format(
					"No spectrum file could be found " +
					"for ms_run[%d] of mzTab file [%s].",
					msRun, mzTabRecord.mzTabFile.getMzTabFilename()));
			spectrumFileID = recordSpectrumFile(spectrumFile);
			if (spectrumFileID == null || spectrumFileID <= 0)
				throw new IllegalArgumentException(String.format(
					"Spectrum file ms_run[%d] [%s] could not be recorded.",
					msRun, spectrumFile.getMsRunLocation()));
			// be sure this PSM's peptide and variant have been recorded
			String peptide = psm.getSequence();
			peptideID = cascadePeptide(peptide, accession, modifications);
			if (peptideID == null || peptideID <= 0)
				throw new IllegalArgumentException(String.format(
					"Peptide sequence [%s] is invalid.", peptide));
			String variant = psm.getModifiedSequence();
			variantID = cascadeVariant(
				variant, accession, psm.getCharge(), peptideID, modifications);
			if (variantID == null || variantID <= 0)
				throw new IllegalArgumentException(String.format(
					"Variant peptide sequence [%s] is invalid.", variant));
			psmID = recordPSM(psm, spectrumFileID, peptideID, variantID);
			// only process further if this is a legitimate psm
			if (psmID != null && psmID > 0) {
				// record the protein associated with this psm
				proteinID = cascadeProtein(accession, modifications);
				if (proteinID != null && proteinID > 0)
					recordPSMProtein(psmID, proteinID);
				// record all modifications associated with this psm
				if (modifications != null && modifications.isEmpty() == false) {
					for (Modification modification : modifications) {
						modificationID = cascadeModification(modification);
						if (modificationID != null && modificationID > 0)
							recordPSMModification(psmID, modificationID);
					}
				}
			}
			return psmID;
		} catch (Throwable error) {
			System.err.println(String.format("Could not cascade PSM %d:" +
				"\n\tspectrum file ID = %s" +
				"\n\tpeptide ID = %s" +
				"\n\tvariant ID = %s" +
				"\n\tPSM ID = %s" +
				"\n\tprotein ID = %s" +
				"\n\tmodification ID = %s",
				psm.getID(),
				spectrumFileID != null ? spectrumFileID.toString() : "null",
				peptideID != null ? peptideID.toString() : "null",
				variantID != null ? variantID.toString() : "null",
				psmID != null ? psmID.toString() : "null",
				proteinID != null ? proteinID.toString() : "null",
				modificationID != null ? modificationID.toString() : "null"));
			if (error instanceof RuntimeException)
				throw (RuntimeException)error;
			else throw new RuntimeException(error);
		}
	}
	
	private Integer recordSpectrumFile(MzTabMsRun spectrumFile) {
		if (spectrumFile == null)
			return null;
		// get unique spectrum file descriptor
		String descriptor = spectrumFile.getDescriptor();
		// first check to see if this spectrum file has already been recorded
		Integer spectrumFileID = getElementID("spectrumFile", descriptor);
		if (spectrumFileID != null)
			return spectrumFileID;
		// then write spectrum file to the database
		PreparedStatement statement = null;
		ResultSet result = null;
		try {
			StringBuilder sql = new StringBuilder(
				"INSERT IGNORE INTO proxi.spectrumfiles " +
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
				statement.setInt(3, mzTabRecord.datasetID);
			int insertion = statement.executeUpdate();
			// if the row already exists, need to look it up manually to get ID
			if (insertion == 0) {
				try { statement.close(); } catch (Throwable error) {}
				statement = connection.prepareStatement(
					"SELECT id FROM proxi.spectrumfiles " +
					"WHERE file_descriptor=?");
				statement.setString(1, descriptor);
				result = statement.executeQuery();
				if (result.next())
					spectrumFileID = result.getInt(1);
				else throw new RuntimeException(String.format(
					"No spectrumfile row was found for descriptor [%s] " +
					"even though the previous insert was ignored.",
					descriptor));
			}
			// if the insert succeeded, get its generated row ID
			else if (insertion == 1) {
				result = statement.getGeneratedKeys();
				if (result.next())
					spectrumFileID = result.getInt(1);
				else throw new RuntimeException("The spectrumfile insert " +
					"statement did not generate a row ID.");
			}
			else throw new RuntimeException(String.format(
				"The spectrumfile insert statement returned a value of \"%d\".",
				insertion));
			// commit spectrum file insertion, since spectrum files might be
			// shared across multiple concurrent import operations, so we
			// don't want to try to roll back this insertion if something goes
			// wrong with some but not all of those imports
			connection.commit();
		} catch (Throwable error) {
			throw new RuntimeException("Error recording spectrumfile: There " +
				"was an error inserting the spectrumfile row into the " +
				"database.", error);
		} finally {
			try { statement.close(); } catch (Throwable error) {}
			try { result.close(); } catch (Throwable error) {}
		}
		// add this spectrum file to the set of recorded spectrumfiles
		addElement("spectrumFile", descriptor, spectrumFileID);
		return spectrumFileID;
	}
	
	private Integer recordPSM(
		PSM psm, int spectrumFileID, int peptideID, int variantID
	) {
		if (psm == null)
			return null;
		// first check to see if this psm has already been recorded
		Integer psmID = getElementID("psm", psm.getID().toString());
		if (psmID != null)
			return psmID;
		// then write psm to the database
		PreparedStatement statement = null;
		ResultSet result = null;
		try {
			StringBuilder sql = new StringBuilder(
				"INSERT IGNORE INTO proxi.psms " +
				"(id_in_file, nativeid, variant_sequence, " +
				"charge, exp_mass_to_charge, " +
				"resultfile_id, spectrumfile_id, peptide_id, variant_id");
			if (mzTabRecord.datasetID != null)
				sql.append(", dataset_id");
			sql.append(") VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?");
			if (mzTabRecord.datasetID != null)
				sql.append(", ?");
			sql.append(")");
			statement = connection.prepareStatement(
				sql.toString(), Statement.RETURN_GENERATED_KEYS);
			statement.setInt(1, psm.getID());
			statement.setString(2, psm.getNativeID());
			statement.setString(3, psm.getModifiedSequence());
			statement.setInt(4, psm.getCharge());
			Double massToCharge = psm.getMassToCharge();
			if (massToCharge == null)
				statement.setNull(5, Types.DOUBLE);
			else statement.setDouble(5, massToCharge);
			statement.setInt(6, mzTabRecord.id);
			statement.setInt(7, spectrumFileID);
			statement.setInt(8, peptideID);
			statement.setInt(9, variantID);
			if (mzTabRecord.datasetID != null)
				statement.setInt(10, mzTabRecord.datasetID);
			int insertion = statement.executeUpdate();
			// if the row already exists, need to look it up manually to get ID
			if (insertion == 0) {
				try { statement.close(); } catch (Throwable error) {}
				statement = connection.prepareStatement(
					"SELECT id FROM proxi.psms " +
					"WHERE resultfile_id=? AND id_in_file=?");
				statement.setInt(1, mzTabRecord.id);
				statement.setInt(2, psm.getID());
				result = statement.executeQuery();
				if (result.next())
					psmID = result.getInt(1);
				else throw new RuntimeException(String.format(
					"No psm row was found for psm [%s] even though " +
					"the previous insert was ignored.", psm.toString()));
			}
			// if the insert succeeded, get its generated row ID
			else if (insertion == 1) {
				result = statement.getGeneratedKeys();
				if (result.next())
					psmID = result.getInt(1);
				else throw new RuntimeException(
					"The psm insert statement did not generate a row ID.");
			}
			else throw new RuntimeException(String.format(
				"The psm insert statement returned a value of \"%d\".",
				insertion));
		} catch (Throwable error) {
			throw new RuntimeException("Error recording psm: There " +
				"was an error inserting the psm row into the database.",
				error);
		} finally {
			try { statement.close(); } catch (Throwable error) {}
			try { result.close(); } catch (Throwable error) {}
		}
		// validate PSM ID
		if (psmID == null)
			throw new RuntimeException(String.format(
				"No valid row ID could be obtained for PSM %d " +
				"of result file %d.", psm.getID(), mzTabRecord.id));
		// add this psm to the set of recorded psms
		else addElement("psm", psm.getID().toString(), psmID);
		return peptideID;
	}
	
	private void recordPSMProtein(int psmID, int proteinID) {
		// write psm/protein link to the database
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(
				"INSERT IGNORE INTO proxi.psm_proteins " +
				"(psm_id, protein_id) VALUES(?, ?)");
			statement.setInt(1, psmID);
			statement.setInt(2, proteinID);
			int insertion = statement.executeUpdate();
			if (insertion != 0 && insertion != 1)
				throw new RuntimeException(String.format(
					"The psm_protein insert statement " +
					"returned a value of \"%d\".", insertion));
		} catch (Throwable error) {
			throw new RuntimeException(String.format("Error recording " +
				"psm_protein (psm_id %d, protein_id %d): There was an error " +
				"inserting the psm_protein row into the database.",
				psmID, proteinID), error);
		} finally {
			try { statement.close(); } catch (Throwable error) {}
		}
	}
	
	private void recordPSMModification(int psmID, int modificationID) {
		// write psm/modification link to the database
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(
				"INSERT IGNORE INTO proxi.psm_modifications " +
				"(psm_id, modification_id) VALUES(?, ?)");
			statement.setInt(1, psmID);
			statement.setInt(2, modificationID);
			int insertion = statement.executeUpdate();
			if (insertion != 0 && insertion != 1)
				throw new RuntimeException(String.format(
					"The psm_modification insert statement " +
					"returned a value of \"%d\".", insertion));
		} catch (Throwable error) {
			throw new RuntimeException("Error recording " +
				"psm_modification: There was an error inserting the " +
				"psm_modification row into the database.", error);
		} finally {
			try { statement.close(); } catch (Throwable error) {}
		}
	}
	
	private Integer recordPeptide(String sequence) {
		if (sequence == null)
			return null;
		// first check to see if this peptide has already been recorded
		Integer peptideID = getElementID("sequence", sequence);
		if (peptideID != null)
			return peptideID;
		// otherwise, write peptide to the database
		PreparedStatement statement = null;
		ResultSet result = null;
		try {
			statement = connection.prepareStatement(
				"INSERT IGNORE INTO proxi.peptides (sequence) VALUES(?)",
				Statement.RETURN_GENERATED_KEYS);
			statement.setString(1, sequence);
			int insertion = statement.executeUpdate();
			// if the row already exists, need to look it up manually to get ID
			if (insertion == 0) {
				try { statement.close(); } catch (Throwable error) {}
				statement = connection.prepareStatement(
					"SELECT id FROM proxi.peptides WHERE sequence=?");
				statement.setString(1, sequence);
				result = statement.executeQuery();
				if (result.next())
					peptideID = result.getInt(1);
				else throw new RuntimeException(String.format(
					"No peptide row was found for sequence [%s] " +
					"even though the previous insert was ignored.", sequence));
			}
			// if the insert succeeded, get its generated row ID
			else if (insertion == 1) {
				result = statement.getGeneratedKeys();
				if (result.next())
					peptideID = result.getInt(1);
				else throw new RuntimeException(
					"The peptide insert statement did not generate a row ID.");
			}
			else throw new RuntimeException(String.format(
				"The peptide insert statement returned a value of \"%d\".",
				insertion));
		} catch (Throwable error) {
			throw new RuntimeException("Error recording peptide: There " +
				"was an error inserting the peptide row into the database.",
				error);
		} finally {
			try { statement.close(); } catch (Throwable error) {}
			try { result.close(); } catch (Throwable error) {}
		}
		// validate peptide ID
		if (peptideID == null)
			throw new RuntimeException(String.format(
				"No valid row ID could be obtained for peptide [%s].",
				sequence));
		// write resultfile/peptide link to the database
		else try {
			statement = connection.prepareStatement(
				"INSERT IGNORE INTO proxi.resultfile_peptides " +
				"(sequence, resultfile_id, peptide_id) VALUES(?, ?, ?)");
			statement.setString(1, sequence);
			statement.setInt(2, mzTabRecord.id);
			statement.setInt(3, peptideID);
			int insertion = statement.executeUpdate();
			if (insertion != 0 && insertion != 1)
				throw new RuntimeException(String.format(
					"The resultfile_peptide insert statement " +
					"returned a value of \"%d\".", insertion));
		} catch (Throwable error) {
			throw new RuntimeException("Error recording resultfile_peptide: " +
				"There was an error inserting the resultfile_peptide row " +
				"into the database.", error);
		} finally {
			try { statement.close(); } catch (Throwable error) {}
		}
		// if this is a dataset file, write dataset/peptide link to the database
		if (mzTabRecord.datasetID != null) try {
			statement = connection.prepareStatement(
				"INSERT IGNORE INTO proxi.dataset_peptides " +
				"(sequence, dataset_id, peptide_id) VALUES(?, ?, ?)");
			statement.setString(1, sequence);
			statement.setInt(2, mzTabRecord.datasetID);
			statement.setInt(3, peptideID);
			int insertion = statement.executeUpdate();
			if (insertion != 0 && insertion != 1)
				throw new RuntimeException(String.format(
					"The dataset_peptide insert statement " +
					"returned a value of \"%d\".", insertion));
		} catch (Throwable error) {
			throw new RuntimeException("Error recording dataset_peptide: " +
				"There was an error inserting the dataset_peptide row " +
				"into the database.", error);
		} finally {
			try { statement.close(); } catch (Throwable error) {}
		}
		// add this peptide to the set of recorded peptides
		addElement("sequence", sequence, peptideID);
		return peptideID;
	}
	
	private void recordPeptideProtein(
		int peptideID, int proteinID, String sequence
	) {
		if (sequence == null)
			throw new NullPointerException(
				"Argument peptide sequence is null.");
		// write peptide/protein link to the database
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(
				"INSERT IGNORE INTO proxi.peptide_proteins " +
				"(peptide_id, protein_id, sequence) VALUES(?, ?, ?)");
			statement.setInt(1, peptideID);
			statement.setInt(2, proteinID);
			statement.setString(3, sequence);
			int insertion = statement.executeUpdate();
			if (insertion != 0 && insertion != 1)
				throw new RuntimeException(String.format(
					"The peptide_protein insert statement " +
					"returned a value of \"%d\".", insertion));
		} catch (Throwable error) {
			throw new RuntimeException("Error recording " +
				"peptide_protein: There was an error inserting the " +
				"peptide_protein row into the database.", error);
		} finally {
			try { statement.close(); } catch (Throwable error) {}
		}
	}
	
	private void recordPeptideModification(int peptideID, int modificationID) {
		// write peptide/modification link to the database
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(
				"INSERT IGNORE INTO proxi.peptide_modifications " +
				"(peptide_id, modification_id) VALUES(?, ?)");
			statement.setInt(1, peptideID);
			statement.setInt(2, modificationID);
			int insertion = statement.executeUpdate();
			if (insertion != 0 && insertion != 1)
				throw new RuntimeException(String.format(
					"The peptide_modification insert statement " +
					"returned a value of \"%d\".", insertion));
		} catch (Throwable error) {
			throw new RuntimeException("Error recording " +
				"peptide_modification: There was an error inserting the " +
				"peptide_modification row into the database.", error);
		} finally {
			try { statement.close(); } catch (Throwable error) {}
		}
	}
	
	private Integer recordVariant(String sequence, int charge, int peptideID) {
		if (sequence == null)
			return null;
		// first check to see if this variant has already been recorded
		Integer variantID = getElementID("variant", sequence);
		if (variantID != null)
			return variantID;
		// otherwise, write variant to the database
		PreparedStatement statement = null;
		ResultSet result = null;
		try {
			statement = connection.prepareStatement(
				"INSERT IGNORE INTO proxi.variants " +
				"(sequence, charge, peptide_id) VALUES(?, ?, ?)",
				Statement.RETURN_GENERATED_KEYS);
			statement.setString(1, sequence);
			statement.setInt(2, charge);
			statement.setInt(3, peptideID);
			int insertion = statement.executeUpdate();
			// if the row already exists, need to look it up manually to get ID
			if (insertion == 0) {
				try { statement.close(); } catch (Throwable error) {}
				statement = connection.prepareStatement(
					"SELECT id FROM proxi.variants WHERE sequence=?");
				statement.setString(1, sequence);
				result = statement.executeQuery();
				if (result.next())
					variantID = result.getInt(1);
				else throw new RuntimeException(String.format(
					"No variant row was found for sequence [%s] " +
					"even though the previous insert was ignored.", sequence));
			}
			// if the insert succeeded, get its generated row ID
			else if (insertion == 1) {
				result = statement.getGeneratedKeys();
				if (result.next())
					variantID = result.getInt(1);
				else throw new RuntimeException(
					"The variant insert statement did not generate a row ID.");
			}
			else throw new RuntimeException(String.format(
				"The variant insert statement returned a value of \"%d\".",
				insertion));
		} catch (Throwable error) {
			throw new RuntimeException("Error recording variant: There " +
				"was an error inserting the variant row into the database.",
				error);
		} finally {
			try { statement.close(); } catch (Throwable error) {}
			try { result.close(); } catch (Throwable error) {}
		}
		// validate variant ID
		if (variantID == null)
			throw new RuntimeException(String.format(
				"No valid row ID could be obtained for variant [%s].",
				sequence));
		// write resultfile/variant link to the database
		else try {
			statement = connection.prepareStatement(
				"INSERT IGNORE INTO proxi.resultfile_variants " +
				"(sequence, resultfile_id, variant_id) VALUES(?, ?, ?)");
			statement.setString(1, sequence);
			statement.setInt(2, mzTabRecord.id);
			statement.setInt(3, variantID);
			int insertion = statement.executeUpdate();
			if (insertion != 0 && insertion != 1)
				throw new RuntimeException(String.format(
					"The resultfile_variant insert statement " +
					"returned a value of \"%d\".", insertion));
		} catch (Throwable error) {
			throw new RuntimeException("Error recording resultfile_variant: " +
				"There was an error inserting the resultfile_variant row " +
				"into the database.", error);
		} finally {
			try { statement.close(); } catch (Throwable error) {}
		}
		// if this is a dataset file, write dataset/variant link to the database
		if (mzTabRecord.datasetID != null) try {
			statement = connection.prepareStatement(
				"INSERT IGNORE INTO proxi.dataset_variants " +
				"(sequence, dataset_id, variant_id) VALUES(?, ?, ?)");
			statement.setString(1, sequence);
			statement.setInt(2, mzTabRecord.datasetID);
			statement.setInt(3, variantID);
			int insertion = statement.executeUpdate();
			if (insertion != 0 && insertion != 1)
				throw new RuntimeException(String.format(
					"The dataset_variant insert statement " +
					"returned a value of \"%d\".", insertion));
		} catch (Throwable error) {
			throw new RuntimeException("Error recording dataset_variant: " +
				"There was an error inserting the dataset_variant row " +
				"into the database.", error);
		} finally {
			try { statement.close(); } catch (Throwable error) {}
		}
		// add this variant to the set of recorded variants
		addElement(
			"variant", String.format("%s_%d", sequence, charge), variantID);
		return variantID;
	}
	
	private void recordVariantProtein(int variantID, int proteinID) {
		// write variant/protein link to the database
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(
				"INSERT IGNORE INTO proxi.variant_proteins " +
				"(variant_id, protein_id) VALUES(?, ?)");
			statement.setInt(1, variantID);
			statement.setInt(2, proteinID);
			int insertion = statement.executeUpdate();
			if (insertion != 0 && insertion != 1)
				throw new RuntimeException(String.format(
					"The variant_protein insert statement " +
					"returned a value of \"%d\".", insertion));
		} catch (Throwable error) {
			throw new RuntimeException("Error recording " +
				"variant_protein: There was an error inserting the " +
				"variant_protein row into the database.", error);
		} finally {
			try { statement.close(); } catch (Throwable error) {}
		}
	}
	
	private void recordVariantModification(
		int variantID, int modificationID, int position
	) {
		// write variant/modification link to the database
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(
				"INSERT IGNORE INTO proxi.variant_modifications " +
				"(variant_id, modification_id, location) VALUES(?, ?, ?)");
			statement.setInt(1, variantID);
			statement.setInt(2, modificationID);
			statement.setInt(3, position);
			int insertion = statement.executeUpdate();
			if (insertion != 0 && insertion != 1)
				throw new RuntimeException(String.format(
					"The variant_modification insert statement " +
					"returned a value of \"%d\".", insertion));
		} catch (Throwable error) {
			throw new RuntimeException("Error recording " +
				"variant_modification: There was an error inserting the " +
				"variant_modification row into the database.", error);
		} finally {
			try { statement.close(); } catch (Throwable error) {}
		}
	}
	
	private Integer recordProtein(String accession) {
		if (accession == null)
			return null;
		// first check to see if this protein has already been recorded
		Integer proteinID = getElementID("accession", accession);
		if (proteinID != null)
			return proteinID;
		// otherwise, write protein to the database
		PreparedStatement statement = null;
		ResultSet result = null;
		try {
			statement = connection.prepareStatement(
				"INSERT IGNORE INTO proxi.proteins (name) VALUES(?)",
				Statement.RETURN_GENERATED_KEYS);
			statement.setString(1, accession);
			int insertion = statement.executeUpdate();
			// if the row already exists, need to look it up manually to get ID
			if (insertion == 0) {
				try { statement.close(); } catch (Throwable error) {}
				statement = connection.prepareStatement(
					"SELECT id FROM proxi.proteins WHERE name=?");
				statement.setString(1, accession);
				result = statement.executeQuery();
				if (result.next())
					proteinID = result.getInt(1);
				else throw new RuntimeException(String.format(
					"No protein row was found for accession [%s] " +
					"even though the previous insert was ignored.", accession));
			}
			// if the insert succeeded, get its generated row ID
			else if (insertion == 1) {
				result = statement.getGeneratedKeys();
				if (result.next())
					proteinID = result.getInt(1);
				else throw new RuntimeException(
					"The protein insert statement did not generate a row ID.");
			}
			else throw new RuntimeException(String.format(
				"The protein insert statement returned a value of \"%d\".",
				insertion));
		} catch (Throwable error) {
			throw new RuntimeException("Error recording protein: There " +
				"was an error inserting the protein row into the database.",
				error);
		} finally {
			try { statement.close(); } catch (Throwable error) {}
			try { result.close(); } catch (Throwable error) {}
		}
		// validate protein ID
		if (proteinID == null)
			throw new RuntimeException(String.format(
				"No valid row ID could be obtained for protein [%s].",
				accession));
		// write resultfile/protein link to the database
		else try {
			statement = connection.prepareStatement(
				"INSERT IGNORE INTO proxi.resultfile_proteins " +
				"(resultfile_id, protein_id) VALUES(?, ?)");
			statement.setInt(1, mzTabRecord.id);
			statement.setInt(2, proteinID);
			int insertion = statement.executeUpdate();
			if (insertion != 0 && insertion != 1)
				throw new RuntimeException(String.format(
					"The resultfile_protein insert statement " +
					"returned a value of \"%d\".", insertion));
		} catch (Throwable error) {
			throw new RuntimeException("Error recording resultfile_protein: " +
				"There was an error inserting the resultfile_protein row " +
				"into the database.", error);
		} finally {
			try { statement.close(); } catch (Throwable error) {}
		}
		// if this is a dataset file, write dataset/protein link to the database
		if (mzTabRecord.datasetID != null) try {
			statement = connection.prepareStatement(
				"INSERT IGNORE INTO proxi.dataset_proteins " +
				"(dataset_id, protein_id) VALUES(?, ?)");
			statement.setInt(1, mzTabRecord.datasetID);
			statement.setInt(2, proteinID);
			int insertion = statement.executeUpdate();
			if (insertion != 0 && insertion != 1)
				throw new RuntimeException(String.format(
					"The dataset_protein insert statement " +
					"returned a value of \"%d\".", insertion));
		} catch (Throwable error) {
			throw new RuntimeException("Error recording dataset_protein: " +
				"There was an error inserting the dataset_protein row " +
				"into the database.", error);
		} finally {
			try { statement.close(); } catch (Throwable error) {}
		}
		// add this protein to the set of recorded proteins
		addElement("accession", accession, proteinID);
		return proteinID;
	}
	
	private void recordProteinModification(int proteinID, int modificationID) {
		// write protein/modification link to the database
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(
				"INSERT IGNORE INTO proxi.protein_modifications " +
				"(protein_id, modification_id) VALUES(?, ?)");
			statement.setInt(1, proteinID);
			statement.setInt(2, modificationID);
			int insertion = statement.executeUpdate();
			if (insertion != 0 && insertion != 1)
				throw new RuntimeException(String.format(
					"The protein_modification insert statement " +
					"returned a value of \"%d\".", insertion));
		} catch (Throwable error) {
			throw new RuntimeException("Error recording " +
				"protein_modification: There was an error inserting the " +
				"protein_modification row into the database.", error);
		} finally {
			try { statement.close(); } catch (Throwable error) {}
		}
	}
	
	private Integer recordModification(String name, Double mass) {
		if (name == null)
			return null;
		// first check to see if this modification has already been recorded
		Integer modificationID = getElementID("modification", name);
		if (modificationID != null)
			return modificationID;
		// otherwise, write modification to the database
		PreparedStatement statement = null;
		ResultSet result = null;
		try {
			StringBuffer sql = new StringBuffer(
				"INSERT IGNORE INTO proxi.modifications (name");
			if (mass != null)
				sql.append(", mass");
			sql.append(") VALUES(?");
			if (mass != null)
				sql.append(", ?");
			sql.append(")");
			statement = connection.prepareStatement(
				sql.toString(), Statement.RETURN_GENERATED_KEYS);
			statement.setString(1, name);
			if (mass != null)
				statement.setDouble(2, mass);
			int insertion = statement.executeUpdate();
			// if the row already exists, need to look it up manually to get ID
			if (insertion == 0) {
				try { statement.close(); } catch (Throwable error) {}
				statement = connection.prepareStatement(
					"SELECT id FROM proxi.modifications WHERE name=?");
				statement.setString(1, name);
				result = statement.executeQuery();
				if (result.next())
					modificationID = result.getInt(1);
				else throw new RuntimeException(String.format(
					"No modification row was found for name [%s] " +
					"even though the previous insert was ignored.", name));
			}
			// if the insert succeeded, get its generated row ID
			else if (insertion == 1) {
				result = statement.getGeneratedKeys();
				if (result.next())
					modificationID = result.getInt(1);
				else throw new RuntimeException("The modification insert " +
					"statement did not generate a row ID.");
			}
			else throw new RuntimeException(String.format(
				"The modification insert statement returned a value of \"%d\".",
				insertion));
		} catch (Throwable error) {
			throw new RuntimeException("Error recording modification: There " +
				"was an error inserting the modification row into the " +
				"database.", error);
		} finally {
			try { statement.close(); } catch (Throwable error) {}
			try { result.close(); } catch (Throwable error) {}
		}
		// validate modification ID
		if (modificationID == null)
			throw new RuntimeException(String.format(
				"No valid row ID could be obtained for modification [%s].",
				name));
		// write resultfile/modification link to the database
		else try {
			statement = connection.prepareStatement(
				"INSERT IGNORE INTO proxi.resultfile_modifications " +
				"(resultfile_id, modification_id) VALUES(?, ?)");
			statement.setInt(1, mzTabRecord.id);
			statement.setInt(2, modificationID);
			int insertion = statement.executeUpdate();
			if (insertion != 0 && insertion != 1)
				throw new RuntimeException(String.format(
					"The resultfile_modification insert statement " +
					"returned a value of \"%d\".", insertion));
		} catch (Throwable error) {
			throw new RuntimeException("Error recording " +
				"resultfile_modification: There was an error inserting the " +
				"resultfile_modification row into the database.", error);
		} finally {
			try { statement.close(); } catch (Throwable error) {}
		}
		// if this is a dataset file, write dataset/modification
		// link to the database
		if (mzTabRecord.datasetID != null) try {
			statement = connection.prepareStatement(
				"INSERT IGNORE INTO proxi.dataset_modifications " +
				"(dataset_id, modification_id) VALUES(?, ?)");
			statement.setInt(1, mzTabRecord.datasetID);
			statement.setInt(2, modificationID);
			int insertion = statement.executeUpdate();
			if (insertion != 0 && insertion != 1)
				throw new RuntimeException(String.format(
					"The dataset_modification insert statement " +
					"returned a value of \"%d\".", insertion));
		} catch (Throwable error) {
			throw new RuntimeException("Error recording " +
				"dataset_modification: There was an error inserting the " +
				"dataset_modification row into the database.", error);
		} finally {
			try { statement.close(); } catch (Throwable error) {}
		}
		// add this modification to the set of recorded modifications
		addElement("modification", name, modificationID);
		return modificationID;
	}
	
	private Integer getElementID(String type, String value) {
		if (type == null || value == null ||
			value.trim().equalsIgnoreCase("null"))
			return 0;
		Map<String, Integer> values = uniqueElements.get(type);
		if (values == null || values.containsKey(value) == false)
			return null;
		else return values.get(value);
	}
	
	private void addElement(String type, String value, int id) {
		if (type == null || value == null ||
			value.trim().equalsIgnoreCase("null"))
			return;
		Map<String, Integer> values = uniqueElements.get(type);
		if (values == null)
			values = new HashMap<String, Integer>();
		values.put(value, id);
		uniqueElements.put(type, values);
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
	
	private Throwable getRootCause(Throwable error) {
		if (error == null)
			return null;
		Throwable cause = error.getCause();
		if (cause == null)
			return error;
		else return getRootCause(cause);
	}
}
