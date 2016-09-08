package edu.ucsd.mztab.model;

import java.util.regex.Pattern;

public class MzTabConstants
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	// constants pertaining to mzTab file structure
	public static enum MzTabSection { MTD, PRT, PEP, PSM, SML }
	
	// constants pertaining to important MTD section fields
	public static final String FDR_MTD_FIELD = "false_discovery_rate";
	public static final Pattern FDR_LINE_PATTERN = Pattern.compile(
		"^MTD\\s+" + FDR_MTD_FIELD + "\\s+(.+)$");
	public static final Pattern PSM_SEARCH_ENGINE_SCORE_LINE_PATTERN =
		Pattern.compile("^MTD\\s+psm_search_engine_score\\[(\\d+)\\]\\s+(.+)$");
	public static final Pattern FILE_LINE_PATTERN = Pattern.compile(
		"^MTD\\s+ms_run\\[(\\d+)\\]-location\\s+(.+)$");
	
	// constants pertaining to important PSM section header (PSH) columns
	public static final String PSH_PSM_ID_COLUMN = "PSM_ID";
	public static final String PSH_PEPTIDE_COLUMN = "sequence";
	public static final String PSH_PROTEIN_COLUMN = "accession";
	public static final String PSH_SPECTRA_REF_COLUMN = "spectra_ref";
	
	// constants pertaining to important PRT section header (PRH) columns
	public static final String PRH_PROTEIN_COLUMN = "accession";
	public static final String PRH_MODIFICATIONS_COLUMN = "modifications";
	
	// constants pertaining to important PEP section header (PEH) columns
	public static final String PEH_PEPTIDE_COLUMN = "sequence";
	public static final String PEH_PROTEIN_COLUMN = "accession";
	public static final String PEH_MODIFICATIONS_COLUMN = "modifications";
	
	// constants pertaining to mzTab "spectra_ref" column values,
	// as seen in PSM rows
	public static final Pattern SPECTRA_REF_PATTERN = Pattern.compile(
		"ms_run\\[(\\d+)\\]:(.+)");
	public static final Pattern SCAN_PATTERN = Pattern.compile("scan=(\\d+)");
	public static final Pattern INDEX_PATTERN = Pattern.compile("index=(\\d+)");
	public static final Pattern QUERY_PATTERN = Pattern.compile("query=(\\d+)");
	public static final Pattern FILE_PATTERN = Pattern.compile("file=(.+)");
	
	// constants pertaining to mzTab "modifications" column values,
	// as seen in PRT, PEP and PSM rows
	public static final Pattern MZTAB_MODIFICATION_PATTERN = Pattern.compile(
		"\\s*(?:(\\d+.*?(?:\\|\\d+.*?)*)-)?" +						// position
		"((?:\\[[^,]*,\\s*[^,]*,\\s*\"?[^\"]*\"?,\\s*[^,]*\\])|" +	// CV tuple
		"(?:[^,]+))\\s*");											// mod ID
	public static final Pattern MZTAB_POSITION_PATTERN = Pattern.compile(
		"(\\d+)([^\\|]*)?\\|?");
//	public static final Pattern MZTAB_POSITION_PATTERN = Pattern.compile(
//		"^(?:(\\d+)" +
//		"(\\[[^,]*,\\s*[^,]*,\\s*\"?[^\"]*\"?,\\s*[^,]*\\])?" +		// CV tuple
//		")+(?:\\|(\\d+)" +
//		"(\\[[^,]*,\\s*[^,]*,\\s*\"?[^\"]*\"?,\\s*[^,]*\\])?)*$");	// CV tuple
	public static final Pattern MZTAB_CHEMMOD_PATTERN = Pattern.compile(
		"^CHEMMOD:(.*)$");
	public static final String MZTAB_MODIFICATION_STRING_FORMAT =
		"{position}{Parameter}-{Modification or Substitution identifier}" +
		"|{neutral loss}";
	
	// constants pertaining to CCMS-controlled mzTab PSM row validity fields
	public static final String VALID_COLUMN = "opt_global_valid";
	public static final String INVALID_REASON_COLUMN =
		"opt_global_invalid_reason";
	
	// constants pertaining to CCMS-controlled mzTab FDR fields
	public static enum FDRType { PSM, PEPTIDE, PROTEIN }
	public static final String PASS_THRESHOLD_COLUMN =
		"opt_global_pass_threshold";
	public static final String IS_DECOY_COLUMN =
		"opt_global_cv_MS:1002217_decoy_peptide";
	public static final String Q_VALUE_COLUMN =
		"opt_global_cv_MS:1002354_PSM-level_q-value";
	public static final String GLOBAL_PSM_FDR_TERM =
		"[MS, MS:1002350, PSM-level global FDR, %s]";
	public static final String GLOBAL_PEPTIDE_FDR_TERM =
		"[MS, MS:1001364, peptide sequence-level global FDR, %s]";
	public static final String GLOBAL_PROTEIN_FDR_TERM =
		"[MS, MS:1001214, protein-level global FDR, %s]";
	
	// constants pertaining to known optional column values
	public static final String[] KNOWN_QVALUE_COLUMNS = new String[]{
		"QValue", "MS-GF:QValue"
	};
	
	// constants pertaining to peptide strings
	public static final String AMINO_ACID_CHARSET = "ARDNCEQGHILKMFPSTWYV";
	public static final Pattern AMINO_ACID_PATTERN = Pattern.compile(
		String.format("^[%s]$", AMINO_ACID_CHARSET));
	public static final Pattern PEPTIDE_STRING_PATTERN = Pattern.compile(
		String.format("^[%s]+$", AMINO_ACID_CHARSET));
	
	// constants pertaining to general CV term syntax
	public static final Pattern CV_TERM_PATTERN = Pattern.compile(
		"^\\[" +
		"([^,]*),\\s*" +						// ontology identifier
		"([^,]*),\\s*" +						// CV term accession
		"((?:\"[^\"]*\")|(?:[^,]*)),\\s*" +		// CV term name
		"((?:\"[^\"]*\")|(?:[^,]*))" +			// CV term "value"
		"\\]$");
	public static final Pattern CV_ACCESSION_PATTERN = Pattern.compile(
		"^(.*?:\\d*)$");
	
	// general constants
	public static final Pattern FILE_URI_PROTOCOL_PATTERN =
		Pattern.compile("file:(?:[/]{2})?(.*)");
	public static final Pattern FLOAT_PATTERN = Pattern.compile(
		"[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?");
	public static final String SIMPLE_FLOAT_PATTERN_STRING = 
		"((?:\\+?-?\\d+\\.?\\d*)|(?:\\+?-?\\d*\\.?\\d+))";
	public static final Pattern SIMPLE_FLOAT_PATTERN = Pattern.compile(
		SIMPLE_FLOAT_PATTERN_STRING);
}
