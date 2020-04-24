package edu.ucsd.mztab.model;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.commons.lang3.ArrayUtils;

public class MzTabConstants
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	// constants pertaining to general CV term syntax
	public static final String NON_MATCHING_CV_TERM =
		"\\[" +									// opening bracket
		"(?:[^,]*),\\s*" +						// ontology identifier
		"(?:[^,]*),\\s*" +						// CV term accession
		"(?:(?:\"[^\"]*\")|(?:[^,]*)),\\s*" +		// CV term name
		"(?:(?:\"[^\"]*\")|(?:[^,]*))" +			// CV term "value"
		"\\]";									// closing bracket
	public static final String MATCHING_CV_TERM =
		"\\[" +									// opening bracket
		"([^,]*),\\s*" +						// ontology identifier
		"([^,]*),\\s*" +						// CV term accession
		"((?:\"[^\"]*\")|(?:[^,]*)),\\s*" +	// CV term name
		"((?:\"[^\"]*\")|(?:[^,]*))" +		// CV term "value"
		"\\]";							
	public static final Pattern CV_TERM_PATTERN = Pattern.compile(
		String.format("^%s$", MATCHING_CV_TERM));
	public static final Pattern CV_ACCESSION_PATTERN = Pattern.compile(
		"^(.*?:\\d*)$");
	
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
	public static final Pattern SCAN_ID_PATTERN =
		Pattern.compile("scanId=(\\d+)");
	public static final Pattern INDEX_PATTERN = Pattern.compile("index=(\\d+)");
	public static final Pattern QUERY_PATTERN = Pattern.compile("query=(\\d+)");
	public static final Pattern FILE_PATTERN = Pattern.compile("file=(.+)");
    public static final String THERMO_SCAN_PREFIX =
        "controllerType=0 controllerNumber=1 scan=";
	
	// constants pertaining to mzTab "modifications" column values,
	// as seen in PRT, PEP and PSM rows
	public static final String MZTAB_POSITION = 
		"(?:null)|(?:\\d+)" +						// position
		"(?:" + NON_MATCHING_CV_TERM + ")?";		// CV parameter
	public static final Pattern MZTAB_POSITION_PATTERN = Pattern.compile(
		String.format("^(%s)$", MZTAB_POSITION));
	public static final String MZTAB_POSITION_TUPLE = 
		"(?:" + MZTAB_POSITION + ")" +				// initial position
		"(?:\\|(?:" + MZTAB_POSITION + "))*";		// ambiguous positions
	public static final Pattern MZTAB_POSITION_TUPLE_PATTERN = Pattern.compile(
		String.format("^(%s)$", MZTAB_POSITION_TUPLE));
	public static final Pattern MZTAB_MODIFICATION_PATTERN = Pattern.compile(
		"(" + MZTAB_POSITION_TUPLE + ")-" +			// position(s)
		"((?:" + NON_MATCHING_CV_TERM + ")|" +		// CV parameter OR
		"(?:[^,]+))");								// mod ID
//	public static final Pattern MZTAB_POSITION_PATTERN = Pattern.compile(
//		"(\\d+)([^\\|]*)?\\|?");
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
	
	// constants pertaining to FDR-based MassIVE search import
	public static final double DEFAULT_IMPORT_Q_VALUE_THRESHOLD = 0.01;
	
	// constants pertaining to known optional column values
	public static final String[] KNOWN_PSM_QVALUE_COLUMNS = new String[]{
		"QValue", "MS-GF:QValue"
	};
	public static final String[] KNOWN_PEPTIDE_QVALUE_COLUMNS = new String[]{
		"PepQValue", "MS-GF:PepQValue"
	};
	public static final String[] KNOWN_DECOY_PATTERNS = new String[]{
		"XXX_"
	};
	
	// constants pertaining to ProteoSAFe params.xml result/peak file mapping
	public static final String EXTRACTED_FILE_DELIMITER = "#";
	public static final Pattern EXTRACTED_FILE_DELIMITER_PATTERN =
		Pattern.compile("((?i)mztab|mzid)" + EXTRACTED_FILE_DELIMITER);
	public static final Pattern MANGLED_FILE_PATH_PATTERN =
		Pattern.compile("(.*/)?([^/]+-[0-9]{5}\\.[^/]+)");
	
	// constants pertaining to peptides
	public static final Map<Character, Double> AMINO_ACID_MASSES =
		new TreeMap<Character, Double>();
	static {
		AMINO_ACID_MASSES.put('A', 71.037113787);
		AMINO_ACID_MASSES.put('C', 103.009184477);
		AMINO_ACID_MASSES.put('D', 115.026943031);
		AMINO_ACID_MASSES.put('E', 129.042593095);
		AMINO_ACID_MASSES.put('F', 147.068413915);
		AMINO_ACID_MASSES.put('G', 57.021463723);
		AMINO_ACID_MASSES.put('H', 137.058911861);
		AMINO_ACID_MASSES.put('I', 113.084063979);
		AMINO_ACID_MASSES.put('K', 128.094963016);
		AMINO_ACID_MASSES.put('L', 113.084063979);
		AMINO_ACID_MASSES.put('M', 131.040484605);
		AMINO_ACID_MASSES.put('N', 114.042927446);
		AMINO_ACID_MASSES.put('P', 97.052763851);
		AMINO_ACID_MASSES.put('Q', 128.058577510);
		AMINO_ACID_MASSES.put('R', 156.101111026);
		AMINO_ACID_MASSES.put('S', 87.032028409);
		AMINO_ACID_MASSES.put('T', 101.047678473);
		AMINO_ACID_MASSES.put('V', 99.068413915);
		AMINO_ACID_MASSES.put('W', 186.079312952);
		AMINO_ACID_MASSES.put('Y', 163.063328537);
	}
	public static final Set<Character> KNOWN_AMINO_ACIDS =
		new HashSet<Character>(AMINO_ACID_MASSES.keySet());
	static { KNOWN_AMINO_ACIDS.add('X'); }
	public static final String AMINO_ACID_CHARSET =
		new String(ArrayUtils.toPrimitive(KNOWN_AMINO_ACIDS.toArray(
			new Character[KNOWN_AMINO_ACIDS.size()])));
	public static final Pattern AMINO_ACID_PATTERN = Pattern.compile(
		String.format("^[%s]$", AMINO_ACID_CHARSET));
	public static final Pattern PEPTIDE_STRING_PATTERN = Pattern.compile(
		String.format(
			"^\"?" +					// optional opening quotation mark
			"\\[?([\\-_%s]+?)\\]?" +	// "pre" amino acid (optional brackets)
			"\\.(.*)\\." +				// actual peptide sequence
			"\\[?([\\-_%s]+?)\\]?" +	// "post" amino acid (optional brackets)
			"\"?$",						// optional closing quotation mark
			AMINO_ACID_CHARSET, AMINO_ACID_CHARSET
		)
	);
	
	// constants pertaining to protein accession strings
	public static final Pattern PRE_POST_PROTEIN_ACCESSION_PATTERN =
		Pattern.compile("^(.*?)\\|?\\(?pre=.{1},post=.{1}\\)?$");
	public static final Pattern NUMBER_SLASH_PREFIX_PROTEIN_ACCESSION_PATTERN =
		Pattern.compile("^[0-9]?/(.*?)$");
	public static final Pattern[] BAD_PROTEIN_ACCESSION_PATTERNS =
	new Pattern[]{
		Pattern.compile("^[-+]?[0-9]+/[-+]?[0-9]+$")
	};
	public static final String[] BAD_PROTEIN_ACCESSION_SUBSTRINGS =
	new String[]{
		"@"
	};
	
	// general constants
	public static final Pattern FILE_URI_PROTOCOL_PATTERN =
		Pattern.compile("file:(?:[/]{2})?(.*)");
	public static final Pattern FLOAT_PATTERN = Pattern.compile(
		"[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?");
	public static final String SIMPLE_FLOAT_PATTERN_STRING = 
		"((?:\\+?-?\\d+\\.?\\d*)|(?:\\+?-?\\d*\\.?\\d+))";
	public static final Pattern SIMPLE_FLOAT_PATTERN = Pattern.compile(
		SIMPLE_FLOAT_PATTERN_STRING);
	public static final Pattern QUOTED_STRING_PATTERN =
		Pattern.compile("^\"(.*)\"$");
}
