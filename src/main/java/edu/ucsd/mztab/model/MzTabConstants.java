package edu.ucsd.mztab.model;

import java.util.regex.Pattern;

public class MzTabConstants
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	// constants pertaining to mzTab file structure
	public static enum MzTabSection { MTD, PRT, PEP, PSM, SML }
	public static final Pattern FILE_LINE_PATTERN = Pattern.compile(
		"^MTD\\s+ms_run\\[(\\d+)\\]-location\\s+(.+)$");
	
	// constants pertaining to mzTab "spectra_ref" column values,
	// as seen in PSM rows
	public static final Pattern SPECTRA_REF_PATTERN = Pattern.compile(
		"ms_run\\[(\\d+)\\]:(.+)");
	
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
	
	// constants pertaining to peptide strings
	public static final String AMINO_ACID_CHARSET = "ARDNCEQGHILKMFPSTWYV";
	public static final Pattern AMINO_ACID_PATTERN = Pattern.compile(
		String.format("^[%s]$", AMINO_ACID_CHARSET));
	public static final Pattern PEPTIDE_STRING_PATTERN = Pattern.compile(
		String.format("^[%s]+$", AMINO_ACID_CHARSET));
	
	// constants pertaining to general CV term syntax
	public static final Pattern CV_TERM_PATTERN = Pattern.compile(
		"^\\[([^,]*),\\s*([^,]*),\\s*\"?([^\"]*)\"?,\\s*([^,]*)\\]$");
	public static final Pattern CV_ACCESSION_PATTERN = Pattern.compile(
		"^(.*?:\\d*)$");
	
	// general constants
	public static final Pattern FILE_URI_PROTOCOL_PATTERN =
		Pattern.compile("file:(?:[/]{2})?(.*)");
	
	public static final Pattern FLOAT_PATTERN = Pattern.compile(
		"[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?");
}
