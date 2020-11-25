package edu.ucsd.mztab;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;

import javax.xml.bind.TypeConstraintException;

import org.apache.xpath.XPathAPI;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import edu.ucsd.mztab.model.MzTabConstants;
import edu.ucsd.mztab.util.FileIOUtils;
import edu.ucsd.mztab.util.OntologyUtils;
import edu.ucsd.mztab.util.ProteomicsUtils;

public class TSVToMzTabParamGenerator
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String USAGE = "---------- Usage: ----------\n" +
		"java -cp MzTabUtils.jar edu.ucsd.mztab.TSVToMzTabParamGenerator " +
		"\n\t-tsv                    <InputTSVFile>" +
		"\n\t-params                 <ProteoSAFeParametersFile>" +
		"\n\t-output                 <OutputTSVToMzTabParametersFile>" +
		"\n\t-header_line            \"true\"/\"false\"" +
		"\n\t-filename               <SpectrumFilenameColumnHeaderOrIndex>" +
		"\n\t[-max_filenames         <MaxDistinctFilenameColumnValues>]" +
		"\n\t-modified_sequence      " +
			"<ModifiedPeptideSequenceColumnHeaderOrIndex>" +
		"\n\t-mod_pattern            <ModificationStringFormat> " +
			"(parameter may occur more than once)" +
		"\n\t-fixed_mod              <ModAccession>|<ModPattern> " +
			"(parameter may occur more than once)" +
		"\n\t-variable_mod           <ModAccession>|<ModPattern> " +
			"(parameter may occur more than once)" +
		"\n\t[-fixed_mods_reported   \"true\"/\"false\" (default \"false\")]" +
		"\n\t[-match_mass_precision  <RoundingToMatchMod> " +
			"(if specified, will attempt to match mod masses found in TSV " +
			"file to known standard mod masses by rounding to this precision)]" +
		"\n\t[-match_mass_difference <LargestMassDifferenceToMatchMod> "+
			"(if specified, will attempt to match mod masses found in TSV " +
			"file to known standard mod masses according to given " +
			"difference threshold)]" +
		"\n\t[-spectrum_id_type      \"scan\"/\"index\"]" +
		"\n\t[-scan                  <ScanColumnHeaderOrIndex> "+
			"(if -spectrum_id_type=\"scan\")]" +
		"\n\t[-index                 <SpectrumIndexColumnHeaderOrIndex> " +
			"(if -spectrum_id_type=\"index\")]" +
		"\n\t[-index_numbering       0/1 " +
			"(if -spectrum_id_type=\"index\", " +
			"specify 0-based/1-based numbering, default 0)]" +
		"\n\t[-accession             <ProteinAccessionColumnHeaderOrIndex>]" +
		"\n\t[-charge                <PrecursorChargeColumnHeaderOrIndex>]" +
		"\n\t[-exp_mass_to_charge    <ExperimentalMZColumnHeaderOrIndex>]" +
		"\n\t[-calc_mass_to_charge   <CalculatedMZColumnHeaderOrIndex>]" +
		"\n\t[-msgf_evalue           <MSGF:EValueColumnHeaderOrIndex>]" +
		"\n\t[-msgf_spec_evalue      <MS-GF:SpecEValueColumnHeaderOrIndex>]" +
		"\n\t[-msgf_qvalue           <MS-GF:QValueColumnHeaderOrIndex>]" +
		"\n\t[-msgf_pep_qvalue       <MS-GF:PepQValueColumnHeaderOrIndex>]";
	
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private File                  tsvFile;
	private File                  paramsFile;
	private boolean               hasHeader;
	private boolean               fixedModsReported;
	private boolean               scanMode;
	private boolean               zeroBased;
	private boolean               matchMods;
	private Integer               massPrecisionToMatch;
	private Double                maxMassDifferenceToMatch;
	private Map<String, String>   columnIdentifiers;
	private Map<String, Integer>  columnIndices;
	private Map<Double, String[]> variableModMasses;
	private Set<String>           staticModAccessions;
	private Set<String>           dynamicFixedModAccessions;
	private Collection<Double>    foundMods;
	private Collection<String>    fixedMods;
	private Collection<String>    variableMods;
	private Collection<String>    modPatterns;
	
	/*========================================================================
	 * Constructors
	 *========================================================================*/
	public TSVToMzTabParamGenerator(
		File inputParams, File tsvFile, File paramsFile, String hasHeader,
		String filenameColumn, Integer maxFilenames, String sequenceColumn,
		String fixedModsReported, String specIDType, String scanColumn,
		String indexColumn, String indexNumbering,
		String accessionColumn, String chargeColumn,
		String experimentalMZColumn, String calculatedMZColumn,
		String msgfEValueColumn, String msgfSpecEValueColumn,
		String msgfQValueColumn, String msgfPepQValueColumn,
		String massPrecisionToMatch, String maxMassDifferenceToMatch,
		Collection<String> modPatterns, Collection<String> staticFixedMods,
		Collection<String> staticVariableMods
	) throws IOException {
		// validate input parameter file
		if (inputParams == null)
			throw new NullPointerException(
				"Input parameter file cannot be null.");
		else if (inputParams.isFile() == false)
			throw new IllegalArgumentException(String.format(
				"Input parameter file [%s] must be a normal " +
				"(non-directory) file.", inputParams.getName()));
		else if (inputParams.canRead() == false)
			throw new IllegalArgumentException(String.format(
				"Input parameter file [%s] must be readable.",
				inputParams.getName()));
		// validate input TSV file
		if (tsvFile == null)
			throw new NullPointerException(
				"Input tab-delimited result file cannot be null.");
		else if (tsvFile.isFile() == false)
			throw new IllegalArgumentException(String.format(
				"Input tab-delimited result file [%s] must be a normal " +
				"(non-directory) file.", tsvFile.getName()));
		else if (tsvFile.canRead() == false)
			throw new IllegalArgumentException(String.format(
				"Input tab-delimited result file [%s] must be readable.",
				tsvFile.getName()));
		else this.tsvFile = tsvFile;
		String filename = this.tsvFile.getName();
		// validate output conversion parameters file
		if (paramsFile == null)
			throw new NullPointerException(
				"Output parameter file cannot be null.");
		// attempt to create output file and test its writeability
		boolean writeable = true;
		if (paramsFile.exists())
			writeable = paramsFile.delete();
		if (writeable)
			writeable = paramsFile.createNewFile() && paramsFile.canWrite();
		if (writeable == false)
			throw new IllegalArgumentException(
				String.format("Output parameter file [%s] must be writable.",
					paramsFile.getName()));
		else this.paramsFile = paramsFile;
		// set whether there is a header line in the input file
		this.hasHeader = false;
		if (hasHeader == null)
			throw new NullPointerException("\"header_line\" cannot be null.");
		else if (hasHeader.equalsIgnoreCase("true") ||
			hasHeader.equalsIgnoreCase("yes") || hasHeader.equals("1"))
			this.hasHeader = true;
		else if (hasHeader.equalsIgnoreCase("false") == false &&
			hasHeader.equalsIgnoreCase("no") == false &&
			hasHeader.equals("0") == false)
			throw new IllegalArgumentException(String.format(
				"Unrecognized \"header_line\" value: [%s]", hasHeader));
		// initialize mod collections
		fixedMods = new LinkedHashSet<String>();
		variableMods = new LinkedHashSet<String>();
		foundMods = new LinkedHashSet<Double>();
		variableModMasses = new HashMap<Double, String[]>();
		staticModAccessions = new HashSet<String>();
		dynamicFixedModAccessions = new HashSet<String>();
		// set mod patterns
		if (modPatterns == null || modPatterns.isEmpty())
			throw new NullPointerException(
				"At least one \"mod_pattern\" parameter must be provided.");
		else this.modPatterns = modPatterns;
		// add static mods specified on the command line
		if (staticFixedMods != null)
			for (String fixedMod : staticFixedMods)
				addStaticMod(fixedMod, true);
		if (staticVariableMods != null)
			for (String variableMod : staticVariableMods)
				addStaticMod(variableMod, false);
		// set whether fixed mods are explicitly written to
		// the input TSV file's modified peptide strings
		if (fixedModsReported == null)
			this.fixedModsReported = false;
		else this.fixedModsReported = Boolean.parseBoolean(fixedModsReported);
		// set whether to try to match found mods to known mods using
		// a precision to round to, and what that precision should be
		if (massPrecisionToMatch == null) {
			this.massPrecisionToMatch = null;
			// if no precision is specified, then
			// look for a mass difference threshold
			if (maxMassDifferenceToMatch == null) {
				matchMods = false;
				this.maxMassDifferenceToMatch = null;
			} else try {
				matchMods = true;
				this.maxMassDifferenceToMatch =
					Math.abs(Double.parseDouble(maxMassDifferenceToMatch));
			} catch (NumberFormatException error) {
				throw new IllegalArgumentException(String.format(
					"Maximum mass difference to match mods [%s] must be a " +
					"valid real number representing a mass difference in " +
					"Daltons.", maxMassDifferenceToMatch));
			}
		} else try {
			matchMods = true;
			this.massPrecisionToMatch =
				Math.abs(Integer.parseInt(massPrecisionToMatch));
			this.maxMassDifferenceToMatch = null;
		} catch (NumberFormatException error) {
			throw new IllegalArgumentException(String.format(
				"Mass rounding precision to match mods [%s] must be a valid " +
				"integer representing the number of decimal places to which " +
				"the masses in the TSV file must be rounded to match " +
				"ontology mods.", massPrecisionToMatch));
		}
		// parse and process the input tab-delimited result file
		Collection<String> filenames = new HashSet<String>();
		BufferedReader reader = null;
		String line = null;
		int lineNumber = -1;
		try {
			System.out.println(String.format(
				"Reading input TSV file [%s]...", filename));
			// read the first line of the file
			reader = new BufferedReader(new FileReader(this.tsvFile));
			reader.mark(65535);
			line = reader.readLine();
			// if the first line is not supposed to be a header line, rewind
			if (this.hasHeader == false) {
				reader.reset();
				lineNumber = 1;
			} else lineNumber = 2;
			// parse out the elements of the first line
			// to validate all column IDs
			String[] elements = line.split("\t");
			if (elements == null || elements.length < 1)
				throw new IllegalArgumentException(
					String.format("Could not parse the tab-delimited " +
						"elements of the first line from input TSV file [%s].",
						filename));
			validateColumnHeaders(line, filename, elements,
				filenameColumn, sequenceColumn, scanColumn, indexColumn,
				accessionColumn, chargeColumn,
				experimentalMZColumn, calculatedMZColumn,
				msgfEValueColumn, msgfSpecEValueColumn,
				msgfQValueColumn, msgfPepQValueColumn);
			// get spectrum filename string column index
			Integer fileIndex = columnIndices.get("filename");
			// it should be impossible for the filename column index to
			// not be known, since it would have thrown an exception
			// when determining it earlier if it was not found
			if (fileIndex == null)
				throw new IllegalStateException();
			// get modified peptide string column index
			Integer psmIndex = columnIndices.get("modified_sequence");
			// it should be impossible for the peptide sequence column index
			// to not be known, since it would have thrown an exception when
			// determining it earlier if it was not found
			if (psmIndex == null)
				throw new IllegalStateException();
			// set whether spectrum IDs in the input file are scans or indices,
			// if explicitly specified on the command line
			Integer scan = columnIndices.get("scan");
			Integer index = columnIndices.get("index");
			// allow for the special case where scan nativeIDs
			// can be packed into the index column, e.g. MS-GF+
			boolean scanInIndexColumn = false;
			if (specIDType != null) {
				scanMode = false;
				if (specIDType.equalsIgnoreCase("scan"))
					scanMode = true;
				else if (specIDType.equalsIgnoreCase("index") == false)
					throw new IllegalArgumentException(String.format(
						"Unrecognized \"spectrum_id_type\" value: [%s]",
						specIDType));
				// if mode is set on the command line,
				// then that column had better be present
				if (scanMode && scan == null)
					throw new IllegalArgumentException(String.format(
						"\"spectrum_id_type\" was specified as \"%s\", but " +
						"no value was provided for the \"scan\" column.",
						specIDType));
				else if (scanMode == false && index == null)
					throw new IllegalArgumentException(String.format(
						"\"spectrum_id_type\" was specified as \"%s\", but " +
						"no value was provided for the \"index\" column.",
						specIDType));
			}
			// otherwise, see if only one spectrum ID column was specified
			// on the command line; if so, then that must be the type
			else {
				if (scan == null && index == null)
					throw new NullPointerException("Either a \"scan\" or " +
						"\"index\" column must be provided, to uniquely " +
						"identify each spectrum in the source file.");
				else if (scan == null && index != null)
					scanMode = false;
				else if (scan != null && index == null)
					scanMode = true;
				// if both columns are present, then we need to read the first
				// data line of the file to determine the spectrum ID type
				else {
					line = reader.readLine();
					reader.reset();
					if (line == null)
						elements = null;
					else elements = line.split("\t");
					if (elements == null || elements.length < 1)
						throw new IllegalArgumentException(
							String.format("Could not parse the tab-delimited " +
								"elements of the first data line from input " +
								"TSV file [%s].", filename));
					try {
						scanMode =
							determineSpectrumIDType(elements, scan, index);
					}
					// the "determineSpectrumIDType" method will
					// throw a TypeConstraintException iff a scan
					// nativeID was found in the index column
					catch (TypeConstraintException error) {
						scanMode = true;
						scanInIndexColumn = true;
					}
					// add any found mods while we're looking at a data row
					addModMasses(elements[psmIndex]);
				}
			}
			// note proper spectrum ID column, now that it's been determined
			if (scanMode) {
				if (scanInIndexColumn)
					columnIdentifiers.put("spectrum_id", indexColumn);
				else columnIdentifiers.put("spectrum_id", scanColumn);
			} else columnIdentifiers.put("spectrum_id", indexColumn);
			// set whether spectrum indices in the input file
			// are ordered with 0-based or 1-based numbering
			zeroBased = true;
			if (indexNumbering != null && indexNumbering.trim().equals("1"))
				zeroBased = false;
			// read the remaining lines of the file, if parameters call for it
			if (maxFilenames != null || matchMods) {
				while ((line = reader.readLine()) != null) {
					lineNumber++;
					elements = line.split("\t");
					if (elements == null || elements.length < 1)
						continue;
					// if a filename limit has been given, collect all found
					// filenames from the values of the filename column
					if (maxFilenames != null && elements.length > fileIndex)
						filenames.add(elements[fileIndex]);
					// if the mod-matching mode calls for it, collect all found
					// mods from the values of the modified_sequence column
					if (matchMods && elements.length > psmIndex)
						addModMasses(elements[psmIndex]);
				}
			}
		} catch (Throwable error) {
			throw new RuntimeException(String.format("There was an error " +
				"parsing input tab-delimited result file [%s], line %d:\n%s",
				filename, lineNumber, error.getMessage()), error);
		} finally {
			try { reader.close(); }
			catch (Throwable error) {}
		}
		// verify that there aren't too many unique filenames
		if (maxFilenames != null && filenames.size() > maxFilenames)
			throw new IllegalArgumentException(String.format(
				"Input tab-delimited result file [%s] contains too many " +
				"spectrum filenames (column [%s]): found %d distinct values " +
				"(%d maximum).", filename, filenameColumn, filenames.size(),
				maxFilenames));
		// parse params.xml file to match found mods against user-declared ones
		try {
			System.out.println(String.format(
				"Reading input XML parameters file [%s]...",
				inputParams.getName()));
			// parse input file as an XML document
			Document document = FileIOUtils.parseXML(inputParams);
			if (document == null)
				throw new NullPointerException(
					"Parameters XML document could not be parsed.");
			// determine from params.xml which fixed cysteine mod to use
			Node parameter = XPathAPI.selectSingleNode(
				document, "//parameter[@name='cysteine_protease.cysteine']");
			if (parameter != null) {
				String cysteine = parameter.getFirstChild().getNodeValue();
				if (cysteine != null && cysteine.trim().isEmpty() == false) {
					if (cysteine.trim().equals("c57"))
						addDynamicMod(
							"[UNIMOD,UNIMOD:4,Carbamidomethyl,\"%s\"]",
							"C", "57.021464", true, null);
					else if (cysteine.trim().equals("c58"))
						addDynamicMod("[UNIMOD,UNIMOD:6,Carboxymethyl,\"%s\"]",
							"C", "58.005479", true, null);
					else if (cysteine.trim().equals("c99"))
						addDynamicMod("[UNIMOD,UNIMOD:17,NIPCAM,\"%s\"]",
							"C", "99.068414", true, null);
				}
			}
			// add all "standard" ProteoSAFe variable mods,
			// regardless of whether or not the user added them
			addDynamicMod("[UNIMOD,UNIMOD:7,Deamidated,\"%s\"]",
				"NQ", "0.984016", false, null);
			addDynamicMod("[UNIMOD,UNIMOD:34,Methyl,\"%s\"]",
				"K", "14.015650", false, null);
			addDynamicMod("[UNIMOD,UNIMOD:1,Acetyl,\"%s\"]",
				"*", "42.010565", false, true);
			addDynamicMod("[UNIMOD,UNIMOD:5,Carbamyl,\"%s\"]",
				"*", "43.005814", false, true);
			addDynamicMod("[UNIMOD,UNIMOD:35,Oxidation,\"%s\"]",
				"M", "15.994915", false, null);
			addDynamicMod("[UNIMOD,UNIMOD:21,Phospho,\"%s\"]",
				"STY", "79.966331", false, null);
			addDynamicMod("[UNIMOD,UNIMOD:28,Gln->pyro-Glu,\"%s\"]",
				"Q", "-17.026549", false, true);
			// handle all "standard" ProteoSAFe mods selected
			// by the user in "custom PTM" format
			NodeList parameters = XPathAPI.selectNodeList(
				document, "//parameter[@name='ptm.custom_PTM']");
			if (parameters != null && parameters.getLength() > 0) {
				for (int i=0; i<parameters.getLength(); i++) {
					String mod =
						parameters.item(i).getFirstChild().getNodeValue();
					if (mod != null && mod.trim().isEmpty() == false) {
						// handle all known fixed mods (known variable mods are
						// all handled by blanket-adding as dynamic mods above)
						if (mod.equals("+304.199040,K,fix"))
							addDynamicMod("[UNIMOD,UNIMOD:731," +
								"iTRAQ8plex:13C(6)15N(2),\"%s\"]",
								"K", "+304.199040", true, null);
						else if (mod.equals("+304.199040,*,fix_nterm"))
							addDynamicMod("[UNIMOD,UNIMOD:731," +
								"iTRAQ8plex:13C(6)15N(2),\"%s\"]",
								"*", "+304.199040", true, true);
						else if (mod.equals("+229.162932,K,fix"))
							addDynamicMod("[UNIMOD,UNIMOD:737,TMT6plex,\"%s\"]",
								"K", "+229.162932", true, null);
						else if (mod.equals("+229.162932,*,fix_nterm"))
							addDynamicMod("[UNIMOD,UNIMOD:737,TMT6plex,\"%s\"]",
								"*", "+229.162932", true, true);
						// TODO: the following code isn't really useful unless
						// we try to match these mods up with UNIMOD or PSI-MOD
						// accessions, since adding more "unknown modifications"
						// will just result in CHEMMODs; meaning the result will
						// be the same as if we had not bothered to do this and
						// just let the catch-all mod specifier pick these up
//						// "ptm.custom_PTM" parameters should have as their
//						// value a string with the following format:
//						// <mass>,<residues>,<type>
//						String[] tokens = mod.split(",");
//						if (tokens == null || tokens.length != 3)
//							continue;
//						// determine fixed/variable status from declared type
//						boolean fixed = false;
//						if (tokens[2].startsWith("fix"))
//							fixed = true;
//						// determine terminal status from declared type
//						Boolean terminal = null;
//						if (tokens[2].contains("nterm"))
//							terminal = true;
//						else if (tokens[2].contains("cterm"))
//							terminal = false;
//						addDynamicMod(
//							"[MS,MS:1001460,unknown modification,\"%s\"]",
//							tokens[1], tokens[0], fixed, terminal);
					}
				}
			}
			// if the mod-matching mode is mass difference threshold,
			// and there are any remaining found masses that haven't
			// yet been assigned to a mod, then try to match the
			// leftovers to mods that were already added
			if (matchMods && this.maxMassDifferenceToMatch != null &&
				foundMods.isEmpty() == false &&
				variableModMasses.isEmpty() == false) {
				for (Double floatingModMass : foundMods) {
					for (Double addedModMass : variableModMasses.keySet()) {
						if (Math.abs(floatingModMass - addedModMass) <=
							this.maxMassDifferenceToMatch) {
							String[] mod = variableModMasses.get(addedModMass);
							// build a separate mod CV term string
							// for each possible mod format
							for (String modPattern : modPatterns) {
								String modCVTerm = String.format(mod[0],
									getModFormatString(modPattern, mod[1],
									Double.toString(floatingModMass), false,
									mod[2] == null ? null :
										Boolean.parseBoolean(mod[2])));
								variableMods.add(modCVTerm);
							}
							break;
						}
					}
				}
			}
			// finally, go through all fixed mods, and only keep those
			// for which a dynamic add was triggered (meaning the user
			// actually selected them in the input form), removing all
			// other fixed mods (since presumably they do not occur in
			// this search due to not having been selected by the user)
			for (String cvTerm : new HashSet<String>(fixedMods)) {
				Matcher matcher =
					MzTabConstants.CV_TERM_PATTERN.matcher(cvTerm);
				if (matcher.matches() == false)
					throw new IllegalStateException();
				String accession = matcher.group(2);
				if (dynamicFixedModAccessions.contains(accession) == false)
					fixedMods.remove(cvTerm);
			}
		} catch (Throwable error) {
			die(null, error);
		}
	}
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public void writeParameterFile() {
		PrintWriter output = null;
		try {
			// open output file for writing
			System.out.println(String.format(
				"Writing output text parameters file [%s]...",
				paramsFile.getName()));
			output = new PrintWriter(paramsFile);
			// write header status
			if (hasHeader)
				output.println("header_line=true");
			else output.println("header_line=false");
			// write spectrum ID mode
			if (scanMode)
				output.println("spectrum_id_type=scan");
			else {
				output.println("spectrum_id_type=index");
				// write index numbering
				if (zeroBased)
					output.println("index_numbering=0");
				else output.println("index_numbering=1");
			}
			// add generic catch-all unknown variable mod specifiers
			if (variableMods == null)
				variableMods = new LinkedHashSet<String>();
			for (String modPattern : modPatterns)
				variableMods.add(String.format(
					"[MS,MS:1001460,unknown modification,\"%s\"]", modPattern));
			// write variable mods
			StringBuilder mods = new StringBuilder("variable_mods=");
			for (String variableMod : variableMods)
				mods.append(variableMod).append("|");
			// chomp trailing pipe ("|")
			if (mods.charAt(mods.length() - 1) == '|')
				mods.setLength(mods.length() - 1);
			output.println(mods.toString());
			// write fixed mods
			if (fixedMods != null && fixedMods.isEmpty() == false) {
				mods = new StringBuilder("fixed_mods=");
				for (String fixedMod : fixedMods)
					mods.append(fixedMod).append("|");
				// chomp trailing pipe ("|")
				if (mods.charAt(mods.length() - 1) == '|')
					mods.setLength(mods.length() - 1);
				output.println(mods.toString());
				// write whether fixed mods are reported
				if (fixedModsReported)
					output.println("fixed_mods_reported=true");
			}
			// write filename column identifier
			output.println(String.format("filename=%s",
				columnIdentifiers.get("filename")));
			// write spectrum_id column identifier
			output.println(String.format("spectrum_id=%s",
				columnIdentifiers.get("spectrum_id")));
			// write modified_sequence column identifier
			output.println(String.format("modified_sequence=%s",
				columnIdentifiers.get("modified_sequence")));
			// write accession column identifier, if present
			String accession = columnIdentifiers.get("accession");
			if (accession != null)
				output.println(String.format("accession=%s", accession));
			// write charge column identifier, if present
			String charge = columnIdentifiers.get("charge");
			if (charge != null)
				output.println(String.format("charge=%s", charge));
			// write experimental M/Z column identifier, if present
			String experimentalMZ = columnIdentifiers.get("exp_mass_to_charge");
			if (experimentalMZ != null)
				output.println(
					String.format("exp_mass_to_charge=%s", experimentalMZ));
			// write calculated M/Z column identifier, if present
			String calculatedMZ = columnIdentifiers.get("calc_mass_to_charge");
			if (calculatedMZ != null)
				output.println(
					String.format("calc_mass_to_charge=%s", calculatedMZ));
			// write MS-GF:EValue column identifier, if present
			String msgfEValue = columnIdentifiers.get("msgf_evalue");
			if (msgfEValue != null)
				output.println(
					String.format("msgf_evalue=%s", msgfEValue));
			// write MS-GF:SpecEValue column identifier, if present
			String msgfSpecEValue = columnIdentifiers.get("msgf_spec_evalue");
			if (msgfSpecEValue != null)
				output.println(
					String.format("msgf_spec_evalue=%s", msgfSpecEValue));
			// write MS-GF:QValue column identifier, if present
			String msgfQValue = columnIdentifiers.get("msgf_qvalue");
			if (msgfQValue != null)
				output.println(
					String.format("msgf_qvalue=%s", msgfQValue));
			// write MS-GF:PepQValue column identifier, if present
			String msgfPepQValue = columnIdentifiers.get("msgf_pep_qvalue");
			if (msgfPepQValue != null)
				output.println(
					String.format("msgf_pep_qvalue=%s", msgfPepQValue));
			System.out.println("Done.");
		} catch (Throwable error) {
			die(null, error);
		} finally {
			try { output.close(); }
			catch (Throwable error) {}
		}
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private void validateColumnHeaders(
		String line, String filename, String[] elements,
		String filenameColumn, String sequenceColumn, String scanColumn,
		String indexColumn, String accessionColumn, String chargeColumn,
		String experimentalMZColumn, String calculatedMZColumn,
		String msgfEValueColumn, String msgfSpecEValueColumn,
		String msgfQValueColumn, String msgfPepQValueColumn
	) {
		// validate all columns declared on the command line
		columnIdentifiers = new LinkedHashMap<String, String>();
		columnIndices = new LinkedHashMap<String, Integer>();
		// filename column
		columnIdentifiers.put("filename", filenameColumn);
		Integer index = TSVToMzTabParameters.extractColumnIndex(
			filenameColumn, filename, "filename",
			line, this.hasHeader, elements);
		if (index == null)
			throw new NullPointerException(String.format(
				"There was an error parsing \"filename\" column [%s].",
				filenameColumn));
		else columnIndices.put("filename", index);
		// modified_sequence column
		columnIdentifiers.put("modified_sequence", sequenceColumn);
		index = TSVToMzTabParameters.extractColumnIndex(
			sequenceColumn, filename, "modified_sequence",
			line, this.hasHeader, elements);
		if (index == null)
			throw new NullPointerException(String.format(
				"There was an error parsing \"modified_sequence\" " +
				"column [%s].", sequenceColumn));
		else columnIndices.put("modified_sequence", index);
		// scan column
		index = TSVToMzTabParameters.extractColumnIndex(
			scanColumn, filename, "scan",
			line, this.hasHeader, elements);
		if (index != null)
			columnIndices.put("scan", index);
		// index column
		index = TSVToMzTabParameters.extractColumnIndex(
			indexColumn, filename, "index",
			line, this.hasHeader, elements);
		if (index != null)
			columnIndices.put("index", index);
		// accession column
		index = TSVToMzTabParameters.extractColumnIndex(
			accessionColumn, filename, "accession",
			line, this.hasHeader, elements);
		if (index != null) {
			columnIndices.put("accession", index);
			columnIdentifiers.put("accession", accessionColumn);
		}
		// charge column
		index = TSVToMzTabParameters.extractColumnIndex(
			chargeColumn, filename, "charge",
			line, this.hasHeader, elements);
		if (index != null) {
			columnIndices.put("charge", index);
			columnIdentifiers.put("charge", chargeColumn);
		}
		// experimental M/Z column
		index = TSVToMzTabParameters.extractColumnIndex(
			experimentalMZColumn, filename, "exp_mass_to_charge",
			line, this.hasHeader, elements);
		if (index != null) {
			columnIndices.put("exp_mass_to_charge", index);
			columnIdentifiers.put("exp_mass_to_charge", experimentalMZColumn);
		}
		// calculated M/Z column
		index = TSVToMzTabParameters.extractColumnIndex(
			calculatedMZColumn, filename, "calc_mass_to_charge",
			line, this.hasHeader, elements);
		if (index != null) {
			columnIndices.put("calc_mass_to_charge", index);
			columnIdentifiers.put("calc_mass_to_charge", calculatedMZColumn);
		}
		// MS-GF:EValue column
		index = TSVToMzTabParameters.extractColumnIndex(
			msgfEValueColumn, filename, "msgf_evalue",
			line, this.hasHeader, elements);
		if (index != null) {
			columnIndices.put("msgf_evalue", index);
			columnIdentifiers.put("msgf_evalue", msgfEValueColumn);
		}
		// MS-GF:SpecEValue column
		index = TSVToMzTabParameters.extractColumnIndex(
			msgfSpecEValueColumn, filename, "msgf_spec_evalue",
			line, this.hasHeader, elements);
		if (index != null) {
			columnIndices.put("msgf_spec_evalue", index);
			columnIdentifiers.put("msgf_spec_evalue", msgfSpecEValueColumn);
		}
		// MS-GF:QValue column
		index = TSVToMzTabParameters.extractColumnIndex(
			msgfQValueColumn, filename, "msgf_qvalue",
			line, this.hasHeader, elements);
		if (index != null) {
			columnIndices.put("msgf_qvalue", index);
			columnIdentifiers.put("msgf_qvalue", msgfQValueColumn);
		}
		// MS-GF:PepQValue column
		index = TSVToMzTabParameters.extractColumnIndex(
			msgfPepQValueColumn, filename, "msgf_pep_qvalue",
			line, this.hasHeader, elements);
		if (index != null) {
			columnIndices.put("msgf_pep_qvalue", index);
			columnIdentifiers.put("msgf_pep_qvalue", msgfPepQValueColumn);
		}
	}
	
	private boolean determineSpectrumIDType(
		String[] elements, int scanColumn, int indexColumn
	) throws TypeConstraintException {
		String scan = elements[scanColumn];
		String index = elements[indexColumn];
		// first try to parse scan number as a nativeID
		Integer value = null;
		Matcher matcher = MzTabConstants.SCAN_PATTERN.matcher(scan);
		if (matcher.find()) try {
			value = Integer.parseInt(matcher.group(1));
		} catch (NumberFormatException error) {}
		if (value == null) {
			matcher = MzTabConstants.SCAN_ID_PATTERN.matcher(scan);
			if (matcher.find()) try {
				value = Integer.parseInt(matcher.group(1));
			} catch (NumberFormatException error) {}
		}
		// then try to parse scan number as a plain integer
		if (value == null) try {
			value = Integer.parseInt(scan);
		} catch (NumberFormatException error) {}
		// if the scan column value is parsable as a positive integer,
		// then we can assume that the file uses scan numbers
		if (value != null && value > 0)
			return true;
		// otherwise, try to parse index as a nativeID
		value = null;
		matcher = MzTabConstants.INDEX_PATTERN.matcher(index);
		if (matcher.find()) try {
			value = Integer.parseInt(matcher.group(1));
		} catch (NumberFormatException error) {}
		// then try to parse index as a plain integer
		if (value == null) try {
			value = Integer.parseInt(index);
		} catch (NumberFormatException error) {}
		// if the index column value is parsable as a valid integer,
		// then we can assume that the file uses spectrum indices
		if (value != null) {
			// verify the validity of the parsed index based on
			// whether or not 0 is considered a valid index
			if ((zeroBased && value >= 0) ||
				(zeroBased == false && value > 0))
				return false;
		}
		// the "index" column may in fact be a generic nativeID column,
		// e.g. MS-GF+. In this case, check for known scan nativeID formats
		value = null;
		matcher = MzTabConstants.SCAN_PATTERN.matcher(index);
		if (matcher.find()) try {
			value = Integer.parseInt(matcher.group(1));
		} catch (NumberFormatException error) {}
		if (value == null) {
			matcher = MzTabConstants.SCAN_ID_PATTERN.matcher(index);
			if (matcher.find()) try {
				value = Integer.parseInt(matcher.group(1));
			} catch (NumberFormatException error) {}
		}
		if (value != null && value > 0)
			throw new TypeConstraintException(String.format(
				"Found scan number nativeID [%s] in index column.", index));
		// if neither column could be validated, then the
		// row is bad and therefore so is the file
		throw new IllegalArgumentException(String.format("Could not " +
			"determine the spectrum ID type of the input TSV file, since the " +
			"values of neither the scan column [%s] nor the index column " +
			"[%s] in the first data row were valid for their type.",
			scan, index));
	}
	
	private void addModMasses(String psm) {
		if (psm == null)
			return;
		// ensure that found mods collection is initialized
		if (foundMods == null)
			foundMods = new LinkedHashSet<Double>();
		// extract all mass values from this string
		Matcher matcher = MzTabConstants.SIMPLE_FLOAT_PATTERN.matcher(psm);
		while (matcher.find()) {
			try { foundMods.add(Double.parseDouble(matcher.group(1))); }
			catch (NumberFormatException error) {}
		}
	}
	
	/**
	 * @param terminal	true N-term, false C-term, null neither
	 */
	private void addDynamicMod(
		String cvTerm, String aminoAcids, String mass,
		boolean fixed, Boolean terminal
	) {
		// validate this mod's CV term
		if (cvTerm == null)
			return;
		Matcher matcher = MzTabConstants.CV_TERM_PATTERN.matcher(cvTerm);
		if (matcher.matches() == false)
			throw new IllegalStateException();
		// extract this mod's accession
		String accession = matcher.group(2);
		// if this is a fixed mod, then the fact that it is being added
		// dynamically means that it was actually selected by the user in
		// the input form; note this, so we can later cull the hard-coded
		// static fixed mods that were not selected by the user
		if (fixed)
			dynamicFixedModAccessions.add(accession);
		// if this mod has already been added statically,
		// do not add it again dynamically
		if (staticModAccessions.contains(accession))
			return;
		// determine how to process this mass to actually
		// match the mods that occur in the TSV file
		if (matchMods == false) {
			// do nothing; the literal mass string provided
			// must occur as-is in the file to be matched
		} else if (mass != null) {
			Double massValue = null;
			try { massValue = Double.parseDouble(mass); }
			catch (NumberFormatException error) {}
			if (massValue != null) {
				// if a precision was provided, then round
				// the argument mass to that precision
				if (massPrecisionToMatch != null) {
					StringBuilder formatString = new StringBuilder("#");
					if (massPrecisionToMatch > 0) {
						formatString.append(".");
						for (int i=0; i<massPrecisionToMatch; i++)
							formatString.append("#");
					}
					DecimalFormat format =
						new DecimalFormat(formatString.toString());
					format.setRoundingMode(RoundingMode.HALF_UP);
					mass = format.format(massValue);
				}
				// otherwise, if a difference threshold was provided, then
				// try to match this mod to the closest found mod
				else if (maxMassDifferenceToMatch != null) {
					// look at all found mods, find the closest one
					Double smallestDifference = null;
					Double bestMatch = null;
					for (Double foundMod : foundMods) {
						double difference = Math.abs(massValue - foundMod);
						if (smallestDifference == null ||
							smallestDifference > difference) {
							smallestDifference = difference;
							bestMatch = foundMod;
						}
					}
					// only take the best match if it's within some
					// reasonable difference of the declared mass
					if (bestMatch != null && smallestDifference != null &&
						smallestDifference <= maxMassDifferenceToMatch) {
						mass = Double.toString(bestMatch);
						// remove this found mass from the map, so we
						// can handle whatever's left separately
						foundMods.remove(mass);
					}
				}
			}
		}
		// build a separate mod CV term string for each possible mod format
		for (String modPattern : modPatterns) {
			String modCVTerm = String.format(cvTerm, getModFormatString(
				modPattern, aminoAcids, mass,
				fixed && (fixedModsReported == false), terminal));
			if (fixed)
				fixedMods.add(modCVTerm);
			else {
				variableMods.add(modCVTerm);
				// record this mod mass, if present, so we can assign
				// other floating masses to this mod if close enough
				try {
					variableModMasses.put(
						Double.parseDouble(mass),
						new String[]{
							cvTerm, aminoAcids,
							terminal == null ? "null" :
								Boolean.toString(terminal)
						}
					);
				} catch (Throwable error) {}
			}
		}
	}
	
	private void addStaticMod(String specifier, boolean fixed) {
		if (specifier == null)
			return;
		// split specifier into accession and pattern
		int pipe = specifier.indexOf('|');
		if (pipe < 1)
			throw new IllegalArgumentException(String.format(
				"Modification specifier [%s] does not conform to the " +
				"required format:\n%s", specifier, "<accession>|<pattern>"));
		String accession = specifier.substring(0, pipe);
		String pattern = specifier.substring(pipe + 1);
		// extract mod ontology label from the accession
		String[] tokens = accession.split(":");
		if (tokens == null || tokens.length != 2)
			throw new IllegalArgumentException(String.format(
				"Modification accession [%s] does not conform to the " +
				"required format of a CV accession string:\n%s",
				accession, "<cvLabel>:<accession>"));
		// get accession name; if one cannot be found,
		// then this is not a valid ontology mod
		String name = OntologyUtils.getOntologyModificationName(accession);
		if (name == null)
			throw new IllegalArgumentException(String.format(
				"Modification accession [%s] does not correspond to any " +
				"known ontology modification.", accession));
		// validate that the argument mod pattern does not contain
		// any quotation marks, since these are used to enclose
		// the pattern as the fourth CV tuple "value" element
		if (pattern.contains("\""))
			throw new IllegalArgumentException(String.format(
				"Modification pattern [%s] is invalid: pattern may not " +
				"contain quotation marks (\").", pattern));
		// build CV term string
		String modCVTerm = String.format("[%s,%s,%s,\"%s\"]",
			tokens[0], accession, name, pattern);
		// add mod to the proper collection
		if (fixed)
			fixedMods.add(modCVTerm);
		else variableMods.add(modCVTerm);
		// note that this mod accession was statically added,
		// so we should not try to add it dynamically
		staticModAccessions.add(accession);
	}
	
	/**
	 * @param terminal	true N-term, false C-term, null neither
	 */
	private String getModFormatString(
		String modPattern, String aminoAcids, String mass,
		boolean fixedModNotReported, Boolean terminal
	) {
		// ensure a valid amino acid set is present
		if (aminoAcids == null)
			aminoAcids = "*";
		else {
			aminoAcids = ProteomicsUtils.cleanPeptide(aminoAcids);
			if (aminoAcids == null || aminoAcids.trim().isEmpty())
				aminoAcids = "*";
		}
		// if this is a fixed mod, and it's not explicitly shown
		// in the TSV  file, just return the amino acid set
		if (fixedModNotReported)
			return aminoAcids;
		// otherwise, ensure a valid mod pattern is present
		if (modPattern == null)
			modPattern = "#";
		// insert the amino acid set into the proper
		// place in the mod format string
		String modFormatString = null;
		// if there is no placeholder for amino acids in the mod pattern, then
		// just put the amino acid set in the proper place for this mod type
		if (modPattern.indexOf('*') < 0) {
			// normal (i.e. not terminal) or C-term mods
			if (terminal == null || terminal == false)
				modFormatString = String.format("%s%s", aminoAcids, modPattern);
			// N-term mods
			else modFormatString =
				String.format("%s%s", modPattern, aminoAcids);
		}
		// otherwise splice in the amino acid set where the placeholder is
		else modFormatString = modPattern.replaceFirst("\\*", aminoAcids);
		// add the proper terminal regex character, if applicable
		if (terminal != null) {
			// N-term mods
			if (terminal)
				modFormatString = String.format("^%s", modFormatString);
			// C-term mods
			else modFormatString = String.format("%s$", modFormatString);
		}
		// if there is no mass string, then this is
		// a generic mod and can be returned as-is
		if (mass == null)
			return modFormatString;
		// if there is no placeholder for mass in the mod pattern,
		// then put the mass immediately after the amino acids
		else if (modFormatString.indexOf('#') < 0) {
			// when the mass comes immediately after the amino acids,
			// then by convention add a "+" before the mass value,
			// unless the mass is already modified by a sign character
			if (mass.startsWith("+") == false && mass.startsWith("-") == false)
				mass = String.format("+%s", mass);
			int aminoAcidPosition = modFormatString.indexOf(aminoAcids);
			modFormatString = String.format("%s%s%s%s",
				modFormatString.substring(0, aminoAcidPosition), aminoAcids,
				mass, modFormatString.substring(
					aminoAcidPosition + aminoAcids.length(),
					modFormatString.length()));
		}
		// otherwise splice in the mass where the placeholder is
		else {
			// if the placeholder is at the end of the mod pattern,
			// then by convention add a "+" before the mass value,
			// unless the mass is already modified by a sign character
			if (modPattern.endsWith("#") &&
				mass.startsWith("+") == false && mass.startsWith("-") == false)
				mass = String.format("+%s", mass);
			modFormatString = modFormatString.replaceFirst("#", mass);
		}
		// if the format string is just "*#", where "#" is the mass, then
		// remove the star ("*") to account for N-term mods where the mass
		// string may occur before the amino acid
		if (modFormatString.equals(String.format("*%s", mass)))
			modFormatString = mass;
		return modFormatString;
	}
	
	/*========================================================================
	 * Static application methods
	 *========================================================================*/
	public static void main(String[] args) {
		TSVToMzTabParamGenerator generator = extractArguments(args);
		if (generator == null)
			die(USAGE);
		else generator.writeParameterFile();
	}
	
	private static TSVToMzTabParamGenerator extractArguments(String[] args) {
		if (args == null || args.length < 1)
			return null;
		// extract file arguments
		File tsvFile = null;
		File inputParams = null;
		File paramsFile = null;
		String hasHeader = null;
		String filenameColumn = null;
		Integer maxFilenames = null;
		String sequenceColumn = null;
		String fixedModsReported = null;
		String specIDType = null;
		String scanColumn = null;
		String indexColumn = null;
		String indexNumbering = null;
		String accessionColumn = null;
		String chargeColumn = null;
		String experimentalMZColumn = null;
		String calculatedMZColumn = null;
		String msgfEValueColumn = null;
		String msgfSpecEValueColumn = null;
		String msgfQValueColumn = null;
		String msgfPepQValueColumn = null;
		String massPrecisionToMatch = null;
		String maxMassDifferenceToMatch = null;
		Collection<String> modPatterns = new LinkedHashSet<String>();
		Collection<String> fixedMods = new LinkedHashSet<String>();
		Collection<String> variableMods = new LinkedHashSet<String>();
		for (int i=0; i<args.length; i++) {
			String argument = args[i];
			if (argument == null)
				return null;
			else {
				i++;
				if (i >= args.length)
					return null;
				String value = args[i];
				if (argument.equalsIgnoreCase("-tsv"))
					tsvFile = new File(value);
				else if (argument.equalsIgnoreCase("-params"))
					inputParams = new File(value);
				else if (argument.equalsIgnoreCase("-output"))
					paramsFile = new File(value);
				else if (argument.equalsIgnoreCase("-header_line"))
					hasHeader = value;
				else if (argument.equalsIgnoreCase("-filename"))
					filenameColumn = value;
				else if (argument.equalsIgnoreCase("-max_filenames")) try {
					maxFilenames = Integer.parseInt(value);
					if (maxFilenames < 1)
						maxFilenames = null;
				} catch (NumberFormatException error) {}
				else if (argument.equalsIgnoreCase("-modified_sequence"))
					sequenceColumn = value;
				else if (argument.equalsIgnoreCase("-fixed_mods_reported"))
					fixedModsReported = value;
				else if (argument.equalsIgnoreCase("-spectrum_id_type"))
					specIDType = value;
				else if (argument.equalsIgnoreCase("-index_numbering"))
					indexNumbering = value;
				else if (argument.equalsIgnoreCase("-scan"))
					scanColumn = value;
				else if (argument.equalsIgnoreCase("-index"))
					indexColumn = value;
				else if (argument.equalsIgnoreCase("-accession"))
					accessionColumn = value;
				else if (argument.equalsIgnoreCase("-charge"))
					chargeColumn = value;
				else if (argument.equalsIgnoreCase("-exp_mass_to_charge"))
					experimentalMZColumn = value;
				else if (argument.equalsIgnoreCase("-calc_mass_to_charge"))
					calculatedMZColumn = value;
				else if (argument.equalsIgnoreCase("-msgf_evalue"))
					msgfEValueColumn = value;
				else if (argument.equalsIgnoreCase("-msgf_spec_evalue"))
					msgfSpecEValueColumn = value;
				else if (argument.equalsIgnoreCase("-msgf_qvalue"))
					msgfQValueColumn = value;
				else if (argument.equalsIgnoreCase("-msgf_pep_qvalue"))
					msgfPepQValueColumn = value;
				else if (argument.equalsIgnoreCase("-match_mass_precision"))
					massPrecisionToMatch = value;
				else if (argument.equalsIgnoreCase("-match_mass_difference"))
					maxMassDifferenceToMatch = value;
				else if (argument.equalsIgnoreCase("-mod_pattern"))
					modPatterns.add(value);
				else if (argument.equalsIgnoreCase("-fixed_mod"))
					fixedMods.add(value);
				else if (argument.equalsIgnoreCase("-variable_mod"))
					variableMods.add(value);
				else throw new IllegalArgumentException(String.format(
					"Unrecognized parameter at index %d: [%s]", i, argument));
			}
		}
		// process extracted file arguments into an
		// initialized parameter file generator
		try {
			return new TSVToMzTabParamGenerator(inputParams, tsvFile,
				paramsFile, hasHeader, filenameColumn, maxFilenames,
				sequenceColumn, fixedModsReported, specIDType, scanColumn,
				indexColumn, indexNumbering, accessionColumn, chargeColumn,
				experimentalMZColumn, calculatedMZColumn,
				msgfEValueColumn, msgfSpecEValueColumn,
				msgfQValueColumn, msgfPepQValueColumn,
				massPrecisionToMatch, maxMassDifferenceToMatch,
				modPatterns, fixedMods, variableMods);
		} catch (IOException error) {
			throw new RuntimeException(error);
		}
	}
	
	/*========================================================================
	 * Static application convenience methods
	 *========================================================================*/
	private static void die(String message) {
		die(message, null);
	}
	
	private static void die(String message, Throwable error) {
		if (message == null)
			message = "There was an error generating the " +
				"TSV to mzTab converter parameters file";
		if (error != null)
			message += ":";
		else if (message.endsWith(".") == false)
			message += ".";
		System.err.println(message);
		if (error != null)
			error.printStackTrace();
		System.exit(1);
	}
}
