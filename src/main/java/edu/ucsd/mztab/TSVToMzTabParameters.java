package edu.ucsd.mztab;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.xpath.XPathAPI;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import edu.ucsd.util.FileIOUtils;
import edu.ucsd.util.OntologyUtils;
import uk.ac.ebi.pride.jmztab.model.CVParam;
import uk.ac.ebi.pride.jmztab.model.FixedMod;
import uk.ac.ebi.pride.jmztab.model.Mod;
import uk.ac.ebi.pride.jmztab.model.VariableMod;

public class TSVToMzTabParameters
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String USAGE = "---------- Usage: ----------\n" +
		"java -cp MzTabUtils.jar edu.ucsd.mztab.TSVToMzTabParameters " +
		"\n\t-input  <ProteoSAFeParametersFile>" +
		"\n\t-output <ConverterParametersFile>";
	private static final Pattern CV_TERM_PATTERN = Pattern.compile(
		"^\\[([^,]*),\\s*([^,]*),\\s*\"?([^\"]*)\"?,\\s*([^,]*)\\]$");
	private static final String[] REQUIRED_COLUMNS = {
		"filename", "spectrum_id", "modified_sequence"
	};
	public static final Map<Character, Double> AMINO_ACID_MASSES =
		new TreeMap<Character, Double>();
	static {
		AMINO_ACID_MASSES.put('A', 71.037113787);
		AMINO_ACID_MASSES.put('R', 156.101111026);
		AMINO_ACID_MASSES.put('D', 115.026943031);
		AMINO_ACID_MASSES.put('N', 114.042927446);
		AMINO_ACID_MASSES.put('C', 103.009184477);
		AMINO_ACID_MASSES.put('E', 129.042593095);
		AMINO_ACID_MASSES.put('Q', 128.058577510);
		AMINO_ACID_MASSES.put('G', 57.021463723);
		AMINO_ACID_MASSES.put('H', 137.058911861);
		AMINO_ACID_MASSES.put('I', 113.084063979);
		AMINO_ACID_MASSES.put('L', 113.084063979);
		AMINO_ACID_MASSES.put('K', 128.094963016);
		AMINO_ACID_MASSES.put('M', 131.040484605);
		AMINO_ACID_MASSES.put('F', 147.068413915);
		AMINO_ACID_MASSES.put('P', 97.052763851);
		AMINO_ACID_MASSES.put('S', 87.032028409);
		AMINO_ACID_MASSES.put('T', 101.047678473);
		AMINO_ACID_MASSES.put('W', 186.079312952);
		AMINO_ACID_MASSES.put('Y', 163.063328537);
		AMINO_ACID_MASSES.put('V', 99.068413915);
	}
	
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private File                 tsvFile;
	private File                 mzTabFile;
	private boolean              hasHeader;
	private boolean              scanMode;
	private Map<String, Integer> columnIndices;
	private Map<Double, Mod>     modifications;
	private Collection<URL>      spectrumFiles;
	
	/*========================================================================
	 * Constructors
	 *========================================================================*/
	public TSVToMzTabParameters(
		File paramsFile, File tsvFile, File mzTabFile
	) throws IOException {
		// validate input parameter file
		if (paramsFile == null)
			throw new NullPointerException(
				"Input parameter file cannot be null.");
		else if (paramsFile.isFile() == false)
			throw new IllegalArgumentException(String.format(
				"Input parameter file [%s] must be a normal " +
				"(non-directory) file.", paramsFile.getName()));
		else if (paramsFile.canRead() == false)
			throw new IllegalArgumentException(String.format(
				"Input parameter file [%s] must be readable.",
				paramsFile.getName()));
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
		// validate output mzTab file
		if (mzTabFile == null)
			throw new NullPointerException(
				"Output mzTab file cannot be null.");
		else if (mzTabFile.isDirectory())
			throw new IllegalArgumentException(
				String.format("Output mzTab file [%s] " +
					"must be a normal (non-directory) file.",
					mzTabFile.getName()));
		else this.mzTabFile = mzTabFile;
		// attempt to create output file and test its writeability
		boolean writeable = true;
		if (mzTabFile.exists())
			writeable = mzTabFile.delete();
		if (writeable)
			writeable = mzTabFile.createNewFile() && mzTabFile.canWrite();
		if (writeable == false)
			throw new IllegalArgumentException(
				String.format("Output mzTab file [%s] must be writable.",
					mzTabFile.getName()));
		// initialize mod collection
		modifications = new LinkedHashMap<Double, Mod>();
		// initialize tab-delimited content parameters
		hasHeader = false;
		scanMode = false;
		Map<String, String> columns = new LinkedHashMap<String, String>();
		// read all parameters from input parameters file
		Properties properties = new Properties();
		properties.load(new FileInputStream(paramsFile));
		for (String parameter : properties.stringPropertyNames()) {
			if (parameter == null)
				continue;
			else parameter = parameter.trim();
			String value = properties.getProperty(parameter);
			if (value == null)
				continue;
			else value = value.trim();
			// set whether there is a header line in the input file
			if (parameter.equalsIgnoreCase("header_line")) {
				if (value.equalsIgnoreCase("true") ||
					value.equalsIgnoreCase("yes") || value.equals("1"))
					hasHeader = true;
				else if (value.equalsIgnoreCase("false") == false &&
					value.equalsIgnoreCase("no") == false &&
					value.equals("0") == false)
					throw new IllegalArgumentException(String.format(
						"Unrecognized \"header_line\" value: [%s]", value));
			}
			// set whether spectrum IDs in the input file are scans or indices
			else if (parameter.equalsIgnoreCase("spectrum_id_type")) {
				if (value.equalsIgnoreCase("scan"))
					scanMode = true;
				else if (value.equalsIgnoreCase("index") == false)
					throw new IllegalArgumentException(String.format(
						"Unrecognized \"spectrum_id_type\" value: [%s]",
						value));
			}
			// add all fixed mods
			else if (parameter.equalsIgnoreCase("fixed_mods")) {
				String[] cvTerms = value.split("\\|");
				if (cvTerms == null || cvTerms.length < 1)
					continue;
				else for (int i=0; i<cvTerms.length; i++)
					addFixedMod(cvTerms[i]);
			}
			// add all variable mods
			else if (parameter.equalsIgnoreCase("variable_mods")) {
				String[] cvTerms = value.split("\\|");
				if (cvTerms == null || cvTerms.length < 1)
					continue;
				else for (int i=0; i<cvTerms.length; i++)
					addVariableMod(cvTerms[i]);
			}
			// add all column identifiers
			else columns.put(parameter, value);
		}
		// validate extracted columns to ensure that all required ones were set
		for (String column : REQUIRED_COLUMNS)
			if (columns.containsKey(column) == false)
				throw new IllegalArgumentException(String.format(
					"An index or header string for column \"%s\" is required.",
					column));
		// parse and process the input tab-delimited result file
		BufferedReader reader = null;
		String line = null;
		int lineNumber = -1;
		try {
			// read the first line of the file
			reader = new BufferedReader(new FileReader(this.tsvFile));
			reader.mark(10000);
			line = reader.readLine();
			// if the first line is not supposed to be a header line, rewind
			if (hasHeader() == false) {
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
			// validate all columns extracted from the parameters file
			columnIndices = new LinkedHashMap<String, Integer>(3);
			for (String column : columns.keySet()) {
				String columnID = columns.get(column);
				Integer index = extractColumnIndex(
					columnID, filename, column, line, hasHeader(), elements);
				if (index == null)
					throw new NullPointerException(String.format(
						"There was an error parsing \"%s\" column [%s].",
						column, columnID));
				else columnIndices.put(column, index);
			}
			// read all PSM rows, to collect spectrum filenames and to validate
			// each row for complete inclusion of all registered column indices
			spectrumFiles = new LinkedHashSet<URL>();
			while ((line = reader.readLine()) != null) {
				// parse out the elements of the line
				elements = line.split("\t");
				if (elements == null || elements.length < 1)
					throw new IllegalArgumentException(
						String.format("Could not parse the tab-delimited " +
							"elements of line %d from input TSV file [%s].",
							lineNumber, filename));
				// validate this line against the registered column indices
				for (String column : columnIndices.keySet()) {
					// the specified index must be within the bounds
					// of the elements array
					int index = columnIndices.get(column);
					if (index >= elements.length)
						throw new IllegalArgumentException(String.format(
							"Error parsing input TSV file [%s]: the index " +
							"of the \"%s\" column was given as %d, but line " +
							"%d of the file contains only %d elements:\n%s",
							filename, column, index, lineNumber,
							elements.length, line));
					// get filename and add it to the set of found files
					else if (column.equals("filename")) {
						String spectrumFilename = elements[index];
						URL url =
							TSVToMzTabConverter.getFileURL(spectrumFilename);
						if (url == null)
							throw new RuntimeException(String.format(
								"Could not generate a valid file URL for " +
								"spectrum file [%s].", spectrumFilename));
						else spectrumFiles.add(url);
					}
				}
				lineNumber++;
			}
		} catch (Throwable error) {
			throw new RuntimeException(String.format("There was an error " +
				"parsing the input tab-delimited result file, line %d:\n%s",
				lineNumber, error.getMessage()), error);
		} finally {
			try { reader.close(); }
			catch (Throwable error) {}
		}
	}
	
	/*========================================================================
	 * Property accessor methods
	 *========================================================================*/
	public File getTSVFile() {
		return tsvFile;
	}
	
	public File getMzTabFile() {
		return mzTabFile;
	}
	
	public boolean hasHeader() {
		return hasHeader;
	}
	
	public boolean isScanMode() {
		return scanMode;
	}
	
	public Map<Double, Mod> getModifications() {
		return modifications;
	}
	
	public Mod getModification(double mass) {
		return modifications.get(mass);
	}
	
	public void addFixedMod(String cvTerm) {
		addMod(cvTerm, true);
	}
	
	public void addVariableMod(String cvTerm) {
		addMod(cvTerm, false);
	}
	
	public Collection<URL> getSpectrumFiles() {
		return spectrumFiles;
	}
	
	public Collection<String> getColumns() {
		return columnIndices.keySet();
	}
	
	public Integer getColumnIndex(String column) {
		if (column == null)
			return null;
		else return columnIndices.get(column);
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private void addMod(String cvTerm, boolean fixed) {
		if (cvTerm == null)
			return;
		else if (modifications == null)
			modifications = new LinkedHashMap<Double, Mod>();
		Mod mod = null;
		// determine the current counts of fixed and variable mods
		int fixedCount = 0;
		int variableCount = 0;
		for (Mod added : modifications.values()) {
			if (added instanceof FixedMod)
				fixedCount++;
			else if (added instanceof VariableMod)
				variableCount++;
			else throw new IllegalStateException();
		}
		if (fixed) {
			mod = new FixedMod(fixedCount + 1);
		} else mod = new VariableMod(variableCount + 1);
		// parse CV term from the argument square bracket-enclosed tuple string
		Matcher matcher = CV_TERM_PATTERN.matcher(cvTerm);
		if (matcher.matches() == false)
			throw new IllegalArgumentException(String.format(
				"Argument CV term [%s] does not conform to the required " +
				"string format of a square bracket-enclosed (\"[]\") " +
				"CV tuple:\n%s", cvTerm, "[cvLabel, accession, name, value]"));
		// try to determine this mod's mass
		// by looking it up in the ontology
		Double mass = null;
		String value = null;
		ImmutablePair<Double, String> ontologyEntry =
			OntologyUtils.getOntologyModification(matcher.group(2));
		if (ontologyEntry != null)
			mass = ontologyEntry.getKey();
		// if this mod was not found in the ontology,
		// try to extract and parse its "value" field
		else try {
			// record the "value" field in the transferred CV parameter
			// if it cannot be looked up in the ontology
			value = matcher.group(4);
			mass = Double.parseDouble(value);
		} catch (NumberFormatException error) {}
		if (mass == null)
			throw new IllegalArgumentException(String.format(
				"No valid mass offset could be determined for the " +
				"modification represented by CV term [%s]. Therefore, adding " +
				"this term to the output mzTab file would introduce " +
				"irreconcilable ambiguity.", cvTerm));
		mod.setParam(new CVParam(
			matcher.group(1), matcher.group(2), matcher.group(3), value));
		// ensure that a mod with this mass is not already in this map
		if (modifications.containsKey(mass)) {
			Mod existing = modifications.get(mass);
			throw new IllegalArgumentException(String.format(
				"A mass offset of %s was already found in the modifications " +
				"map, representing CV term [%s]. Therefore, adding another " +
				"term with the exact same mass offset [%s] to the output " +
				"mzTab file would introduce irreconcilable ambiguity.", mass,
				existing.getParam().toString(), mod.getParam().toString()));
		} else modifications.put(mass, mod);
	}
	
	private Integer extractColumnIndex(
		String columnID, String tsvFilename, String columnName, String line,
		boolean header, String[] headers
	) {
		if (columnID == null || tsvFilename == null || columnName == null ||
			line == null || headers == null)
			return null;
		// first try to parse the user-supplied columnID as an integer index
		try {
			int index = Integer.parseInt(columnID);
			// the specified index must be within the bounds of the header array
			if (index < 0 || index >= headers.length)
				throw new IllegalArgumentException(String.format(
					"Error parsing input TSV file [%s]: the index of the " +
					"%s column was given as %s, but the first " +
					"line of the file contains %d elements:\n%s",
					tsvFilename, columnName, columnID, headers.length, line));
			else return index;
		}
		// if the user-supplied value of the column ID was not an
		// integer, then it must be the string name of a column header
		catch (NumberFormatException error) {
			// if there is no header row, then a column ID that cannot be
			// parsed as an integer is illegal, since it can't be looked up
			if (header == false)
				throw new IllegalArgumentException(String.format(
					"Error parsing input TSV file [%s]: the %s column header " +
					"was given as [%s], but the converter parameters did " +
					"not indicate that the file contains a header line.",
					tsvFilename, columnName, columnID));
			// otherwise, try to find the index of the specified column header
			for (int i=0; i<headers.length; i++)
				if (columnID.equals(headers[i]))
					return i;
			// if no matching column header was found, then throw an exception
			throw new IllegalArgumentException(String.format(
				"Error parsing input TSV file [%s]: the %s " +
				"column header was given as [%s], but this header could " +
				"not be found in the first line of the file:\n%s",
				tsvFilename, columnName, columnID, line));
		}
	}
	
	/*========================================================================
	 * Static application methods
	 *========================================================================*/
	public static void mainy(String[] args) {
		ImmutablePair<File, File> files = extractArguments(args);
		if (files == null)
			die(USAGE);
		// parse params.xml file, convert to TSV converter key=value params file
		PrintWriter output = null;
		try {
			System.out.println(String.format(
				"Reading input XML parameters file [%s]...",
				files.getLeft().getName()));
			// parse input file as an XML document
			Document document = FileIOUtils.parseXML(files.getLeft());
			if (document == null)
				throw new NullPointerException(
					"Parameters XML document could not be parsed.");
			// open output file for writing
			System.out.println(String.format(
				"Writing output text parameters file [%s]...",
				files.getRight().getName()));
			output = new PrintWriter(files.getRight());
			// extract header line status, and write it to the output file
			Node parameter = XPathAPI.selectSingleNode(
				document, "//parameter[@name='header_line']");
			String name = null;
			String value = null;
			if (parameter != null) {
				value = parameter.getFirstChild().getNodeValue();
				if (value != null && value.equalsIgnoreCase("on"))
					output.println("header_line=true");
				else output.println("header_line=false");
			}
			// extract scan/index mode, and write it to the output file
			parameter = XPathAPI.selectSingleNode(
				document, "//parameter[@name='spectrum_id_type']");
			if (parameter != null) {
				value = parameter.getFirstChild().getNodeValue();
				if (value != null && value.equalsIgnoreCase("scan"))
					output.println("spectrum_id_type=scan");
				else output.println("spectrum_id_type=index");
			}
			// extract fixed mods, and write them to the output file
			parameter = XPathAPI.selectSingleNode(
				document, "//parameter[@name='fixed_mods']");
			if (parameter != null) {
				value = parameter.getFirstChild().getNodeValue();
				if (value != null)
					output.println(String.format("fixed_mods=%s", value));
			}
			// extract variable mods, and write them to the output file
			parameter = XPathAPI.selectSingleNode(
				document, "//parameter[@name='variable_mods']");
			if (parameter != null) {
				value = parameter.getFirstChild().getNodeValue();
				if (value != null)
					output.println(String.format("variable_mods=%s", value));
			}
			// extract all column index identifiers,
			// and write them to the output file
			NodeList parameters = XPathAPI.selectNodeList(
				document, "//parameter[starts-with(@name,'column.')]");
			if (parameters != null && parameters.getLength() > 0) {
				for (int i=0; i<parameters.getLength(); i++) {
					parameter = parameters.item(i);
					name = parameter.getAttributes().getNamedItem("name")
						.getNodeValue();
					value = parameter.getFirstChild().getNodeValue();
					if (name == null || value == null)
						continue;
					// extract "column." prefix
					String[] tokens = name.split("\\.");
					if (tokens == null || tokens.length < 2)
						continue;
					else output.println(
						String.format("%s=%s", tokens[1], value));
				}
			}
			System.out.println("Done.");
		} catch (Throwable error) {
			die(null, error);
		} finally {
			try { output.close(); }
			catch (Throwable error) {}
		}
	}
	
	public static void main(String[] args) {
		String[] testModIDs = {"STY+80", "(*,-17)", "+40"};
		Pattern[] patterns = new Pattern[testModIDs.length];
		for (int i=0; i<testModIDs.length; i++)
			patterns[i] = parseModIDString(testModIDs[i]);
		String testPSM = "PE+40PT+80I(D,-17)ES+80";
		Matcher matcher = null;
		String cleaned = testPSM;
		for (Pattern pattern : patterns) {
			matcher = pattern.matcher(testPSM);
			while (matcher.find()) {
				System.out.println(String.format("%d-%s",
					findModIndex(testPSM, matcher.group(), matcher.start()),
					pattern.pattern()));
				cleaned = extractMod(cleaned, matcher.group());
				System.out.println(
					String.format("Cleaned string = %s", cleaned));
			}
		}
	}
	
	public static Pattern parseModIDString(String modID)
	throws PatternSyntaxException {
		if (modID == null)
			return null;
		StringBuffer pattern = new StringBuffer();
		Set<Character> foundAminoAcids = null;
		for (int i=0; i<modID.length(); i++) {
			char current = modID.charAt(i);
			// if the current character is an asterisk ("*"), then add all
			// known amino acids to the regular expression for this region
			if (current == '*') {
				if (foundAminoAcids == null)
					foundAminoAcids = new LinkedHashSet<Character>();
				for (char aminoAcid : AMINO_ACID_MASSES.keySet())
					foundAminoAcids.add(aminoAcid);
				continue;
			}
			// if the current character is a standalone amino acid,
			// then add it to the regular expression for this region
			boolean isAminoAcid = AMINO_ACID_MASSES.containsKey(current);
			if (isAminoAcid) {
				if (foundAminoAcids == null)
					foundAminoAcids = new LinkedHashSet<Character>();
				foundAminoAcids.add(current);
				continue;
			}
			// if the current character is not an amino acid, then add any
			// recently found amino acids to the regular expression
			if (foundAminoAcids != null) {
				pattern.append("[");
				for (char aminoAcid : foundAminoAcids)
					pattern.append(aminoAcid);
				pattern.append("]");
				foundAminoAcids = null;
			}
			// add the regex-escaped character to the pattern
			pattern.append(Pattern.quote(Character.toString(current)));
		}
		// if the ID string ended in an amino acid pattern, add it now
		if (foundAminoAcids != null) {
			pattern.append("[");
			for (char aminoAcid : foundAminoAcids)
				pattern.append(aminoAcid);
			pattern.append("]");
		}
		return Pattern.compile(pattern.toString());
	}
	
	public static int findModIndex(String psm, String mod, int start) {
		if (psm == null || mod == null || start < 0 || start >= psm.length())
			return -1;
		// determine if the captured mod contains any amino acids;
		// if not, then we'll have to match the affected site to the
		// amino acid seen most recently before reaching the mod
		// region (or perhaps after, in the case of N-terminal mods).
		boolean aaMissing = true;
		for (int i=0; i<mod.length(); i++) {
			if (AMINO_ACID_MASSES.containsKey(mod.charAt(i))) {
				aaMissing = false;
				break;
			}
		}
		// iterate over the modified peptide string, counting only amino
		// acids until we get to the index of the mod in question
		int index = 0;
		boolean aminoAcidFound = false;
		for (int i=0; i<psm.length(); i++) {
			char current = psm.charAt(i);
			boolean isAminoAcid = AMINO_ACID_MASSES.containsKey(current);
			// increment the index only if we see an amino acid, since we're
			// counting indices only for the clean (unmodified) peptide
			if (isAminoAcid) {
				aminoAcidFound = true;
				index++;
			}
			// if we've reached the mod's start position, then
			// the correct index should be coming up soon
			if (i >= start) {
				// if the mod string contains an amino acid, then this is the
				// affected site; otherwise, if we know that the mod string
				// contains no amino acids, but one has already been seen in
				// the PSM string, then that was the affected site
				if (isAminoAcid || (aaMissing && aminoAcidFound))
					return index;
			}
		}
		// if no matching amino acid could be found, then that must mean that
		// the PSM string was not well-formed, e.g. it contained no amino acid
		// characters at all.  In that case, return -1 to indicate failure.
		return -1;
	}
	
	public static String extractMod(String psm, String mod) {
		if (psm == null|| mod == null)
			return psm;
		// determine mod substring's indices
		int start = psm.indexOf(mod);
		if (start < 0)
			return psm;
		int end = start + mod.length();
		// clean the substring by removing all non-amino acid characters
		StringBuffer cleaned = new StringBuffer();
		for (int i=0; i<mod.length(); i++) {
			char current = mod.charAt(i);
			if (AMINO_ACID_MASSES.containsKey(current))
				cleaned.append(current);
		}
		// splice the cleaned substring into the original string
		return String.format("%s%s%s",
			psm.substring(0, start), cleaned.toString(), psm.substring(end));
	}
	
	/*========================================================================
	 * Static application convenience methods
	 *========================================================================*/
	private static ImmutablePair<File, File> extractArguments(String[] args) {
		if (args == null || args.length < 1)
			return null;
		else try {
			// extract file arguments
			File inputFile = null;
			File outputFile = null;
			for (int i=0; i<args.length; i++) {
				String argument = args[i];
				if (argument == null)
					return null;
				else {
					i++;
					if (i >= args.length)
						return null;
					String value = args[i];
					if (argument.equalsIgnoreCase("-input"))
						inputFile = new File(value);
					else if (argument.equalsIgnoreCase("-output"))
						outputFile = new File(value);
					else throw new IllegalArgumentException(String.format(
						"Unrecognized parameter at index %d: [%s]", i, argument));
				}
			}
			// validate extracted file arguments
			if (inputFile == null)
				throw new NullPointerException(
					"Input parameter file cannot be null.");
			else if (inputFile.isFile() == false)
				throw new IllegalArgumentException(String.format(
					"Input parameter file [%s] must be a normal " +
					"(non-directory) file.", inputFile.getName()));
			else if (inputFile.canRead() == false)
				throw new IllegalArgumentException(String.format(
					"Input parameter file [%s] must be readable.",
					inputFile.getName()));
			else if (outputFile == null)
				throw new NullPointerException(
					"Output parameter file cannot be null.");
			else if (outputFile.isDirectory())
				throw new IllegalArgumentException(String.format(
					"Output parameter file [%s] must be a normal " +
					"(non-directory) file.", outputFile.getName()));
			// attempt to create output file and test its writeability
			boolean writeable = true;
			if (outputFile.exists())
				writeable = outputFile.delete();
			if (writeable)
				writeable = outputFile.createNewFile() && outputFile.canWrite();
			if (writeable == false)
				throw new IllegalArgumentException(String.format(
					"Output parameter file [%s] must be writable.",
					outputFile.getName()));
			else return new ImmutablePair<File, File>(inputFile, outputFile);
		} catch (Throwable error) {
			error.printStackTrace();
			return null;
		}
	}
	
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
