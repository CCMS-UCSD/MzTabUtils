package edu.ucsd.mztab;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.xpath.XPathAPI;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import uk.ac.ebi.pride.jmztab.model.Modification;
import edu.ucsd.mztab.model.ModificationParse;
import edu.ucsd.mztab.util.CommonUtils;
import edu.ucsd.mztab.util.FileIOUtils;
import edu.ucsd.mztab.util.ProteomicsUtils;

public class TSVToMzTabParameters
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String USAGE = "---------- Usage: ----------\n" +
		"java -cp MzTabUtils.jar edu.ucsd.mztab.TSVToMzTabParameters " +
		"\n\t-input  <ProteoSAFeParametersFile>" +
		"\n\t-output <ConverterParametersFile>";
	private static final String[] REQUIRED_COLUMNS = {
		"filename", "spectrum_id", "modified_sequence"
	};
	
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private File                       tsvFile;
	private File                       mzTabFile;
	private boolean                    hasHeader;
	private boolean                    scanMode;
	private boolean                    zeroBased;
	private boolean                    fixedModsReported;
	private Map<String, Integer>       columnIndices;
	private Map<String, Integer>       extraColumns;
	private List<String>               psmScores;
	private Collection<ModRecord>      modifications;
	private Collection<URL>            spectrumFiles;
	private Map<String, ProteinRecord> proteins;
	private Map<String, PeptideRecord> peptides;
	
	/*========================================================================
	 * Constructors
	 *========================================================================*/
	public TSVToMzTabParameters(
		File paramsFile, File tsvFile, File mzTabDirectory
	) throws IOException {
		System.out.println("Processing converter parameters...");
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
		// validate output mzTab directory
		if (mzTabDirectory == null)
			throw new NullPointerException(
				"Output mzTab directory cannot be null.");
		else if (mzTabDirectory.isDirectory() == false)
			throw new IllegalArgumentException(String.format(
				"Output mzTab directory [%s] must be a directory.",
				mzTabDirectory.getName()));
		// generate output mzTab file
		mzTabFile = new File(mzTabDirectory,
			String.format("%s.mzTab", FilenameUtils.getBaseName(filename)));
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
		modifications = new LinkedHashSet<ModRecord>();
		// initialize tab-delimited content parameters
		hasHeader = false;
		scanMode = false;
		zeroBased = true;
		fixedModsReported = false;
		Map<String, String> columns = new LinkedHashMap<String, String>();
		// read all parameters from input parameters file
		System.out.println(String.format(
			"Reading converter parameters file [%s]...",
			paramsFile.getAbsolutePath()));
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
			// set whether spectrum indices in the input file
			// are ordered with 0-based or 1-based numbering
			else if (parameter.equalsIgnoreCase("index_numbering")) {
				if (value.equals("1"))
					zeroBased = false;
				else if (value.equals("0") == false)
					throw new IllegalArgumentException(String.format(
						"Unrecognized \"index_numbering\" value: [%s]", value));
			}
			// set whether fixed mods are explicitly reported in
			// the input TSV file's modified peptide strings
			else if (parameter.equalsIgnoreCase("fixed_mods_reported")) {
				if (value.equalsIgnoreCase("true") ||
					value.equalsIgnoreCase("yes") || value.equals("1"))
					fixedModsReported = true;
				else if (value.equalsIgnoreCase("false") == false &&
					value.equalsIgnoreCase("no") == false &&
					value.equals("0") == false)
					throw new IllegalArgumentException(String.format(
						"Unrecognized \"fixed_mods_reported\" value: [%s]",
						value));
			}
			// add all fixed mods
			else if (parameter.equalsIgnoreCase("fixed_mods")) {
				String[] cvTerms = value.split("\\|");
				if (cvTerms == null || cvTerms.length < 1)
					continue;
				else for (int i=0; i<cvTerms.length; i++)
					addFixedMod(cvTerms[i], fixedModsReported);
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
		System.out.println(String.format(
			"Reading input TSV file [%s] to finish setting up converter...",
			this.tsvFile.getAbsolutePath()));
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
			// remove any enclosing quotation marks from column names,
			// e.g. ProteomeDiscoverer output
			for (int i=0; i<elements.length; i++) {
				String cleaned = CommonUtils.stripQuotation(elements[i]);
				if (cleaned != null && cleaned.equals(elements[i]) == false)
					elements[i] = cleaned;
			}
			// validate all columns extracted from the parameters file
			columnIndices = new LinkedHashMap<String, Integer>(columns.size());
			psmScores = new ArrayList<String>(4);
			for (String column : columns.keySet()) {
				String columnID = columns.get(column);
				Integer index = extractColumnIndex(
					columnID, filename, column, line, hasHeader(), elements);
				if (index == null)
					throw new NullPointerException(String.format(
						"There was an error parsing \"%s\" column [%s].",
						column, columnID));
				else columnIndices.put(column, index);
				// collect recognized score columns in ordered list
				if (column.equals("msgf_evalue") ||
					column.equals("msgf_spec_evalue") ||
					column.equals("msgf_qvalue") ||
					column.equals("msgf_pep_qvalue"))
					psmScores.add(column);
			}
			// collect all "extra" columns
			int extraColumnCount = elements.length - columnIndices.size();
			if (extraColumnCount >= 1)
				extraColumns =
					new LinkedHashMap<String, Integer>(extraColumnCount);
			else extraColumns = new LinkedHashMap<String, Integer>();
			for (int i=0; i<elements.length; i++) {
				// skip standard mzTab columns, since they're already
				// recorded; however, keep any known TSV columns that
				// do not correspond to any standard mzTab column
				if (columnIndices.containsValue(i)) {
					// get this column's name
					String column = null;
					for (String name : columnIndices.keySet()) {
						if (columnIndices.get(name) == i) {
							column = name;
							break;
						}
					}
					// column name should never be null since this
					// index was found in the set of map values
					if (column == null)
						throw new IllegalStateException();
					// only skip this column if it does not belong to the
					// set of known columns that should be recorded as extras
					else if (column.equals("psm_q_value") == false &&
						column.equals("protein_q_value") == false &&
						column.equals("peptide_q_value") == false)
						continue;
				}
				if (hasHeader())
					extraColumns.put(elements[i], i);
				else extraColumns.put(String.format("column_%d", i + 1), i);
			}
			// read all PSM rows, to collect spectrum filenames and to validate
			// each row for complete inclusion of all registered column indices
			spectrumFiles = new LinkedHashSet<URL>();
			Integer accessionIndex = columnIndices.get("accession");
			if (accessionIndex != null) {
				proteins = new LinkedHashMap<String, ProteinRecord>();
				if (accessionIndex >= elements.length)
					throw new IllegalArgumentException(String.format(
						"Error parsing input TSV file [%s]: the index " +
						"of the \"%s\" column was given as %d, but line " +
						"%d of the file contains only %d elements:\n%s",
						filename, "accession", accessionIndex, lineNumber,
						elements.length, line));
			}
			while ((line = reader.readLine()) != null) {
				// parse out the elements of the line
				elements = line.split("\t");
				if (elements == null || elements.length < 1)
					throw new IllegalArgumentException(
						String.format("Could not parse the tab-delimited " +
							"elements of line %d from input TSV file [%s].",
							lineNumber, filename));
				// remove any enclosing quotation marks from column values,
				// e.g. ProteomeDiscoverer output
				for (int i=0; i<elements.length; i++) {
					String cleaned = CommonUtils.stripQuotation(elements[i]);
					if (cleaned != null && cleaned.equals(elements[i]) == false)
						elements[i] = cleaned;
				}
				// validate this line against the registered column
				// indices and determine its "ms_run" index
				Integer msRun = null;
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
						else {
							spectrumFiles.add(url);
							// determine the "ms_run" index of this file
							int i = 0;
							for (URL file : spectrumFiles) {
								i++;
								if (file.equals(url)) {
									msRun = i;
									break;
								}
							}
						}
					}
				}
				// update statistics for this row's protein accession
				if (accessionIndex != null) {
					// every row should have a filename column value
					if (msRun == null)
						throw new IllegalStateException();
					// split accession column value into separate
					// proteins, since they may be "rolled up"
					String[] accessions = elements[accessionIndex].split(";");
					for (String accession : accessions) {
						// get protein record for this accession
						ProteinRecord record = proteins.get(accession);
						if (record == null)
							record = new ProteinRecord(accession);
						// add this PSM to this protein
						record.addPSM(msRun);
						// get this row's peptide and add it to this protein
						String peptide =
							elements[columnIndices.get("modified_sequence")];
						record.addPeptide(
							msRun, ProteomicsUtils.cleanPeptide(peptide));
						// get this row's modifications
						// and add them to this protein
						ModificationParse extracted =
							TSVToMzTabConverter.extractPTMsFromPSM(
								peptide, getModifications());
						Collection<Modification> mods =
							extracted.getModificationOccurrences();
						if (mods != null)
							for (Modification mod : mods)
								record.addModification(mod);
						proteins.put(accession, record);
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
		System.out.println("Done processing converter parameters.");
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
	
	public boolean isZeroBased() {
		return zeroBased;
	}
	
	public Collection<ModRecord> getModifications() {
		return modifications;
	}
	
	public ModRecord getModification(double mass) {
		if (modifications == null)
			return null;
		for (ModRecord mod : modifications)
			if (mod.getMass() == mass)
				return mod;
		return null;
	}
	
	public void addFixedMod(String cvTerm, boolean reported) {
		addMod(cvTerm, true, reported);
	}
	
	public void addVariableMod(String cvTerm) {
		addMod(cvTerm, false, true);
	}
	
	public Collection<URL> getSpectrumFiles() {
		return spectrumFiles;
	}
	
	public Map<String, ProteinRecord> getProteins() {
		return proteins;
	}
	
	public Map<String, PeptideRecord> getPeptides() {
		return peptides;
	}
	
	public Collection<String> getColumns() {
		return columnIndices.keySet();
	}
	
	public Integer getColumnIndex(String column) {
		if (column == null)
			return null;
		else return columnIndices.get(column);
	}
	
	public List<String> getPSMScores() {
		return new ArrayList<String>(psmScores);
	}
	
	public Integer getPSMScoreIndex(String column) {
		if (column == null)
			return null;
		int index = psmScores.indexOf(column);
		if (index < 0)
			return null;
		else return index;
	}
	
	public boolean isPSMScore(String column) {
		if (column == null)
			return false;
		else return psmScores.contains(column);
	}
	
	public Collection<String> getExtraColumns() {
		return extraColumns.keySet();
	}
	
	public Integer getExtraColumnIndex(String column) {
		if (column == null)
			return null;
		else return extraColumns.get(column);
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private void addMod(String cvTerm, boolean fixed, boolean reported) {
		if (cvTerm == null)
			return;
		else if (modifications == null)
			modifications = new LinkedHashSet<ModRecord>();
		ModRecord mod = new ModRecord(cvTerm, fixed, reported);
		String pattern = mod.getPattern();
		// it should be impossible for a newly instantiated
		// mod record to have a null pattern
		if (pattern == null)
			throw new IllegalStateException();
		// ensure that a mod with the same regular expression
		// is not already in the collection
		for (ModRecord existing : modifications)
			if (pattern.equals(existing.getPattern()))
				throw new IllegalArgumentException(String.format(
					"Modification identifier regular expression pattern [%s] " +
					"was already found in the modifications collection, " +
					"representing CV term [%s]. Therefore, adding another " +
					"term with the exact same pattern would introduce " +
					"irreconcilable ambiguity.", pattern, cvTerm));
		modifications.add(mod);
	}
	
	/*========================================================================
	 * Static application methods
	 *========================================================================*/
	public static void main(String[] args) {
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
				if (value != null) {
					// if any mod characters are escaped, be sure to double
					// the backslash ("\") character to make Java happy
					value = value.replaceAll("\\\\", "\\\\\\\\");
					output.println(String.format("fixed_mods=%s", value));
				}
			}
			// extract variable mods, and write them to the output file
			parameter = XPathAPI.selectSingleNode(
				document, "//parameter[@name='variable_mods']");
			if (parameter != null) {
				value = parameter.getFirstChild().getNodeValue();
				if (value != null) {
					// if any mod characters are escaped, be sure to double
					// the backslash ("\") character to make Java happy
					value = value.replaceAll("\\\\", "\\\\\\\\");
					output.println(String.format("variable_mods=%s", value));
				}
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
					// skip FDR columns, since we handle them later
					else if (name.equalsIgnoreCase("column.q_value") ||
						name.equalsIgnoreCase("column.pass_threshold") ||
						name.equalsIgnoreCase("column.decoy"))
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
	
	public static Integer extractColumnIndex(
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
			// clean column ID by removing any enclosing quotation marks
			// from column names, e.g. ProteomeDiscoverer output
			String cleanedColumnID = CommonUtils.stripQuotation(columnID);
			// otherwise, try to find the index of the specified column header
			for (int i=0; i<headers.length; i++) {
				if (columnID.equals(headers[i]) ||
					(cleanedColumnID != null &&
					cleanedColumnID.equals(headers[i])))
					return i;
			}
			// if no matching column header was found, then throw an exception
			throw new IllegalArgumentException(String.format(
				"Error parsing input TSV file [%s]: the %s " +
				"column header was given as [%s], but this header could " +
				"not be found in the first line of the file:\n%s",
				tsvFilename, columnName, columnID, line));
		}
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
