package edu.ucsd.mztab;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.Map;
import java.util.SortedMap;
import java.util.regex.Matcher;

import org.apache.commons.lang3.tuple.ImmutablePair;

import edu.ucsd.util.PeptideUtils;
import uk.ac.ebi.pride.jmztab.model.FixedMod;
import uk.ac.ebi.pride.jmztab.model.MZTabColumnFactory;
import uk.ac.ebi.pride.jmztab.model.MZTabDescription;
import uk.ac.ebi.pride.jmztab.model.Metadata;
import uk.ac.ebi.pride.jmztab.model.Modification;
import uk.ac.ebi.pride.jmztab.model.MsRun;
import uk.ac.ebi.pride.jmztab.model.PSM;
import uk.ac.ebi.pride.jmztab.model.Peptide;
import uk.ac.ebi.pride.jmztab.model.Protein;
import uk.ac.ebi.pride.jmztab.model.Section;
import uk.ac.ebi.pride.jmztab.model.VariableMod;
import uk.ac.ebi.pride.jmztab.utils.convert.ConvertProvider;

public class TSVToMzTabConverter
extends ConvertProvider<File, TSVToMzTabParameters>
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String USAGE = "---------- Usage: ----------\n" +
		"java -cp MzTabUtils.jar edu.ucsd.mztab.TSVToMzTabConverter " +
		"\n\t-tsv    <InputTSVFile>" +
		"\n\t-params <InputParametersFile>" +
		"\n\t-mzTab  <OutputMzTabDirectory>";
	public static final String UNKNOWN_MODIFICATION_ACCESSION = "MS:1001460";
	
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private Metadata           metadata;
	private MZTabColumnFactory psmColumnFactory;
	
	/*========================================================================
	 * Constructor
	 *========================================================================*/
	public TSVToMzTabConverter(TSVToMzTabParameters parameters) {
		super(parameters.getTSVFile(), parameters);
	}
	
	/*========================================================================
	 * ConvertProvider methods
	 *========================================================================*/
	@Override
	protected Metadata convertMetadata() {
		System.out.println("Converting metadata...");
		// ensure that metadata is initialized
		if (metadata == null) {
			// set mzTab file type and basic parameters
			String filename = params.getTSVFile().getName();
			MZTabDescription description = new MZTabDescription(
				MZTabDescription.Mode.Summary,
				MZTabDescription.Type.Identification);
			description.setId(filename);
			Metadata metadata = new Metadata(description);
			// set title and description
			metadata.setTitle(String.format(
				"MZTab file converted from tab-delimited result file \"%s\"",
				filename));
			metadata.setDescription(String.format(
				"This MZTab file was programmatically converted from " +
				"tab-delimited result file \"%s\", using conversion software " +
				"provided by the Center for Computational Mass Spectrometry " +
				"of UCSD.", filename));
			// add all fixed and variable mods
			int fixedCount = 0;
			int variableCount = 0;
			for (ModRecord record : params.getModifications()) {
				if (record.isFixed()) {
					FixedMod mod = new FixedMod(++fixedCount);
					mod.setParam(record.getParam());
					metadata.addFixedMod(mod);
				} else {
					VariableMod mod = new VariableMod(++variableCount);
					mod.setParam(record.getParam());
					metadata.addVariableMod(mod);
				}
			}
			// add spectrum file references
			int index = 0;
			for (URL spectrumFile : params.getSpectrumFiles())
				metadata.addMsRunLocation(++index, spectrumFile);
			this.metadata = metadata;
		}
		return metadata;
	}
	
	@Override
	protected MZTabColumnFactory convertProteinColumnFactory() {
		System.out.println("Converting Protein column factory...");
		return MZTabColumnFactory.getInstance(Section.Protein_Header);
	}
	
	@Override
	protected MZTabColumnFactory convertPeptideColumnFactory() {
		System.out.println("Converting Peptide column factory...");
		return MZTabColumnFactory.getInstance(Section.Peptide_Header);
	}
	
	@Override
	protected MZTabColumnFactory convertPSMColumnFactory() {
		System.out.println("Converting PSM column factory...");
		// ensure that PSM column factory is initialized
		if (psmColumnFactory == null)
			psmColumnFactory =
				MZTabColumnFactory.getInstance(Section.PSM_Header);
		// add optional columns
		psmColumnFactory.addOptionalColumn("valid", String.class);
		psmColumnFactory.addOptionalColumn("invalid_reason", String.class);
		for (String column : params.getExtraColumns())
			psmColumnFactory.addOptionalColumn(column, String.class);
		// TODO: gather all original TSV columns in the params object,
		// and then iterate over those columns to add them here
		return psmColumnFactory;
	}
	
	@Override
	protected void fillData() {
		System.out.println("Filling data...");
		// instantiate all protein records into the mzTab file
		Map<String, ProteinRecord> proteinRecords = params.getProteins();
		for (String accession : proteinRecords.keySet()) {
			ProteinRecord record = proteinRecords.get(accession);
			Protein protein = new Protein();
			protein.setAccession(accession);
			// set PSM counts
			SortedMap<Integer, MsRun> msRunMap = metadata.getMsRunMap();
			for (int msRun : msRunMap.keySet())
				protein.setNumPSMs(
					msRunMap.get(msRun), record.getPSMCount(msRun));
			// set peptide counts
			for (int msRun : msRunMap.keySet())
				protein.setNumPeptidesDistinct(
					msRunMap.get(msRun), record.getPeptideCount(msRun));
			// TODO: count unique peptides for this protein
			// set modifications
			// TODO: deal with the fact that these modifications are taken
			// directly from the PSMs, and therefore their "position" elements
			// are PSM indices, whereas the mzTab format expects protein mods
			// to be labeled with the position of the mod within the protein;
			// presumably this requires knowing the protein sequence, or at
			// least the start index of the PSM peptide within the protein
//			Collection<Modification> modifications = record.getModifications();
//			if (modifications != null)
//				for (Modification modification : modifications)
//					protein.addModification(modification);
			proteins.add(protein);
		}
		// instantiate all peptide records into the mzTab file
		Map<String, PeptideRecord> peptideRecords = params.getPeptides();
		if (peptideRecords != null) {
			for (String sequence : peptideRecords.keySet()) {
				PeptideRecord record = peptideRecords.get(sequence);
				Collection<String> accessions = record.getAccessions();
				if (accessions == null || accessions.isEmpty()) {
					Peptide peptide = new Peptide(metadata);
					peptide.setSequence(sequence);
				} else for (String accession : record.getAccessions()) {
					Peptide peptide = new Peptide(metadata);
					peptide.setSequence(sequence);
				}
			}
		}
		// read all lines in the TSV file and add them to an mzTab PSM record
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(source));
			String line = null;
			int lineNumber = 1;
			while ((line = reader.readLine()) != null) {
				// skip header row, if there is one
				if (params.hasHeader() && lineNumber == 1) {
					lineNumber++;
					continue;
				}
				// determine base integer ID of this PSM
				int id;
				if (params.hasHeader())
					id = lineNumber - 1;
				else id = lineNumber;
				// extract the modified peptide string
				String[] elements = line.split("\t");
				String modifiedPeptide =
					elements[params.getColumnIndex("modified_sequence")];
				// if this row represents a mixture spectrum, then generate
				// a separate mzTab PSM for each matched peptide
				String[] peptides = modifiedPeptide.split("!");
				if (peptides == null || peptides.length < 2)
					processPSM(modifiedPeptide, elements, -1, id);
				else for (int i=0; i<peptides.length; i++)
					processPSM(peptides[i], elements, i, id + i);
				lineNumber++;
			}
		} catch (Throwable error) {
			throw new RuntimeException(error);
		} finally {
			try { reader.close(); }
			catch (Throwable error) {}
		}
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private void processPSM(
		String peptide, String[] elements, int index, int id
	) {
		if (peptide == null || elements == null)
			return;
		PSM psm = new PSM(psmColumnFactory, metadata);
		// set PSM integer ID
		psm.setPSM_ID(id);
		// formulate "spectra_ref" value for this row
		StringBuffer spectraRef = new StringBuffer();
		// first get the "ms_run" corresponding to the
		// spectrum filename extracted from this row
		SortedMap<Integer, MsRun> msRunMap = metadata.getMsRunMap();
		String filename = extractMixtureElement(
			elements[params.getColumnIndex("filename")], index);
		URL file = getFileURL(filename);
		for (Integer fileIndex : msRunMap.keySet()) {
			MsRun msRun = msRunMap.get(fileIndex);
			if (msRun.getLocation().equals(file)) {
				spectraRef.append("ms_run[").append(
					msRun.getId()).append("]:");
				break;
			}
		}
		if (spectraRef.length() == 0)
			throw new IllegalArgumentException(String.format(
				"Error creating PSM record: no registered \"ms_run\" " +
				"metadata element could be found to match TSV " +
				"\"filename\" column value [%s]", filename));
		// then set the proper nativeID key
		if (params.isScanMode())
			spectraRef.append("scan=");
		else spectraRef.append("index=");
		// finally, append the actual ID value extracted from this row
		spectraRef.append(extractMixtureElement(
			elements[params.getColumnIndex("spectrum_id")], index));
		psm.setSpectraRef(spectraRef.toString());
		// set (cleaned) peptide string
		psm.setSequence(PeptideUtils.cleanPeptide(peptide));
		// try to extract "pre" and "post" values from the peptide sequence
		psm.setPre(getPre(peptide));
		psm.setPost(getPost(peptide));
		// formulate "modifications" value for this row
		String stripped = stripPreAndPost(peptide);
		ImmutablePair<String, Collection<Modification>> extracted =
			PeptideUtils.extractPTMsFromPSM(
				stripped, params.getModifications());
		// mark this row as "INVALID" if any mods were
		// left unparsed from the peptide string
		String cleaned = extracted.getLeft();
		if (isPeptideClean(cleaned))
			psm.setOptionColumnValue("valid", "VALID");
		else {
			psm.setOptionColumnValue("valid", "INVALID");
			psm.setOptionColumnValue("invalid_reason", String.format(
				"The parsed peptide string [%s] for this row was still " +
				"found to contain non-amino acid characters, even after " +
				"extracting all expected modifications.", cleaned));
		}
		Collection<Modification> mods = extracted.getRight();
		if (mods != null && mods.isEmpty() == false)
			for (Modification mod : mods)
				psm.addModification(mod);
		else psm.setModifications((String)null);
		// initialize non-required column values to null,
		// in case any are not specified in the parameters
		psm.setAccession(null);
		psm.setDatabase(null);
		psm.setDatabaseVersion(null);
		psm.setSearchEngine((String)null);
		psm.setRetentionTime((String)null);
		psm.setCharge((String)null);
		psm.setExpMassToCharge((String)null);
		psm.setCalcMassToCharge((String)null);
		psm.setStart((String)null);
		psm.setEnd((String)null);
		// set remaining column values
		for (String column : params.getColumns()) {
			if (column == null)
				continue;
			String value = extractMixtureElement(
				elements[params.getColumnIndex(column)], index);
			if (value == null)
				continue;
			if (column.equals("accession"))
				psm.setAccession(value);
			else if (column.equals("unique"))
				psm.setUnique(value);
			else if (column.equals("database"))
				psm.setDatabase(value);
			else if (column.equals("database_version"))
				psm.setDatabaseVersion(value);
			// TODO: deal with search engines and scores
			else if (column.equals("retention_time"))
				psm.setRetentionTime(value);
			else if (column.equals("charge"))
				psm.setCharge(value);
			else if (column.equals("exp_mass_to_charge"))
				psm.setExpMassToCharge(value);
			else if (column.equals("calc_mass_to_charge"))
				psm.setCalcMassToCharge(value);
			else if (column.equals("pre"))
				psm.setPre(value);
			else if (column.equals("post"))
				psm.setPost(value);
			else if (column.equals("start"))
				psm.setStart(value);
			else if (column.equals("end"))
				psm.setEnd(value);
		}
		// add extra columns
		for (String column : params.getExtraColumns()) {
			if (column == null)
				continue;
			String value = extractMixtureElement(
				elements[params.getExtraColumnIndex(column)], index);
			if (value == null)
				continue;
			else psm.setOptionColumnValue(column, value);
		}
		// add fully initialized PSM to collection
		psms.add(psm);
	}
	
	private void processPeptide(PeptideRecord record, String accession) {
		if (record == null)
			return;
		Peptide peptide = new Peptide(metadata);
		peptide.setSequence(record.getSequence());
		if (accession != null)
			peptide.setAccession(accession);
		Collection<String> spectraRefs = record.getSpectraRefs(accession);
	}
	
	private String extractMixtureElement(String value, int index) {
		if (value == null)
			return null;
		else if (index < 0)
			return value;
		String[] tokens = value.split("!");
		if (tokens == null || index >= tokens.length)
			return value;
		else return tokens[index];
	}
	
	private String stripPreAndPost(String psm) {
		if (psm == null)
			return null;
		// TODO: the user should specify if this syntax is present, and
		// therefore whether or not this processing should even be done
		Matcher matcher = PeptideUtils.PEPTIDE_STRING_PATTERN.matcher(psm);
		if (matcher.matches())
			return matcher.group(2);
		else return psm;
	}
	
	private String getPre(String psm) {
		if (psm == null)
			return null;
		// TODO: the user should specify if this syntax is present, and
		// therefore whether or not this processing should even be done
		Matcher matcher = PeptideUtils.PEPTIDE_STRING_PATTERN.matcher(psm);
		if (matcher.matches())
			return getAminoAcid(matcher.group(1));
		else return null;
	}
	
	private String getPost(String psm) {
		if (psm == null)
			return null;
		// TODO: the user should specify if this syntax is present, and
		// therefore whether or not this processing should even be done
		Matcher matcher = PeptideUtils.PEPTIDE_STRING_PATTERN.matcher(psm);
		if (matcher.matches())
			return getAminoAcid(matcher.group(3));
		else return null;
	}
	
	private String getAminoAcid(String peptide) {
		if (peptide == null || peptide.length() != 1)
			return null;
		char residue = peptide.charAt(0);
		if (residue == '-' ||
			PeptideUtils.AMINO_ACID_MASSES.containsKey(residue))
			return peptide;
		// sometimes, underscores ("_") are used to indicate terminal residues
		else if (residue == '_')
			return "-";
		else return null;
	}
	
	private boolean isPeptideClean(String peptide) {
		if (peptide == null)
			return false;
		else for (int i=0; i<peptide.length(); i++)
			if (PeptideUtils.AMINO_ACID_MASSES.containsKey(peptide.charAt(i))
				== false)
				return false;
		return true;
	}
	
	/*========================================================================
	 * Static application methods
	 *========================================================================*/
	public static void main(String[] args) {
		TSVToMzTabConverter converter = null;
		FileOutputStream output = null;
		try {
			// initialize converter from file arguments
			System.out.println("Initializing converter...");
			converter = extractArguments(args);
			if (converter == null)
				die(USAGE);
			// run converter, print to stdout
			output = new FileOutputStream(converter.params.getMzTabFile());
			System.out.println("Running converter...");
			converter.getMZTabFile().printMZTab(output);
			System.out.println("Done.");
		} catch (Throwable error) {
			die(null, error);
		} finally {
			try { output.close(); }
			catch (Throwable error) {}
		}
	}
	
	public static URL getFileURL(String filename) {
		// try to extract a URL from the filename string
		URL url = null;
		try {
			url = new URL(filename);
		} catch (MalformedURLException error) {}
		// if the source string was not a valid URL, create
		// a new one with just the "file" protocol
		if (url == null) try {
			url = new URL("file://" + filename);
		} catch (MalformedURLException error) {}
		return url;
	}
	
	/*========================================================================
	 * Static application convenience methods
	 *========================================================================*/
	private static TSVToMzTabConverter extractArguments(String[] args) {
		if (args == null || args.length < 1)
			return null;
		// extract file arguments
		File tsvFile = null;
		File paramsFile = null;
		File mzTabDirectory = null;
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
					paramsFile = new File(value);
				else if (argument.equalsIgnoreCase("-mzTab"))
					mzTabDirectory = new File(value);
				else throw new IllegalArgumentException(String.format(
					"Unrecognized parameter at index %d: [%s]", i, argument));
			}
		}
		// validate extracted file arguments
		if (tsvFile == null) {
			System.err.println("\"-tsv\" is a required parameter.");
			die(USAGE);
		} else if (paramsFile == null) {
			System.err.println("\"-params\" is a required parameter.");
			die(USAGE);
		} else if (mzTabDirectory == null) {
			System.err.println("\"-mzTab\" is a required parameter.");
			die(USAGE);
		}
		// process extracted file arguments into an initialized converter
		try {
			return new TSVToMzTabConverter(
				new TSVToMzTabParameters(paramsFile, tsvFile, mzTabDirectory));
		} catch (IOException error) {
			throw new RuntimeException(error);
		}
	}
	
	private static void die(String message) {
		die(message, null);
	}
	
	private static void die(String message, Throwable error) {
		if (message == null)
			message =
				"There was an error converting the input TSV file to mzTab";
		if (error != null)
			message += ":";
		else if (message.endsWith(".") == false)
			message += ".";
		System.err.println(message);
//		if (error != null)
//			error.printStackTrace();
		System.exit(1);
	}
}
