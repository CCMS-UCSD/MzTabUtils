package edu.ucsd.mztab;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.SortedMap;

import org.apache.commons.lang3.tuple.ImmutablePair;

import uk.ac.ebi.pride.jmztab.model.FixedMod;
import uk.ac.ebi.pride.jmztab.model.MZTabColumnFactory;
import uk.ac.ebi.pride.jmztab.model.MZTabDescription;
import uk.ac.ebi.pride.jmztab.model.Metadata;
import uk.ac.ebi.pride.jmztab.model.Mod;
import uk.ac.ebi.pride.jmztab.model.Modification;
import uk.ac.ebi.pride.jmztab.model.Modification.Type;
import uk.ac.ebi.pride.jmztab.model.MsRun;
import uk.ac.ebi.pride.jmztab.model.PSM;
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
		"\n\t-mzTab  <OutputMzTabFile>";
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
				Mod mod = null;
				if (record.isFixed())
					mod = new FixedMod(++fixedCount);
				else mod = new VariableMod(++variableCount);
				mod.setParam(record.getParam());
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
		return null;
	}
	
	@Override
	protected MZTabColumnFactory convertPeptideColumnFactory() {
		return null;
	}
	
	@Override
	protected MZTabColumnFactory convertPSMColumnFactory() {
		System.out.println("Converting PSM column factory...");
		// ensure that PSM column factory is initialized
		if (psmColumnFactory == null)
			psmColumnFactory =
				MZTabColumnFactory.getInstance(Section.PSM_Header);
		return psmColumnFactory;
	}
	
	@Override
	protected void fillData() {
		System.out.println("Filling data...");
		// read all lines in the TSV file and add them to an mzTab PSM record
		SortedMap<Integer, MsRun> msRunMap = metadata.getMsRunMap();
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
				PSM psm = new PSM(psmColumnFactory, metadata);
				// set PSM integer ID
				if (params.hasHeader())
					psm.setPSM_ID(lineNumber - 1);
				else psm.setPSM_ID(lineNumber);
				// formulate "spectra_ref" value for this row
				String[] elements = line.split("\t");
				StringBuffer spectraRef = new StringBuffer();
				// first get the "ms_run" corresponding to the
				// spectrum filename extracted from this row
				String filename = elements[params.getColumnIndex("filename")];
				URL file = getFileURL(filename);
				for (Integer index : msRunMap.keySet()) {
					MsRun msRun = msRunMap.get(index);
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
				spectraRef.append(
					elements[params.getColumnIndex("spectrum_id")]);
				psm.setSpectraRef(spectraRef.toString());
				// set (cleaned) peptide string
				String modifiedPeptide =
					elements[params.getColumnIndex("modified_sequence")];
				psm.setSequence(cleanPSM(modifiedPeptide));
				// formulate "modifications" value for this row
				Collection<Modification> mods =
					extractPTMsFromPSM(modifiedPeptide);
				if (mods != null && mods.isEmpty() == false) {
					StringBuffer modifications = new StringBuffer();
					boolean first = true;
					for (Modification mod : mods) {
						if (first == false)
							modifications.append(",");
						modifications.append(mod.toString());
						first = false;
					}
					psm.setModifications(modifications.toString());
				} else psm.setModifications((String)null);
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
				psm.setPre(null);
				psm.setPost(null);
				psm.setStart((String)null);
				psm.setEnd((String)null);
				// set remaining column values
				for (String column : params.getColumns()) {
					if (column == null)
						continue;
					String value = elements[params.getColumnIndex(column)];
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
				// add fully initialized PSM to collection
				psms.add(psm);
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
	private String cleanPSM(String psm) {
		if (psm == null)
			return null;
		StringBuffer clean = new StringBuffer();
		for (int i=0; i<psm.length(); i++) {
			char current = psm.charAt(i);
			if (Character.isLetter(current))
				clean.append(current);
		}
		return clean.toString();
	}
	
	private Collection<Modification> extractPTMsFromPSM(String psm) {
		if (psm == null)
			return null;
		Collection<Modification> mods = new ArrayList<Modification>();
		for (ModRecord record : params.getModifications()) {
			ImmutablePair<String, Collection<Integer>> occurrences =
				record.parsePSM(psm);
			if (occurrences == null)
				continue;
			Collection<Integer> indices = occurrences.getRight();
			if (indices == null || indices.isEmpty())
				continue;
			Type type = record.getType();
			String accession = record.getAccession();
			for (int index : indices) {
				// if this mod is a CHEMMOD, then its value needs to be its mass
				String value = null;
				if (type.equals(Type.CHEMMOD))
					value = record.getFormattedMass();
				// otherwise, try to extract the numerical
				// portion of the CV accession
				else if (accession != null) {
					String[] tokens = accession.split(":");
					if (tokens == null || tokens.length < 1)
						throw new IllegalArgumentException(String.format(
							"Unrecognized modification CV accession: [%s].",
							accession));
					else if (tokens.length > 1)
						value = tokens[1];
					else value = accession;
			 	}
				// the mod's value should not be null
				if (value == null)
					throw new IllegalArgumentException(String.format(
						"Could not determine a valid mod value to " +
						"write into the mzTab PSM row, for mod [%s] " +
						"at position %d of input PSM string [%s].",
						record.toString(), index, psm));
				Modification mod = new Modification(Section.PSM, type, value);
				mod.addPosition(index, null);
				mods.add(mod);
			}
		}
		if (mods == null || mods.isEmpty())
			return null;
		else return mods;
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
		File mzTabFile = null;
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
					mzTabFile = new File(value);
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
		} else if (mzTabFile == null) {
			System.err.println("\"-mzTab\" is a required parameter.");
			die(USAGE);
		}
		// process extracted file arguments into an initialized converter
		try {
			return new TSVToMzTabConverter(
				new TSVToMzTabParameters(paramsFile, tsvFile, mzTabFile));
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
		if (error != null)
			error.printStackTrace();
		System.exit(1);
	}
}
