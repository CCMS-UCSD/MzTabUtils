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
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import uk.ac.ebi.pride.jmztab.model.FixedMod;
import uk.ac.ebi.pride.jmztab.model.MZTabColumnFactory;
import uk.ac.ebi.pride.jmztab.model.MZTabDescription;
import uk.ac.ebi.pride.jmztab.model.Metadata;
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
			for (FixedMod mod : params.getFixedMods())
				metadata.addFixedMod(mod);
			for (VariableMod mod : params.getVariableMods())
				metadata.addVariableMod(mod);
			// add spectrum file references
			int index = 1;
			for (URL spectrumFile : params.getSpectrumFiles()) {
				metadata.addMsRunLocation(index, spectrumFile);
				index++;
			}
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
						modifications.append(mod.getMzTabFormattedModString());
						first = false;
					}
					psm.setModifications(modifications.toString());
				} else psm.setModifications((String)null);
				// initialize non-required column values to null,
				// in case any are not specified in the parameters
				psm.setAccession(null);
				psm.setUnique((String)null);
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
	 * Convenience classes
	 *========================================================================*/
	/**
	 * Struct to maintain context data for a single parsed PTM.
	 */
	private static class Modification {
		/*====================================================================
		 * Constants
		 *====================================================================*/
		public static final Pattern[] PTM_PATTERNS = {
			Pattern.compile("^([+-]?\\d*\\.?\\d*)$"),
			Pattern.compile("^\\(\\w,([+-]?\\d*\\.?\\d*)\\)$"),
			Pattern.compile("^\\[([+-]?\\d*\\.?\\d*)\\]$")
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
		
		/*====================================================================
		 * Properties
		 *====================================================================*/
		private int site;
		private double mass;
		
		/*====================================================================
		 * Constructor
		 *====================================================================*/
		public Modification(
			int site, char aminoAcid, String massDescriptor
		) {
			// validate site
			if (site < 0)
				throw new IllegalArgumentException(
					"A modification site index cannot be negative.");
			else this.site = site;
			// validate amino acid
			Double aaMass = AMINO_ACID_MASSES.get(aminoAcid);
			if (aaMass == null && aminoAcid != '*')
				throw new IllegalArgumentException(String.format(
					"Unrecognized amino acid: [%c].", aminoAcid));
			// try all known PTM patterns to extract this PTM's mass
			String mass = null;
			boolean squareBracketFormat = false;
			for (int i=0; i<PTM_PATTERNS.length; i++) {
				Pattern pattern = PTM_PATTERNS[i];
				Matcher matcher = pattern.matcher(massDescriptor);
				if (matcher.matches()) {
					mass = matcher.group(1);
					// note the special case of "[]" mod formats
					if (i == 2) {
						if (aminoAcid == '*')
							throw new IllegalArgumentException(String.format(
								"PTM \"%s\", specified using the " +
								"square-bracket ([]) syntax, cannot be " +
								"applied to an N-terminal amino acid. This " +
								"syntax necessarily implies a sum of " +
								"the modification mass and that of its " +
								"preceding amino acid.", massDescriptor));
						else squareBracketFormat = true;
					}
					break;
				}
			}
			// try to extract the mass from the parsed string
			try {
				this.mass = Double.parseDouble(mass);
				// in the case of "[]" mod formats,
				// we need to subtract the AA mass
				if (squareBracketFormat)
					this.mass -= aaMass;
			} catch (NumberFormatException error) {
				throw new IllegalArgumentException(String.format(
					"Unrecognized PTM mass format: [%s].", massDescriptor));
			}
		}
		
		/*====================================================================
		 * Property accessor methods
		 *====================================================================*/
		public int getSite() {
			return site;
		}
		
		public String getMass() {
			String formattedMass;
			if (mass == (int)mass)
				formattedMass = String.format("%d", (int)mass);
			else formattedMass = String.format("%s", mass);
			// prepend a "+" if this is a non-negative mass offset
			if (mass >= 0.0 && formattedMass.startsWith("+") == false)
				formattedMass = "+" + formattedMass;
			return formattedMass;
		}
		
		public String getMzTabFormattedModString() {
			return String.format("%d-CHEMMOD:%s", getSite(), getMass());
		}
	}
	
	/*========================================================================
	 * Convenience methods
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
	
	private static String cleanPSM(String psm) {
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
	
	private static Collection<Modification> extractPTMsFromPSM(String psm) {
		if (psm == null)
			return null;
		Collection<Modification> ptms = new ArrayList<Modification>();
		int aaCount = 0;
		int start = -1;
		char modifiedAA = '*';
		boolean parentheses = false;
		boolean parentheticalAASeen = false;
		for (int i=0; i<psm.length(); i++) {
			char current = psm.charAt(i);
			// if this is not a letter, then it must be part of a PTM region
			if (Character.isLetter(current) == false) {
				// if no start index has been noted, then
				// this is the beginning of a PTM region
				if (start < 0) {
					start = i;
					// note the previous amino acid, unless there is none,
					// since this might be an N-term mod
					if (i >= 1)
						modifiedAA = psm.charAt(i - 1);
					// note if the start character is an opening parenthesis
					if (current == '(')
						parentheses = true;
				}
			} else {
				// if this is a letter, but the region opener was a parenthesis
				// and no parenthetical amino acid has been seen, then this is it
				if (parentheses && parentheticalAASeen == false) {
					parentheticalAASeen = true;
					modifiedAA = current;
				}
				// otherwise, if a PTM region has started, then
				// this letter marks the end of that region
				else if (start >= 0) try {
					ptms.add(new Modification(
						aaCount, modifiedAA, psm.substring(start, i)));
					start = -1;
					modifiedAA = '*';
					parentheses = false;
					parentheticalAASeen = false;
				} catch (Throwable error) {
					die(error.getMessage());
				}
				// keep track of the actual position within the peptide
				aaCount++;
			}
		}
		if (ptms == null || ptms.isEmpty())
			return null;
		else return ptms;
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
