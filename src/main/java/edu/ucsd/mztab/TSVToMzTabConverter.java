package edu.ucsd.mztab;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TSVToMzTabConverter
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String USAGE = "java -jar MzTabUtils.jar" +
		"\n\t-tool     convertTSV" +
		"\n\t-tsv      <InputTSVFile>" +
		"\n\t-mzTab    <OutputMzTabFile>" +
		"\n\t-header   0,1" +
			"\n\t\t0 = The input TSV file does not contain a header line. " +
			"In this case, the arguments to all column parameters must be " +
			"valid integer indices (0-based)." +
			"\n\t\t1 = The first line of the input TSV file is a header " +
			"line, declaring the names of the columns in each subsequent " +
			"row of the file.  In this case, the arguments to column " +
			"parameters may be either valid indices, or string names " +
			"corresponding to the column headers in this header row." +
		"\n\t-filename <PeakListFilenameColumn> (column name or index)" +
		"\n\t-id       <SpectrumIDColumn>  (column name or index)" +
		"\n\t-idType   index,scan" +
			"\n\t\tindex = The \"-id\" column represents spectrum indices " +
			"within the referenced peak list file." +
			"\n\t\tscan = The \"-id\" column represents scan numbers." +
		"\n\t-psm      <PeptideSpectrumMatchColumn> (column name or index)";
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public static void main(String[] args) {
		TSVToMzTabConversion conversion = null;
		try {
			conversion = extractArguments(args);
			if (conversion == null)
				die("---------- Usage: ----------\n" + USAGE);
			// read all lines in the TSV file and validate them
			String line = null;
			while ((line = conversion.tsvFile.reader.readLine()) != null) {
				StringBuffer report = new StringBuffer("Line ");
				report.append(conversion.tsvFile.lineNumber).append(": ");
				String[] elements = line.split("\t");
				if (conversion.tsvFile.filenameColumn >= elements.length)
					throw new IllegalArgumentException(String.format(
						"Error parsing input TSV file [%s]: the index of the " +
						"\"-filename\" column was given as %d, but line %d " +
						"of the file contains only %d elements:\n%s",
						conversion.tsvFile.filename,
						conversion.tsvFile.filenameColumn,
						conversion.tsvFile.lineNumber, elements.length, line));
				else report.append("filename = [")
					.append(elements[conversion.tsvFile.filenameColumn])
					.append("], ");
				if (conversion.tsvFile.idColumn >= elements.length)
					throw new IllegalArgumentException(String.format(
						"Error parsing input TSV file [%s]: the index of the " +
						"\"-id\" column was given as %d, but line %d " +
						"of the file contains only %d elements:\n%s",
						conversion.tsvFile.filename,
						conversion.tsvFile.idColumn,
						conversion.tsvFile.lineNumber, elements.length, line));
				else report.append(conversion.tsvFile.scan ? "scan" : "index")
					.append(" = [")
					.append(elements[conversion.tsvFile.idColumn])
					.append("], ");
				String psm = null;
				if (conversion.tsvFile.psmColumn >= elements.length)
					throw new IllegalArgumentException(String.format(
						"Error parsing input TSV file [%s]: the index of the " +
						"\"-psm\" column was given as %d, but line %d " +
						"of the file contains only %d elements:\n%s",
						conversion.tsvFile.filename,
						conversion.tsvFile.psmColumn,
						conversion.tsvFile.lineNumber, elements.length, line));
				else {
					psm = elements[conversion.tsvFile.psmColumn];
					report.append("PSM = [").append(psm).append("], ");
				}
				report.append("Peptide = [").append(cleanPSM(psm))
					.append("], ");
				// prepare mzTab mods column value
				report.append("PTMs = [");
				Collection<Modification> ptms = extractPTMsFromPSM(psm);
				if (ptms != null)
					for (Modification ptm : ptms)
						report.append(
							ptm.getMzTabFormattedModString()).append(",");
				// chomp trailing comma, if present
				if (report.charAt(report.length() - 1) == ',')
					report.setLength(report.length() - 1);
				report.append("]");
				System.out.println(report.toString());
				conversion.tsvFile.lineNumber++;
			}
		} catch (Throwable error) {
			die(error.getMessage());
		} finally {
			if (conversion != null && conversion.tsvFile != null)
				conversion.tsvFile.close();
		}
	}
	
	/*========================================================================
	 * Convenience classes
	 *========================================================================*/
	/**
	 * Struct to maintain context data for each TSV to mzTab file conversion
	 * operation.
	 */
	private static class TSVToMzTabConversion {
		/*====================================================================
		 * Properties
		 *====================================================================*/
		private TSVFile tsvFile;
		private File    mzTabFile;
		
		/*====================================================================
		 * Constructors
		 *====================================================================*/
		public TSVToMzTabConversion(
			File tsvFile, File mzTabFile, boolean header, boolean scan,
			String filenameColumn, String idColumn, String psmColumn
		) throws IOException {
			// validate input TSV file
			this.tsvFile = new TSVFile(
				tsvFile, header, scan, filenameColumn, idColumn, psmColumn);
			if (this.tsvFile == null)
				throw new NullPointerException(String.format(
					"There was an error parsing input tsvFile [%s].",
					tsvFile != null ? tsvFile.getName() : "null"));
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
		}
	}
	
	/**
	 * Struct to maintain context data for a single input TSV file.
	 */
	private static class TSVFile {
		/*====================================================================
		 * Properties
		 *====================================================================*/
		private String         filename;
		private BufferedReader reader;
		private boolean        scan;
		private Integer        filenameColumn;
		private Integer        idColumn;
		private Integer        psmColumn;
		private Integer        lineNumber;
		
		/*====================================================================
		 * Constructors
		 *====================================================================*/
		public TSVFile(
			File tsvFile, Boolean header, Boolean scan,
			String filenameColumn, String idColumn, String psmColumn
		) throws IOException {
			// validate input TSV file
			if (tsvFile == null)
				throw new NullPointerException(
					"Input TSV file cannot be null.");
			else if (tsvFile.isFile() == false)
				throw new IllegalArgumentException(
					String.format("Input TSV file [%s] must be a normal " +
						"(non-directory) file.", tsvFile.getName()));
			else if (tsvFile.canRead() == false)
				throw new IllegalArgumentException(
					String.format("Input TSV file [%s] must be readable.",
						tsvFile.getName()));
			else filename = tsvFile.getName();
			// set up TSV file reader
			this.reader = new BufferedReader(new FileReader(tsvFile));
			// set boolean properties
			if (header == null)
				throw new NullPointerException("\"-header\" flag should be " +
					"given a value of either 0 or 1.");
			if (scan == null)
				throw new NullPointerException("\"-scan\" flag should be " +
					"given a value of either 0 or 1.");
			else this.scan = scan;
			// read the first line to validate all column IDs
			reader.mark(10000);
			String line = reader.readLine();
			// if the first line is not supposed to be a header line, rewind
			if (header == false) {
				reader.reset();
				lineNumber = 1;
			} else lineNumber = 2;
			String[] firstLineElements = line.split("\t");
			if (firstLineElements == null || firstLineElements.length < 1)
				throw new IllegalArgumentException(
					String.format("Could not parse the tab-delimited " +
						"elements of the first line from input TSV file [%s].",
						filename));
			// validate filename column
			this.filenameColumn = extractColumnIndex(
				filenameColumn, filename, "-filename", line,
				header, firstLineElements);
			if (this.filenameColumn == null)
				throw new NullPointerException(String.format(
					"There was an error parsing \"-filename\" column [%s].",
					filenameColumn != null ? filenameColumn : "null"));
			// validate spectrum ID column
			this.idColumn = extractColumnIndex(
				idColumn, filename, "-id", line, header, firstLineElements);
			if (this.idColumn == null)
				throw new NullPointerException(String.format(
					"There was an error parsing \"-id\" column [%s].",
					idColumn != null ? idColumn : "null"));
			// validate PSM column
			this.psmColumn = extractColumnIndex(
				psmColumn, filename, "-psm", line, header, firstLineElements);
			if (this.psmColumn == null)
				throw new NullPointerException(String.format(
					"There was an error parsing \"-psm\" column [%s].",
					psmColumn != null ? psmColumn : "null"));
		}
		
		/*====================================================================
		 * Public interface methods
		 *====================================================================*/
		public void close() {
			if (reader != null) try {
				reader.close();
			} catch (Throwable error) {}
		}
	}
	
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
	private static TSVToMzTabConversion extractArguments(String[] args) {
		if (args == null || args.length < 1)
			return null;
		File tsvFile = null;
		File mzTabFile = null;
		Boolean header = null;
		Boolean scan = null;
		String filenameColumn = null;
		String idColumn = null;
		String psmColumn = null;
		for (int i=0; i<args.length; i++) {
			String argument = args[i];
			if (argument == null)
				return null;
			else {
				i++;
				if (i >= args.length)
					throw new IllegalArgumentException(String.format(
						"Could not parse the argument at index %d for " +
						"parameter [%s]: if your string value includes any " +
						"unusual characters, please ensure that it is " +
						"properly enclosed in quotation marks.", i, argument));
				String value = args[i];
				if (argument.equals("-tsv"))
					tsvFile = new File(value);
				else if (argument.equals("-mzTab"))
					mzTabFile = new File(value);
				else if (argument.equals("-header")) {
					if (value.trim().equals("0"))
						header = false;
					else if (value.trim().equals("1"))
						header = true;
				} else if (argument.equals("-idType")) {
					if (value.trim().equals("index"))
						scan = false;
					else if (value.trim().equals("scan"))
						scan = true;
				} else if (argument.equals("-filename"))
					filenameColumn = value;
				else if (argument.equals("-id"))
					idColumn = value;
				else if (argument.equals("-psm"))
					psmColumn = value;
				else throw new IllegalArgumentException(String.format(
					"Unrecognized parameter at index %d: [%s]", i, argument));
			}
		}
		try {
			return new TSVToMzTabConversion(tsvFile, mzTabFile, header, scan,
				filenameColumn, idColumn, psmColumn);
		} catch (Throwable error) {
			System.err.println(error.getMessage());
			return null;
		}
	}
	
	private static Integer extractColumnIndex(
		String columnID, String tsvFilename, String parameterName, String line,
		boolean header, String[] headers
	) {
		if (columnID == null || tsvFilename == null || parameterName == null ||
			line == null || headers == null)
			return null;
		// first try to parse the user-supplied columnID as an integer index
		try {
			int index = Integer.parseInt(columnID);
			// the specified index must be within the bounds of the header array
			if (index < 0 || index >= headers.length)
				throw new IllegalArgumentException(String.format(
					"Error parsing input TSV file [%s]: the index of the " +
					"\"%s\" column was given as %s, but the first " +
					"line of the file contains %d elements:\n%s",
					tsvFilename, parameterName, columnID,
					headers.length, line));
			else return index;
		}
		// if the user-supplied value of the column ID was not an
		// integer, then it must be the string name of a column header
		catch (NumberFormatException error) {
			// if there is no header row, then a column ID that cannot be
			// parsed as an integer is illegal, since it can't be looked up
			if (header == false)
				throw new IllegalArgumentException(String.format(
					"Error parsing input TSV file [%s]: the \"%s\" " +
					"column header was given as [%s], but yet the argument " +
					"to the \"-header\" parameter was given as 0, indicating " +
					"that the file does not contain a header line.",
					tsvFilename, parameterName, columnID));
			// otherwise, try to find the index of the specified column header
			for (int i=0; i<headers.length; i++)
				if (columnID.equals(headers[i]))
					return i;
			// if no matching column header was found, then throw a
			// NoSuchElementFoundException
			throw new IllegalArgumentException(String.format(
				"Error parsing input TSV file [%s]: the \"%s\" " +
				"column header was given as [%s], but this header could " +
				"not be found in the first line of the file:\n%s",
				tsvFilename, parameterName, columnID, line));
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
	
	public static void mainy(String[] args) {
		String test = "+42.011C+57.021NGVL(E,-17)GI[900]R";
		Collection<Modification> ptms = extractPTMsFromPSM(test);
		if (ptms == null)
			System.out.println(String.format(
				"No PTMs were found in PSM string [%s]", test));
		else {
			System.out.println(String.format(
				"Found a PTM count of %d in PSM string [%s]",
				ptms.size(), test));
			for (Modification modification : ptms)
				System.out.println(String.format("\t%d = %s",
					modification.getSite(),
					modification.getMzTabFormattedModString()));
		}
		System.out.println(String.format("\tClean = %s", cleanPSM(test)));
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
