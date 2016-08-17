package edu.ucsd.mztab.processors;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import edu.ucsd.mztab.exceptions.UnverifiableNativeIDException;
import edu.ucsd.mztab.model.MzIdentMLNativeIDMap;
import edu.ucsd.mztab.model.MzTabConstants;
import edu.ucsd.mztab.model.MzTabFile;
import edu.ucsd.mztab.model.MzTabMsRun;
import edu.ucsd.mztab.model.MzTabProcessor;
import edu.ucsd.mztab.model.MzTabSectionHeader;
import edu.ucsd.mztab.model.MzTabConstants.MzTabSection;

public class SpectraRefResolutionProcessor
implements MzTabProcessor
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String[] RELEVANT_PSM_COLUMNS = new String[]{
		MzTabConstants.PSH_SPECTRA_REF_COLUMN,
		MzTabConstants.PSH_PEPTIDE_COLUMN
	};
	
	/*========================================================================
	 * Properties
	 *========================================================================*/
	// mzTab file properties
	private MzTabFile          mzTabFile;
	private String             mzTabFilename;
	private MzTabSectionHeader psmHeader;
	private int                spectraRefIndex;
	private int                sequenceIndex;
	private int                validIndex;
	private int                invalidReasonIndex;
	// keep track of how "ambiguous" nativeIDs have been interpreted so far;
	// null=no ambiguous nativeIDs found so far, true=scan, false=index
	private Boolean            ambiguousNativeIDsAsScans;
	// input spectrum file properties
	private Map<String, ImmutablePair<Integer, Collection<Integer>>> spectra;
	private MzIdentMLNativeIDMap mzidCache;
	
	/*========================================================================
	 * Constructor
	 *========================================================================*/
	public SpectraRefResolutionProcessor(
		File uploadedResultDirectory, File spectrumIDsDirectory
	) {
		this(uploadedResultDirectory, spectrumIDsDirectory, null);
	}
	
	public SpectraRefResolutionProcessor(
		File uploadedResultDirectory, File spectrumIDsDirectory,
		Boolean ambiguousNativeIDsAsScans
	) {
		// initialize basic mzTab properties
		mzTabFilename = null;
		spectraRefIndex = -1;
		sequenceIndex = -1;
		validIndex = -1;
		invalidReasonIndex = -1;
		this.ambiguousNativeIDsAsScans = ambiguousNativeIDsAsScans;
		// initialize spectrum ID data structures
		spectra = new LinkedHashMap<
			String, ImmutablePair<Integer, Collection<Integer>>>();
		mzidCache = new MzIdentMLNativeIDMap(uploadedResultDirectory);
		// validate and process spectrum IDs directory (can be null)
		if (spectrumIDsDirectory != null) {
			if (spectrumIDsDirectory.isDirectory() == false)
				throw new IllegalArgumentException(String.format(
					"Spectrum ID files directory [%s] must be a directory.",
					spectrumIDsDirectory.getAbsolutePath()));
			else if (spectrumIDsDirectory.canRead() == false)
				throw new IllegalArgumentException(String.format(
					"Spectrum ID files directory [%s] must be readable.",
					spectrumIDsDirectory.getAbsolutePath()));
			// populate spectrum IDs data structure from files
			// TODO: pre-reading all the spectrum ID files uses a lot of memory;
			// do this on demand in the getSpectrumIDs method to reduce memory
			// needs in exchange for potentially much worse performance
			else for (File spectrumIDsFile : spectrumIDsDirectory.listFiles()) {
				if (spectrumIDsFile != null) {
					ImmutablePair<Integer, Collection<Integer>> spectrumIDs =
						readSpectrumIDsFile(spectrumIDsFile);
					spectra.put(spectrumIDsFile.getName(), spectrumIDs);
				}
			}
		}
	}
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public void setUp(MzTabFile mzTabFile) {
		if (mzTabFile == null)
			throw new NullPointerException("Argument mzTab file is null.");
		else this.mzTabFile = mzTabFile;
		mzTabFilename = mzTabFile.getMzTabFilename();
	}
	
	public String processMzTabLine(String line, int lineNumber) {
		if (line == null)
			throw new NullPointerException(
				"Processed mzTab line cannot be null.");
		// set up PSM section header
		else if (line.startsWith("PSH")) {
			if (psmHeader != null)
				throw new IllegalArgumentException(String.format(
					"Line %d of mzTab file [%s] is invalid:" +
					"\n----------\n%s\n----------\n" +
					"A \"PSH\" row was already seen previously in this file.",
					lineNumber, mzTabFilename, line));
			psmHeader = new MzTabSectionHeader(line);
			psmHeader.validateHeaderExpectations(
				MzTabSection.PSM, Arrays.asList(RELEVANT_PSM_COLUMNS));
			// record all relevant column indices
			List<String> headers = psmHeader.getColumns();
			for (int i=0; i<headers.size(); i++) {
				String header = headers.get(i);
				if (header == null)
					continue;
				else if (header.equalsIgnoreCase(
					MzTabConstants.PSH_SPECTRA_REF_COLUMN))
					spectraRefIndex = i;
				else if (header.equalsIgnoreCase(
					MzTabConstants.PSH_PEPTIDE_COLUMN))
					sequenceIndex = i;
				else if (header.equalsIgnoreCase(
					MzTabConstants.VALID_COLUMN))
					validIndex = i;
				else if (header.equalsIgnoreCase(
					MzTabConstants.INVALID_REASON_COLUMN))
					invalidReasonIndex = i;
			}
			// add CCMS-controlled validity columns, if not already present
			if (validIndex < 0) {
				validIndex = headers.size();
				line = String.format("%s\t%s",
					line.trim(), MzTabConstants.VALID_COLUMN);
			}
			if (invalidReasonIndex < 0) {
				invalidReasonIndex = headers.size();
				line = String.format("%s\t%s",
					line.trim(), MzTabConstants.INVALID_REASON_COLUMN);
			}
		}
		// resolve and validate the spectra_ref column of this PSM row
		else if (line.startsWith("PSM")) {
			if (psmHeader == null)
				throw new IllegalArgumentException(String.format(
					"Line %d of mzTab file [%s] is invalid:" +
					"\n----------\n%s\n----------\n" +
					"A \"PSM\" row was found before any \"PSH\" row.",
					lineNumber, mzTabFilename, line));
			else psmHeader.validateMzTabRow(line);
			// ensure that the valid column is present in this row
			String[] row = line.split("\\t");
			if (validIndex >= row.length) {
				String[] newRow = new String[validIndex + 1];
				for (int i=0; i<row.length; i++)
					newRow[i] = row[i];
				row = newRow;
			}
			// set the valid column's value to "VALID" by default
			row[validIndex] = "VALID";
			line = getLine(row);
			// ensure that the invalid reason column is present in this row
			if (invalidReasonIndex >= row.length) {
				String[] newRow = new String[invalidReasonIndex + 1];
				for (int i=0; i<row.length; i++)
					newRow[i] = row[i];
				row = newRow;
			}
			// set the invalid reason column's value to "null" by default
			row[invalidReasonIndex] = "null";
			line = getLine(row);
			// retrieve the spectra_ref and peptide sequence column values
			String spectraRef = row[spectraRefIndex];
			String sequence = row[sequenceIndex];
			// parse the spectra_ref column value
			// into file reference and nativeID
			Matcher matcher =
				MzTabConstants.SPECTRA_REF_PATTERN.matcher(spectraRef);
			if (matcher.matches() == false)
				throw new IllegalArgumentException(String.format(
					"Line %d of mzTab file [%s] is invalid:" +
					"\n----------\n%s\n----------\n" +
					"\"spectra_ref\" column value [%s] does not " +
					"conform to the expected format [%s].",
					lineNumber, mzTabFilename, line, spectraRef,
					"ms_run[1-n]:<nativeID-formatted identifier string>"));
			// extract and validate ms_run index
			int msRunIndex;
			try { msRunIndex = Integer.parseInt(matcher.group(1)); }
			// it should be impossible for a parsing error to occur
			// here, since the spectra_ref column value matched
			// the integer portion of the regular expression
			catch (NumberFormatException error) {
				throw new IllegalStateException(error);
			}
			if (msRunIndex <= 0)
				throw new IllegalArgumentException(String.format(
					"Line %d of mzTab file [%s] is invalid:" +
					"\n----------\n%s\n----------\n" +
					"\"spectra_ref\" column value [%s] contains invalid " +
					"ms_run index %d; ms_run indices should start at 1.",
					lineNumber, mzTabFilename, line, spectraRef, msRunIndex));
			// extract nativeID
			String nativeID = matcher.group(2);
			// get spectrum IDs file for this PSM row
			MzTabMsRun msRun = mzTabFile.getMsRun(msRunIndex);
			String mangledPeakListFilename = msRun.getMangledPeakListFilename();
			if (msRun == null || mangledPeakListFilename == null)
				throw new IllegalArgumentException(String.format(
					"Line %d of mzTab file [%s] is invalid:" +
					"\n----------\n%s\n----------\n" +
					"Could not resolve any file mapping for \"ms_run\" " +
					"index %d.", lineNumber, mzTabFilename, line, msRunIndex));
			String spectrumIDsFilename = String.format(
				"%s.scans", FilenameUtils.getBaseName(mangledPeakListFilename));
			ImmutablePair<Integer, Collection<Integer>> spectrumIDs =
				getSpectrumIDs(spectrumIDsFilename);
			if (spectrumIDs == null)
				throw new IllegalArgumentException(String.format(
					"Line %d of mzTab file [%s] is invalid:" +
					"\n----------\n%s\n----------\n" +
					"No spectra were found for \"ms_run\" index %d, " +
					"corresponding to peak list file [%s] " +
					"(parsed into spectra summary file [%s]).",
					lineNumber, mzTabFilename, line, msRunIndex,
					msRun.getPeakListFilename(), spectrumIDsFilename));
			// validate nativeID
			String validatedNativeID =
				validateNativeID(nativeID, sequence, spectrumIDs);
			// if the nativeID could not be validated, then whether
			// or not this row is salvageable depends on the current
			// scheme for interpreting ambiguous nativeIDs
			if (validatedNativeID == null) {
				// if the current scheme is scans, then this nativeID was not
				// found in the scans map, but the index scheme might still
				// work; so throw an exception that will inform the client
				// to try again with a hard-coded index scheme
				if (ambiguousNativeIDsAsScans != null &&
					ambiguousNativeIDsAsScans)
					throw new RuntimeException(
						new UnverifiableNativeIDException(String.format(
							"nativeID [%s] could not be validated as " +
							"a scan number in peak list file [%s]; " +
							"try validating it as a spectrum index.",
							nativeID, msRun.getPeakListFilename())));
				// if no ambiguous nativeID interpretation scheme has been
				// selected yet, or if the current scheme is indices, then
				// we assume that either this nativeID simply could not be
				// validated or that scans have already been tried; in either
				// case, the row should be marked as invalid
				else {
					row[validIndex] = "INVALID";
					row[invalidReasonIndex] = String.format(
						"Invalid \"spectra_ref\" column value [%s]: this " +
						"spectrum could not be found in peak list file [%s].",
						spectraRef, msRun.getPeakListFilename());
					line = getLine(row);
				}
			}
		}
		return line;
	}
	
	public void tearDown() {}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private ImmutablePair<Integer, Collection<Integer>> getSpectrumIDs(
		String spectrumIDsFilename
	) {
		if (spectrumIDsFilename == null)
			return null;
		else return spectra.get(spectrumIDsFilename);
	}
	
	private ImmutablePair<Integer, Collection<Integer>>
	readSpectrumIDsFile(File spectrumIDsFile) {
		if (spectrumIDsFile == null)
			throw new NullPointerException("Spectrum IDs file is null.");
		else if (spectrumIDsFile.isFile() == false ||
			spectrumIDsFile.canRead() == false)
			throw new IllegalArgumentException(String.format(
				"Argument spectrum IDs file [%s] is not a readable file.",
				spectrumIDsFile.getName()));
		// read all lines of the input scans file and store them
		Collection<Integer> scans = new LinkedHashSet<Integer>();
		Integer highestIndex = null;
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(spectrumIDsFile));
			String line = null;
			int lineNumber = 0;
			while (true) {
				line = reader.readLine();
				if (line == null)
					break;
				lineNumber++;
				if (line.trim().equals(""))
					continue;
				String[] tokens = line.split("\\s+");
				if (tokens == null || tokens.length != 3)
					throw new IllegalArgumentException(String.format(
						"Line %d of spectrum IDs file [%s] is invalid:\n" +
						"----------\n%s\n----------\n" +
						"Each non-empty line is expected to consist of " +
						"three tokens separated by whitespace.",
						lineNumber, spectrumIDsFile.getName(), line));
				// parse scan field, which should be an integer
				try {
					scans.add(Integer.parseInt(tokens[1]));
				} catch (NumberFormatException error) {
					throw new IllegalArgumentException(String.format(
						"Line %d of spectrum IDs file [%s] is invalid:\n" +
						"----------\n%s\n----------\n" +
						"The second token [%s] is expected to be an integer " +
						"representing this spectrum's scan number.",
						lineNumber, spectrumIDsFile.getName(), line,
						tokens[1]), error);
				}
				// increment index; use 1-based indexing, just to be sure
				// any potentially valid client index value will be matched
				// even though mzTab indices are supposed to be 0-based
				if (highestIndex == null)
					highestIndex = 1;
				else highestIndex++;
			}
		} catch (Throwable error) {
			throw new RuntimeException(error);
		} finally {
			try { reader.close(); }
			catch (Throwable error) {}
		}
		if (scans.isEmpty() || highestIndex == null)
			return null;
		else return new ImmutablePair<Integer, Collection<Integer>>(
			highestIndex, scans);
	}
	
	@SuppressWarnings("unused")
	private String validateNativeID(
		String nativeID, String sequence,
		ImmutablePair<Integer, Collection<Integer>> spectrumIDs
	) {
		if (nativeID == null)
			return null;
		// extract the spectrum identifier from the nativeID string
		Boolean scan = null;
		Integer value = null;
		// first try to extract a scan number
		Matcher matcher = MzTabConstants.SCAN_PATTERN.matcher(nativeID);
		if (matcher.find()) try {
			value = Integer.parseInt(matcher.group(1));
			scan = true;
		}
		// it should be impossible for a parsing error to occur here, since the
		// nativeID value matched the integer portion of the regular expression
		catch (NumberFormatException error) {
			throw new IllegalStateException(error);
		}
		// then try to extract an index
		if (value == null) {
			matcher = MzTabConstants.INDEX_PATTERN.matcher(nativeID);
			if (matcher.find()) try {
				// if found, assume all spectrum indices are 0-based,
				// as per the definition of nativeID format
				// MS:1000774 ("multiple peak list nativeID format")
				value = Integer.parseInt(matcher.group(1));
				scan = false;
			} catch (NumberFormatException error) {
				throw new IllegalStateException(error);
			}
		}
		// then try to extract a "query"
		if (value == null) {
			matcher = MzTabConstants.QUERY_PATTERN.matcher(nativeID);
			if (matcher.find()) try {
				// nativeIDs of type MS:1001528 ("Mascot query number") are
				// defined to be 1-based indices. However, since we encode all
				// indices as nativeIDs of type MS:1000774 ("multiple peak
				// list nativeID format"), and this format requires 0-based
				// indexing, we must decrement the query number here.
				value = Integer.parseInt(matcher.group(1)) - 1;
				scan = false;
			} catch (NumberFormatException error) {
				throw new IllegalStateException(error);
			}
		}
		// if it's a file specifier, and no index was also specified,
		// then assume it's just a 1-spectrum file
		if (value == null) {
			matcher = MzTabConstants.FILE_PATTERN.matcher(nativeID);
			if (matcher.find()) {
				value = 0;
				scan = false;
			}
		}
		// at this point, the nativeID is not of any recognized or
		// supported type, so try to parse it as a plain integer
		if (value == null) try {
			value = Integer.parseInt(nativeID);
		} catch (NumberFormatException error) {}
		// if the nativeID can't even be parsed as a plain integer, then
		// it's truly bad and there's nothing more we can do about it
		if (value == null)
			return null;
		// if we were able to determine the spectrum ID type directly from
		// the nativeID, then just check against that type in the IDs map
		else if (scan != null) {
			if (scan && isSpectrumIDInMap(value, true, spectrumIDs))
				return String.format("scan=%d", value);
			else if (scan == false &&
				isSpectrumIDInMap(value, false, spectrumIDs))
				return String.format("index=%d", value);
			else return null;
		}
		// if it's just a plain integer, then we don't know if it's a scan or
		// and index, so look it up in the source mzid file (if there is one)
		else if (scan == null) try {
			if (mzidCache.isScan(mzTabFile, sequence, value))
				return String.format("scan=%d", value);
			else return String.format("index=%d", value);
		}
		// if looking it up in the mzid file didn't help,
		// then handle it as an "ambiguous" nativeID
		catch (UnverifiableNativeIDException error) {
			// if no ambiguous nativeID interpretation scheme has been
			// selected yet, then try both, with scan first by default
			if (ambiguousNativeIDsAsScans == null) {
				if (isSpectrumIDInMap(value, true, spectrumIDs)) {
					ambiguousNativeIDsAsScans = true;
					return String.format("scan=%d", value);
				}
				else if (isSpectrumIDInMap(value, false, spectrumIDs)) {
					ambiguousNativeIDsAsScans = false;
					return String.format("index=%d", value);
				}
				else return null;
			}
			// if a scheme has already been selected, then try that one only
			else if (ambiguousNativeIDsAsScans &&
				isSpectrumIDInMap(value, true, spectrumIDs))
				return String.format("scan=%d", value);
			else if (ambiguousNativeIDsAsScans == false &&
				isSpectrumIDInMap(value, false, spectrumIDs))
				return String.format("index=%d", value);
			else return null;
		}
		return null;
	}
	
	private boolean isSpectrumIDInMap(
		Integer value, boolean scan,
		ImmutablePair<Integer, Collection<Integer>> spectrumIDs
	) {
		if (value == null || spectrumIDs == null)
			return false;
		// if this is a scan number, then it must be in the scans
		// collection to be verified present in the source file
		if (scan) {
			Collection<Integer> scans = spectrumIDs.getRight();
			if (scans == null || scans.isEmpty())
				return false;
			else return scans.contains(value);
		}
		// otherwise, it's an index, so just check that the highest index
		// found in the file is greater than or equal to this one
		else {
			Integer highestIndex = spectrumIDs.getLeft();
			if (highestIndex == null)
				return false;
			else return value <= highestIndex;
		}
	}
	
	private String getLine(String[] tokens) {
		if (tokens == null || tokens.length < 1)
			return null;
		StringBuilder line = new StringBuilder();
		for (int i=0; i<tokens.length; i++)
			line.append(tokens[i]).append("\t");
		// chomp trailing tab ("\t")
		if (line.charAt(line.length() - 1) == '\t')
			line.setLength(line.length() - 1);
		return line.toString();
	}
}
