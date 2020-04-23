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

public class SpectraRefValidationProcessor
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
	private Map<String, ImmutablePair<Integer, Collection<String>>> spectra;
	private MzIdentMLNativeIDMap mzidCache;
	
	/*========================================================================
	 * Constructor
	 *========================================================================*/
	public SpectraRefValidationProcessor(
		File uploadedResultDirectory, File spectrumIDsDirectory
	) {
		this(uploadedResultDirectory, spectrumIDsDirectory, null);
	}
	
	public SpectraRefValidationProcessor(
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
			String, ImmutablePair<Integer, Collection<String>>>();
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
					ImmutablePair<Integer, Collection<String>> spectrumIDs =
						readSpectrumIDsFile(spectrumIDsFile);
					if (spectrumIDs != null)
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
				headers.add(MzTabConstants.VALID_COLUMN);
			}
			if (invalidReasonIndex < 0) {
				invalidReasonIndex = headers.size();
				line = String.format("%s\t%s",
					line.trim(), MzTabConstants.INVALID_REASON_COLUMN);
				headers.add(MzTabConstants.INVALID_REASON_COLUMN);
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
				// set the valid column's value to "VALID" by default
				row[validIndex] = "VALID";
				line = getLine(row);
			}
			// ensure that the invalid reason column is present in this row
			if (invalidReasonIndex >= row.length) {
				String[] newRow = new String[invalidReasonIndex + 1];
				for (int i=0; i<row.length; i++)
					newRow[i] = row[i];
				row = newRow;
				// set the invalid reason column's value to "null" by default
				row[invalidReasonIndex] = "null";
				line = getLine(row);
			}
			// retrieve the spectra_ref and peptide sequence column values
			String spectraRef = row[spectraRefIndex];
			String sequence = row[sequenceIndex];
			// parse the spectra_ref column value
			// into file reference and nativeID
			Matcher matcher =
				MzTabConstants.SPECTRA_REF_PATTERN.matcher(spectraRef);
			if (matcher.matches() == false) {
				row[validIndex] = "INVALID";
				row[invalidReasonIndex] = String.format(
					"Invalid \"spectra_ref\" column value [%s]: " +
					"this value does not  conform to the " +
					"expected format [%s].", spectraRef,
					"ms_run[1-n]:<nativeID-formatted identifier string>");
				return getLine(row);
			}
			// extract and validate ms_run index
			int msRunIndex;
			try { msRunIndex = Integer.parseInt(matcher.group(1)); }
			// it should be impossible for a parsing error to occur
			// here, since the spectra_ref column value matched
			// the integer portion of the regular expression
			catch (NumberFormatException error) {
				throw new IllegalStateException(error);
			}
			if (msRunIndex <= 0) {
				row[validIndex] = "INVALID";
				row[invalidReasonIndex] = String.format(
					"Invalid \"spectra_ref\" column value [%s]: " +
					"this value contains invalid " +
					"ms_run index %d; ms_run indices should start at 1.",
					spectraRef, msRunIndex);
				return getLine(row);
			}
			// if this PSM row has already been marked as invalid by some
			// upstream validator, then don't bother with this validation
			String valid = row[validIndex];
			if (valid != null && valid.trim().equalsIgnoreCase("INVALID"))
				return line;
			// also don't bother with further validation unless parsed spectra
			// are present; some workflows don't do this and therefore their
			// PSMs cannot be validated against any source peak list files
			else if (spectra == null || spectra.isEmpty())
				return line;
			// extract nativeID
			String nativeID = matcher.group(2);
			// get spectrum IDs file for this PSM row
			MzTabMsRun msRun = mzTabFile.getMsRun(msRunIndex);
			String mangledPeakListFilename = msRun.getMangledPeakListFilename();
			if (mangledPeakListFilename == null) {
				row[validIndex] = "INVALID";
				row[invalidReasonIndex] = String.format(
					"Could not resolve any file mapping for \"ms_run\" " +
					"index %d.", msRunIndex);
				return getLine(row);
			}
			String spectrumIDsFilename = String.format(
				"%s.scans", FilenameUtils.getBaseName(mangledPeakListFilename));
			ImmutablePair<Integer, Collection<String>> spectrumIDs =
				getSpectrumIDs(spectrumIDsFilename);
			if (spectrumIDs == null) {
				row[validIndex] = "INVALID";
				row[invalidReasonIndex] = String.format(
					"No spectra were found for \"ms_run\" index %d, " +
					"corresponding to peak list file [%s] " +
					"(parsed into spectra summary file [%s]).", msRunIndex,
					msRun.getPeakListFilename(), spectrumIDsFilename);
				return getLine(row);
			}
			// validate nativeID
			String validatedNativeID =
				validateNativeID(nativeID, sequence, spectrumIDs);
			// if the nativeID was successfully validated, and
			// its value changed, then overwrite the previous
			// spectra_ref value with the validated one
			if (validatedNativeID != null) {
				String validatedSpectraRef = String.format(
					"ms_run[%d]:%s", msRunIndex, validatedNativeID);
				if (spectraRef.equals(validatedSpectraRef) == false) {
					row[spectraRefIndex] = validatedSpectraRef;
					line = getLine(row);
				}
			}
			// if the nativeID could not be validated, and the current scheme
			// for interpreting ambiguous nativeIDs is scans, then the index
			// scheme might still work; throw an exception that will inform
			// the client to try again with a hard-coded index scheme
			else if (ambiguousNativeIDsAsScans != null &&
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
		return line;
	}
	
	public void tearDown() {}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private ImmutablePair<Integer, Collection<String>> getSpectrumIDs(
		String spectrumIDsFilename
	) {
		if (spectrumIDsFilename == null)
			return null;
		else return spectra.get(spectrumIDsFilename);
	}
	
	private ImmutablePair<Integer, Collection<String>>
	readSpectrumIDsFile(File spectrumIDsFile) {
		if (spectrumIDsFile == null)
			throw new NullPointerException("Spectrum IDs file is null.");
		else if (spectrumIDsFile.isFile() == false ||
			spectrumIDsFile.canRead() == false)
			throw new IllegalArgumentException(String.format(
				"Argument spectrum IDs file [%s] is not a readable file.",
				spectrumIDsFile.getName()));
		// read all lines of the input file and store them
		Collection<String> nativeIDs = new LinkedHashSet<String>();
		Integer maxMS2Index = null;
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
				String[] tokens = line.split("\\t");
				if (tokens == null || tokens.length != 3)
					throw new IllegalArgumentException(String.format(
						"Line %d of spectrum IDs file [%s] is invalid:\n" +
						"----------\n%s\n----------\n" +
						"Each non-empty line is expected to consist of " +
						"three tokens separated by tabs.",
						lineNumber, spectrumIDsFile.getName(), line));
				// only process MS2+ spectra (i.e. having value >1 in the MS level column)
				try {
					if (Integer.parseInt(tokens[1]) <= 1)
						continue;
				} catch (NumberFormatException error) { continue; }
				// parse nativeID field - might be a comma-separated list
				String[] theseNativeIDs = tokens[0].split(",");
				for (String nativeID : theseNativeIDs) {
					nativeID = nativeID.trim();
					// ignore "null" nativeIDs
					if (nativeID.equalsIgnoreCase("null"))
						continue;
					else nativeIDs.add(nativeID);
				}
				// track max MS2+ index found so far
				try {
					int index = Integer.parseInt(tokens[2]);
					if (maxMS2Index == null || index > maxMS2Index)
						maxMS2Index = index;
				}
				// if for some reason the reported index is not
				// a valid integer then increment manually
				catch (NumberFormatException error) {
					if (maxMS2Index == null)
						maxMS2Index = 0;
					else maxMS2Index++;
				}
			}
		} catch (Throwable error) {
			throw new RuntimeException(error);
		} finally {
			try { reader.close(); }
			catch (Throwable error) {}
		}
		if (nativeIDs.isEmpty() && maxMS2Index == null)
			return null;
		// increment reported max MS2+ index (assumed to be 0-based),
		// just to be sure any potentially valid client index value will
		// be matched even though mzTab indices are supposed to be 0-based
		else return new ImmutablePair<Integer, Collection<String>>(
			(maxMS2Index + 1), nativeIDs);
	}
	
	private String validateNativeID(
		String nativeID, String sequence,
		ImmutablePair<Integer, Collection<String>> spectrumIDs
	) {
		if (nativeID == null)
			return null;
		// first check if the source nativeID was found as-is in the file
		Collection<String> nativeIDs = spectrumIDs.getRight();
		if (isNativeIDInFile(nativeID, nativeIDs))
			return nativeID;
		// if not, then the nativeID might be slightly modified; try different formats
		String modifiedNativeID = null;
		Integer index = null;
		// first try "scan="
		Matcher matcher = MzTabConstants.SCAN_PATTERN.matcher(nativeID);
		if (matcher.find()) {
			modifiedNativeID = matcher.group();
			if (modifiedNativeID.equals(nativeID) == false &&
				isNativeIDInFile(modifiedNativeID, nativeIDs))
				return modifiedNativeID;
		}
		// next try the relatively rare "scanId="
		matcher = MzTabConstants.SCAN_ID_PATTERN.matcher(nativeID);
		if (matcher.find()) {
			modifiedNativeID = matcher.group();
			if (modifiedNativeID.equals(nativeID) == false &&
				isNativeIDInFile(modifiedNativeID, nativeIDs))
				return modifiedNativeID;
		}
		// next try "index="
		matcher = MzTabConstants.INDEX_PATTERN.matcher(nativeID);
		if (matcher.find()) {
			modifiedNativeID = matcher.group();
			if (modifiedNativeID.equals(nativeID) == false &&
				isNativeIDInFile(modifiedNativeID, nativeIDs))
				return modifiedNativeID;
			else try { index = Integer.parseInt(matcher.group(1)); }
			catch (NumberFormatException error) { throw new IllegalStateException(error); }
		}
		// next try Mascot "query="
		matcher = MzTabConstants.QUERY_PATTERN.matcher(nativeID);
		if (matcher.find()) {
			modifiedNativeID = matcher.group();
			if (modifiedNativeID.equals(nativeID) == false &&
				isNativeIDInFile(modifiedNativeID, nativeIDs))
				return modifiedNativeID;
			// Commented out the code below since we currently cannot properly handle
			// "query=" nativeIDs as indices, as there is no consistent way to dereference
			// the query number in Mascot search results converted to mzTab
//			// nativeIDs of type MS:1001528 ("Mascot query number") are
//			// defined to be 1-based indices. However, since we encode all
//			// indices as nativeIDs of type MS:1000774 ("multiple peak
//			// list nativeID format"), and this format requires 0-based
//			// indexing, we must decrement the query number here.
//			else try { index = Integer.parseInt(matcher.group(1)) - 1; }
//			catch (NumberFormatException error) { throw new IllegalStateException(error); }
		}
		// next try "file="
		matcher = MzTabConstants.FILE_PATTERN.matcher(nativeID);
		if (matcher.find()) {
			modifiedNativeID = matcher.group();
			if (modifiedNativeID.equals(nativeID) == false &&
				isNativeIDInFile(modifiedNativeID, nativeIDs))
				return modifiedNativeID;
			// with "file=" nativeIDs we assume it's just a 1-spectrum file
			index = 0;
		}
		// at this point, the nativeID was not found in the file even
		// after being transformed to all recognized types, so if an
		// index was found then just use that and assume it's correct
		if (index != null)
			return String.format("index=%d", index);
		// otherwise try to parse it as a plain integer
		Integer value = null;
		try {
			value = Integer.parseInt(nativeID);
		} catch (NumberFormatException error) {}
		// if the nativeID can't even be parsed as a plain integer, then
		// it's truly bad and there's nothing more we can do about it
		if (value == null)
			return null;
		// if it's just a plain integer, then we don't know if it's a scan or
		// index so look it up in the source mzid file (if there is one)
		else try {
			if (mzidCache.isScan(mzTabFile, sequence, value))
				return String.format("scan=%d", value);
			// fall back on index if it couldn't be found as a scan in the mzid
			else return String.format("index=%d", value);
		}
		// if looking it up in the mzid file didn't help,
		// then handle it as an "ambiguous" nativeID
		catch (UnverifiableNativeIDException error) {
			// if no ambiguous nativeID interpretation scheme has been
			// selected yet, then try both, with scan first by default
			Integer maxMS2Index = spectrumIDs.getLeft();
			if (ambiguousNativeIDsAsScans == null) {
				nativeID = findScanInFile(value, nativeIDs);
				if (nativeID != null) {
					ambiguousNativeIDsAsScans = true;
					return nativeID;
				}
				nativeID = findIndexInFile(value, maxMS2Index);
				if (nativeID != null) {
					ambiguousNativeIDsAsScans = false;
					return nativeID;
				}
			}
			// if a scheme has already been selected, then try that one only
			else if (ambiguousNativeIDsAsScans) {
				nativeID = findScanInFile(value, nativeIDs);
				if (nativeID != null)
					return nativeID;
			}
			else {
				nativeID = findIndexInFile(value, maxMS2Index);
				if (nativeID != null)
					return nativeID;
			}
			return null;
		}
	}
	
	private boolean isNativeIDInFile(
		String nativeID, Collection<String> nativeIDs
	) {
		if (nativeID == null || nativeIDs == null)
			return false;
		else return nativeIDs.contains(nativeID);
	}
	
	private String findScanInFile(
		Integer scan, Collection<String> nativeIDs
	) {
		if (scan == null || nativeIDs == null)
			return null;
		// try exact string match with all known scan number nativeID formats
		String nativeID = String.format("scan=%d", scan);
		if (isNativeIDInFile(nativeID, nativeIDs))
			return nativeID;
		nativeID = String.format("scanId=%d", scan);
		if (isNativeIDInFile(nativeID, nativeIDs))
			return nativeID;
		// look through nativeID set and see if any match
		// known scan number nativeID substring patterns
		String value = Integer.toString(scan);
		for (String nativeIDInFile : nativeIDs) {
			Matcher matcher = MzTabConstants.SCAN_PATTERN.matcher(nativeIDInFile);
			if (matcher.find() && matcher.group(1).equals(value))
				return matcher.group();
			matcher = MzTabConstants.SCAN_ID_PATTERN.matcher(nativeIDInFile);
			if (matcher.find() && matcher.group(1).equals(value))
				return matcher.group();
		}
		// if the argument scan was not found in the file using any known
		// scan number pattern (in whole or part), then it's really just not there
		return null;
	}
	
	private String findIndexInFile(
		Integer index, Integer maxMS2Index
	) {
		if (index == null || maxMS2Index == null)
			return null;
		// we cannot assume the ordinality of the argument index, so we must use an
		// inclusive comparison since the max index found is enforced to be 1-based
		if (index <= maxMS2Index)
			return String.format("index=%d", index);
		else return null;
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
