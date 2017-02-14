package edu.ucsd.mztab.util;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.ucsd.mztab.exceptions.InvalidMzTabColumnValueException;
import edu.ucsd.mztab.model.Modification;
import edu.ucsd.mztab.model.MzTabConstants;

public class ProteomicsUtils
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	public static final Pattern PEPTIDE_STRING_PATTERN = Pattern.compile(
		"^\"?" +			// optional opening quotation mark
		"\\[?(.+?)\\]?" +	// "pre" amino acid (optionally bracketed)
		"\\.(.*)\\." +		// actual peptide sequence
		"\\[?(.+?)\\]?" +	// "post" amino acid (optionally bracketed)
		"\"?$"				// optional closing quotation mark
	);
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
	 * Public interface methods
	 *========================================================================*/
	public static boolean isPeptide(String value) {
		if (value == null || value.isEmpty())
			return false;
		// check every character of the string value
		// to ensure that they are all amino acids
		else for (int i=0; i<value.length(); i++) {
			char current = Character.toUpperCase(value.charAt(i));
			if (AMINO_ACID_MASSES.containsKey(current) == false)
				return false;
		}
		return true;
	}
	
	public static String cleanPeptide(String psm) {
		if (psm == null)
			return null;
		// first, check for the typical "enclosing dot" syntax
		// TODO: the user should specify if this syntax is present, and
		// therefore whether or not this processing should even be done
		Matcher matcher = PEPTIDE_STRING_PATTERN.matcher(psm);
		if (matcher.matches())
			psm = matcher.group(2);
		// then remove all non-amino acid characters from the sequence
		StringBuffer clean = new StringBuffer();
		for (int i=0; i<psm.length(); i++) {
			// explicitly cast this character to upper case, since
			// some TSV formats use lower-case amino acid characters
			// to represent modified sites; presumably lower-case
			// amino acid characters will not be used to encode anything
			// other than valid members of the peptide sequence
			char current = Character.toUpperCase(psm.charAt(i));
			if (AMINO_ACID_MASSES.containsKey(current))
				clean.append(current);
		}
		return clean.toString();
	}
	
	public static String addModToPeptide(
		String peptide, double mass, int position
	) {
		if (peptide == null)
			return null;
		// determine the actual string position at which to insert
		// this mod, based on the given sequence position
		int index;
		if (position <= 0)
			index = 0;
		else {
			// determine string position by iterating over sequence string,
			// incrementing only when encountering amino acid characters
			int foundAminoAcids = 0;
			for (index=1; index<=peptide.length(); index++) {
				String character =
					Character.toString(peptide.charAt(index - 1));
				if (MzTabConstants.AMINO_ACID_PATTERN.matcher(character)
					.matches()) {
					foundAminoAcids++;
					if (foundAminoAcids == position)
						break;
				}
			}
			
		}
		// ensure we don't go past the end of the sequence
		if (index > peptide.length())
			index = peptide.length();
		// if the first part of the remaining sequence is a mass offset, then
		// capture it, add this mod's mass to it, and write the sum in its place
		String suffix = peptide.substring(index);
		Matcher matcher = MzTabConstants.FLOAT_PATTERN.matcher(suffix);
		if (matcher.find()) {
			String currentMass = matcher.group();
			int start = suffix.indexOf(currentMass);
			// only sum masses if this mass is at the beginning of the suffix
			if (start == 0) try {
				mass += Double.parseDouble(currentMass);
				peptide = String.format("%s%s", peptide.substring(0, index),
					suffix.substring(currentMass.length()));
			} catch (NumberFormatException error) {}
		}
		// write mass offset into processed peptide string at the proper index
		return new StringBuilder(peptide).insert(index, formatMass(mass))
			.toString();
	}
	
	public static String formatMass(Double mass) {
		if (mass == null)
			return null;
		String formattedMass;
		if (mass == (int)mass.doubleValue())
			formattedMass = String.format("%d", (int)mass.doubleValue());
		else formattedMass = String.format("%s", mass.toString());
		// prepend a "+" if this is a non-negative mass offset
		if (mass >= 0.0 && formattedMass.startsWith("+") == false)
			formattedMass = "+" + formattedMass;
		return formattedMass;
	}
	
	public static Collection<Modification> getModifications(String mods) {
		if (mods == null || mods.equalsIgnoreCase("null"))
			return null;
		Collection<Modification> modifications =
			new LinkedHashSet<Modification>();
		Matcher matcher =
			MzTabConstants.MZTAB_MODIFICATION_PATTERN.matcher(mods);
		if (matcher.find() == false)
			throw new IllegalArgumentException(String.format(
				"Argument modification string [%s] is invalid: mzTab " +
				"modification strings should be comma-delimited " +
				"lists of strings each conforming to the following " +
				"format:\n%s", mods,
				MzTabConstants.MZTAB_MODIFICATION_STRING_FORMAT));
		else do {
			modifications.add(
				new Modification(matcher.group(2), matcher.group(1)));
		} while (matcher.find());
		if (modifications.isEmpty())
			return null;
		else return modifications;
	}
	
	public static Map<Integer, Double> getModificationMasses(
		String mods, String unmodifiedSequence
	) {
		if (unmodifiedSequence == null)
			return null;
		Collection<Modification> modifications = getModifications(mods);
		if (modifications == null || modifications.isEmpty())
			return null;
		else {
			Map<Integer, Double> masses =
				new LinkedHashMap<Integer, Double>(modifications.size());
			for (Modification modification : modifications) {
				try {
					parseModMass(
						modification.toString(), unmodifiedSequence, masses);
				} catch (InvalidMzTabColumnValueException error) {
					//error.printStackTrace();
					continue;
				}
			}
			if (masses.isEmpty())
				return null;
			else return masses;
		}
	}
	
	public static String getModifiedSequence(
		String unmodifiedSequence, String mods
	) {
		if (unmodifiedSequence == null)
			return null;
		String modifiedSequence = unmodifiedSequence;
		Map<Integer, Double> modificationMasses =
			getModificationMasses(mods, unmodifiedSequence);
		if (modificationMasses == null)
			return unmodifiedSequence;
		else for (Integer position : modificationMasses.keySet())
			modifiedSequence = addModToPeptide(
				modifiedSequence, modificationMasses.get(position), position);
		if (modifiedSequence == null)
			return unmodifiedSequence;
		else return modifiedSequence;
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private static void parseModMass(
		String modification, String unmodifiedSequence,
		Map<Integer, Double> masses
	) throws InvalidMzTabColumnValueException {
		if (modification == null || unmodifiedSequence == null ||
			masses == null)
			return;
		Matcher matcher =
			MzTabConstants.MZTAB_MODIFICATION_PATTERN.matcher(modification);
		if (matcher.find() == false)
			throw new InvalidMzTabColumnValueException(String.format(
				"Argument modification string [%s] is invalid: mzTab " +
				"modification strings should be comma-delimited " +
				"lists of strings each conforming to the following " +
				"format:\n%s", modification,
				MzTabConstants.MZTAB_MODIFICATION_STRING_FORMAT));
		String position = matcher.group(1);
		String identifier = matcher.group(2);
		// if this is a neutral loss declaration (which we
		// assume to be the case if it's a CV declaration
		// enclosed by square brackets), then ignore it
		if (identifier != null &&
			MzTabConstants.CV_TERM_PATTERN.matcher(identifier).matches())
			return;
		// validate the mod's {position} element
		else if (position == null || position.trim().equalsIgnoreCase("null")) {
			throw new InvalidMzTabColumnValueException(String.format(
				"A missing or \"null\" value was found in the \"{position}\" " +
				"element of mzTab modification string [%s]. Therefore, this " +
				"modification cannot be unambiguously written into the " +
				"modified peptide string.", modification));
		}
		matcher = MzTabConstants.MZTAB_POSITION_PATTERN.matcher(position);
		if (matcher.matches() == false)
			throw new InvalidMzTabColumnValueException(String.format(
				"The \"{position}\" element [%s] of mzTab modification " +
				"string [%s] does not conform to the required string " +
				"format, as defined in the mzTab format specification, " +
				"section 5.8.", position, modification));
		else if (position.indexOf('|') >= 0)
			throw new InvalidMzTabColumnValueException(String.format(
				"The \"{position}\" element [%s] of mzTab modification " +
				"string [%s] contains one or more pipe (\"|\") " +
				"characters, indicating that the modification's site " +
				"is ambiguous. Therefore, this modification cannot be " +
				"unambiguously written into the modified peptide string.",
				position, modification));
		// try to extract the integer site position of the referenced mod
		int site;
		try {
			site = Integer.parseInt(matcher.group(1));
		} catch (NumberFormatException error) {
			throw new InvalidMzTabColumnValueException(String.format(
				"The \"{position}\" element [%s] of mzTab modification " +
				"string [%s] could not be parsed into a proper integer " +
				"site index. Therefore, this modification cannot be " +
				"unambiguously written into the modified peptide string.",
				position, modification));
		}
		// make sure that the position is within the bounds
		// of the original peptide sequence's length
		if (site < 0 || site > unmodifiedSequence.length())
			throw new InvalidMzTabColumnValueException(String.format(
				"The \"{position}\" element [%s] of mzTab modification " +
				"string [%s] was parsed into an integer of value %d. This " +
				"position falls outside the bounds of the affected peptide " +
				"[%s] (length %d). Therefore, this modification cannot be " +
				"unambiguously written into the modified peptide string.",
				position, modification, site, unmodifiedSequence,
				unmodifiedSequence.length()));
		// try to match the mod's {Modification or Substitution identifier}
		// element against the set of recognized identifier formats, and
		// use that to determine or extract the modification mass 
		String mass = null;
		matcher = MzTabConstants.CV_ACCESSION_PATTERN.matcher(identifier);
		if (matcher.matches()) {
			Double massValue =
				OntologyUtils.getOntologyModificationMass(matcher.group(1));
			if (massValue != null)
				mass = Double.toString(massValue);
		} else {
			matcher = MzTabConstants.MZTAB_CHEMMOD_PATTERN.matcher(identifier);
			if (matcher.matches())
				mass = matcher.group(1);
			else throw new InvalidMzTabColumnValueException(String.format(
				"The \"{Modification or Substitution identifier}\" element " +
				"[%s] of mzTab modification string [%s] was not recognized " +
				"as a valid identifier format, as defined in the mzTab " +
				"format specification, section 5.8. Therefore, this " +
				"modification cannot be unambiguously written into the " +
				"modified peptide string.", identifier, modification));
		}
		// if no mass could be extracted, then this mod can't be written
		double massValue;
		try {
			massValue = Double.parseDouble(mass);
		} catch (Throwable error) {
			throw new InvalidMzTabColumnValueException(String.format(
				"The \"{Modification or Substitution identifier}\" element " +
				"[%s] of mzTab modification string [%s] could not be " +
				"evaluated into a proper numerical mass value. Therefore, " +
				"this modification cannot be unambiguously written into " +
				"the modified peptide string.", identifier, modification));
		}
		// if the mass is 0, ignore this mod
		if (massValue == 0.0)
			return;
		// add this mass offset to the map
		Double current = masses.get(site);
		if (current == null)
			masses.put(site, massValue);
		// be sure to use BigDecimal for the addition operation,
		// since doubles alone result in lame precision issues
		else masses.put(site, BigDecimal.valueOf(current).add(
			BigDecimal.valueOf(massValue)).doubleValue());
	}
}
