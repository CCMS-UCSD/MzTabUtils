package edu.ucsd.util;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.ucsd.mztab.model.Modification;
import edu.ucsd.mztab.model.MzTabConstants;

public class ProteomicsUtils
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	public static final Pattern PEPTIDE_STRING_PATTERN =
		Pattern.compile("^\"?(.)\\.(.*)\\.(.)\"?$");
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
			char current = value.charAt(i);
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
			char current = psm.charAt(i);
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
}
