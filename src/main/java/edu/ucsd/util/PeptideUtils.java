package edu.ucsd.util;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.ImmutablePair;

import uk.ac.ebi.pride.jmztab.model.Modification;
import edu.ucsd.mztab.ModRecord;

public class PeptideUtils
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
	
	public static ImmutablePair<String, Collection<Modification>>
	extractPTMsFromPSM(String psm, Collection<ModRecord> modifications) {
		if (psm == null || modifications == null)
			return null;
		Collection<Modification> mods = new LinkedHashSet<Modification>();
		String current = psm;
		// check the psm string for occurrences of all registered mods
		for (ModRecord record : modifications) {
			ImmutablePair<String, Collection<Modification>> parsedPSM =
				record.parsePSM(current);
			if (parsedPSM == null)
				continue;
			// keep track of the iteratively cleaned PSM string
			String cleaned = parsedPSM.getLeft();
			if (cleaned != null)
				current = cleaned;
			// if no mods of this type were found, continue
			Collection<Modification> theseMods = parsedPSM.getRight();
			if (theseMods != null && theseMods.isEmpty() == false)
				mods.addAll(theseMods);
		}
		if (mods == null || mods.isEmpty())
			return new ImmutablePair<String, Collection<Modification>>(
				current, null);
		else return new ImmutablePair<String, Collection<Modification>>(
			current, mods);
	}
}
