package edu.ucsd.util;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.regex.Matcher;

import org.apache.commons.lang3.tuple.ImmutablePair;

import uk.ac.ebi.pride.jmztab.model.Modification;
import edu.ucsd.mztab.ModRecord;

public class PeptideUtils
{
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public static String cleanPeptide(String psm) {
		if (psm == null)
			return null;
		// first, check for the typical "enclosing dot" syntax
		// TODO: the user should specify if this syntax is present, and
		// therefore whether or not this processing should even be done
		Matcher matcher = ModRecord.PEPTIDE_STRING_PATTERN.matcher(psm);
		if (matcher.matches())
			psm = matcher.group(2);
		// then remove all non-amino acid characters from the sequence
		StringBuffer clean = new StringBuffer();
		for (int i=0; i<psm.length(); i++) {
			char current = psm.charAt(i);
			if (ModRecord.AMINO_ACID_MASSES.containsKey(current))
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
