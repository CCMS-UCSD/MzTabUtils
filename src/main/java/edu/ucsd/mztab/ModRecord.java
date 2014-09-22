package edu.ucsd.mztab;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.lang3.tuple.ImmutablePair;

import edu.ucsd.util.OntologyUtils;
import uk.ac.ebi.pride.jmztab.model.CVParam;
import uk.ac.ebi.pride.jmztab.model.Param;
import uk.ac.ebi.pride.jmztab.model.UserParam;

public class ModRecord
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final Pattern PARAM_PATTERN = Pattern.compile("^\\[" +
		"\\s*([^,]*)," +				// CV label
		"\\s*([^,]*)," +				// accession
		"\\s*\"?([^\"]*)\"?," +			// name
		"\\s*\"?([^\"]*)\"?" +			// value
		"\\s*\\]$");
	private static final Pattern FLOAT_PATTERN = Pattern.compile(
		"((?:[+-]?\\d+\\.?\\d*)|(?:[+-]?\\d*\\.?\\d+))");
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
	 * Properties
	 *========================================================================*/
	private Param                 param;
	private String                modID;
	private Double                mass;
	private Collection<Character> sites;
	private Pattern               pattern;
	private boolean               fixed;
	
	/*========================================================================
	 * Constructors
	 *========================================================================*/
	public ModRecord(String paramDescriptor, boolean fixed) {
		// validate param descriptor string
		if (paramDescriptor == null)
			throw new IllegalArgumentException(
				"Modification parameter string argument cannot be null.");
		Matcher matcher = PARAM_PATTERN.matcher(paramDescriptor);
		if (matcher.matches() == false)
			throw new IllegalArgumentException(String.format(
				"Argument parameter string [%s] does not conform to the " +
				"required format of a square bracket-enclosed (\"[]\") " +
				"parameter tuple:\n%s",
				paramDescriptor, "[cvLabel, accession, name, value]"));
		// extract mod ID from the last element of the CV tuple
		modID = matcher.group(4);
		// build param object from parsed string elements
		Param param = null;
		// if there's no accession, then it's a user param,
		// otherwise it's a CV param
		String accession = matcher.group(2);
		if (accession == null || accession.trim().equals(""))
			param = new UserParam(matcher.group(3), null);
		else {
			// ensure that a proper CV label is present; if the user didn't
			// provide one, then we can extract it from the accession
			String label = matcher.group(1);
			if (label == null || label.trim().equals("")) {
				String[] tokens = accession.split(":");
				if (tokens == null || tokens.length != 2)
					throw new IllegalArgumentException(String.format(
						"The \"accession\" element [%s] of argument " +
						"parameter string [%s] does not conform to the " +
						"required format of a CV accession string:\n%s",
						accession, paramDescriptor, "<cvLabel>:<accession>"));
				else label = tokens[0];
			}
			// ensure that a proper CV term name is present; if the user didn't
			// provide one, then we can look it up in the ontology
			String name = matcher.group(3);
			if (name == null || name.trim().equals("")) {
				name = OntologyUtils.getOntologyModificationName(accession);
				if (name == null)
					throw new IllegalArgumentException(String.format(
						"No CV entry could be found for \"accession\" " +
						"element [%s] of argument parameter string [%s].",
						accession, paramDescriptor));
			}
			param = new CVParam(label, accession, name, null);
		}
		setParam(param);
		// set fixed/variable status
		this.fixed = fixed;
	}
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public ImmutablePair<String, Collection<Integer>> parsePSM(String psm) {
		if (psm == null)
			return null;
		// iteratively apply this mod's regular expression, to extract any
		// occurrences of this mod, and return the cleaned PSM string with
		// all such occurrences removed
		String cleaned = psm;
		Collection<Integer> occurrences = new LinkedHashSet<Integer>();
		while (true) {
			Matcher matcher = pattern.matcher(cleaned);
			if (matcher.find() == false)
				break;
			String captured = matcher.group();
//
System.out.println(String.format("Captured = [%s]", captured));
//
			// if the captured region contains no mod-indicating
			// characters, then break to avoid an infinite loop
			if (captured == null || captured.trim().isEmpty() ||
				captured.matches("^[ARDNCEQGHILKMFPSTWYV]+$"))
				break;
			ImmutablePair<Integer, String> extracted =
				extractMod(cleaned, captured);
			if (extracted != null) {
				cleaned = extracted.getValue();
				occurrences.add(extracted.getLeft());
			}
			// it should be impossible for the extraction operation to fail,
			// since the matcher ensures that the mod is present in the PSM
			else throw new IllegalStateException();
		}
		// if this mod is fixed, then also iterate over the characters of the
		// cleaned peptide string, since the mod was probably not explicitly
		// written into the PSM string by the upstream search engine
		if (isFixed()) {
			int index = 0;
			for (int i=0; i<cleaned.length(); i++) {
				// only count amino acid characters to
				// keep track of the unmodified index
				char current = cleaned.charAt(i);
				if (AMINO_ACID_MASSES.containsKey(current))
					index++;
				// if this is a site affected by this fixed mod, then add it
				if (sites.contains(current))
					occurrences.add(index);
			}
		}
		if (occurrences != null && occurrences.isEmpty())
			occurrences = null;
		return new ImmutablePair<String, Collection<Integer>>(
			cleaned, occurrences);
	}
	
	public String modifyPSM(String psm) {
		if (psm == null)
			return null;
		return psm;
	}
	
	public String printModOccurrences(Collection<Integer> occurrences) {
		if (occurrences == null)
			return null;
		StringBuffer list = new StringBuffer();
		for (int index : occurrences)
			list.append(index).append("-").append(toString()).append(",");
		if (list == null || list.length() < 1)
			return null;
		// chomp trailing comma
		else if (list.charAt(list.length() - 1) == ',')
			list.setLength(list.length() - 1);
		return list.toString();
	}
	
	@Override
	public String toString() {
		String accession = getAccession();
		if (accession == null || accession.trim().equals("") ||
			accession.equals("MS:1001460"))
			return "CHEMMOD:" + getMass();
		else return accession;
	}
	
	/*========================================================================
	 * Property accessor methods
	 *========================================================================*/
	public String getCVLabel() {
		return param.getCvLabel();
	}
	
	public String getAccession() {
		return param.getAccession();
	}
	
	public String getName() {
		return param.getName();
	}
	
	public String getValue() {
		return param.getValue();
	}
	
	public String getModIDString() {
		return modID;
	}
	
	public String getMass() {
		if (mass == null)
			return null;
		String formattedMass;
		if (mass == (int)mass.doubleValue())
			formattedMass = String.format("%d", (int)mass.doubleValue());
		else formattedMass = String.format("%s", mass);
		// prepend a "+" if this is a non-negative mass offset
		if (mass >= 0.0 && formattedMass.startsWith("+") == false)
			formattedMass = "+" + formattedMass;
		return formattedMass;
	}
	
	public Collection<Character> getSites() {
		if (sites == null || sites.isEmpty())
			return null;
		else return new LinkedHashSet<Character>(sites);
	}
	
	public boolean isFixed() {
		return fixed;
	}
	
	public boolean isVariable() {
		return fixed == false;
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private void setParam(Param param) {
		if (param == null)
			throw new IllegalArgumentException(
				"Modification parameter argument cannot be null.");
		else this.param = param;
		// try to get mass from the CV accession
		mass = OntologyUtils.getOntologyModificationMass(param.getAccession());
		// if the mass could not be found in the ontology,
		// then try to extract it from the mod ID string
		if (mass == null)
			setModIDMass(getModIDString());
		// if the mod is unknown or a user param, set the mass as its value
		String accession = param.getAccession();
		if (accession == null || accession.trim().equals(""))
			this.param = new UserParam(param.getName(), getMass());
		else if (accession.equals("MS:1001460"))
			this.param = new CVParam(
				param.getCvLabel(), accession, param.getName(), getMass());
		// set site collection and pattern
		setModIDProperties(getModIDString());
	}
	
	private void setModIDMass(String modID) {
		if (modID == null)
			throw new NullPointerException(
				"Mod ID string argument cannot be null.");
		// initialize mass
		mass = null;
		// try to extract the mass value from this string
		Matcher matcher = FLOAT_PATTERN.matcher(modID);
		if (matcher.find() == false)
			throw new IllegalArgumentException(String.format(
				"No numerical mass value could be extracted " +
				"from mod ID string [%s].", modID));
		mass = Double.parseDouble(matcher.group(1));
		// if the string contains more than one mass value,
		// then it's ambiguous which one to pick
		if (matcher.find())
			throw new IllegalArgumentException(String.format(
				"Multiple numerical mass values were extracted " +
				"from mod ID string [%s].", modID));
	}
	
	private void setModIDProperties(String modID)
	throws PatternSyntaxException {
		if (modID == null)
			throw new NullPointerException(
				"Mod ID string argument cannot be null.");
		// initialize site collection and pattern
		sites = null;
		pattern = null;
		// parse mod ID string to extract affected amino acid sites, and build
		// a regular expression pattern to detect this mod in PSM strings
		Set<Character> foundAminoAcids = new LinkedHashSet<Character>();
		StringBuffer pattern = new StringBuffer();
		for (int i=0; i<modID.length(); i++) {
			char current = modID.charAt(i);
			// if the current character is an asterisk ("*"), then add all
			// known amino acids to the regular expression for this region
			if (current == '*') {
				// asterisks don't make sense if other
				// amino acids were already specified
				if (foundAminoAcids.isEmpty() == false)
					throw new IllegalArgumentException(String.format(
						"Found an asterisk (\"*\") at position %d in mod ID " +
						"string [%s], even though other site references had " +
						"already been found in the same string.", i, modID));
				for (char aminoAcid : AMINO_ACID_MASSES.keySet())
					foundAminoAcids.add(aminoAcid);
				continue;
			}
			// if the current character is a standalone amino acid,
			// then add it to the regular expression for this region
			else if (AMINO_ACID_MASSES.containsKey(current)) {
				// redundant amino acids are not allowed
				if (foundAminoAcids.contains(current))
					throw new IllegalArgumentException(String.format(
						"Found site \"%c\" at position %d in mod ID string " +
						"[%s], even though a reference to this site had " +
						"already been found in the same string.",
						current, i, modID));
				else foundAminoAcids.add(current);
				continue;
			}
			// if the current character is not an amino acid, then add any
			// recently found amino acids to the regular expression
			else if (foundAminoAcids.isEmpty() == false) {
				setSites(foundAminoAcids, modID);
				pattern.append("[");
				for (char aminoAcid : foundAminoAcids)
					pattern.append(aminoAcid);
				pattern.append("]");
				foundAminoAcids.clear();
			}
			// add the regex-escaped character to the pattern
			pattern.append(Pattern.quote(Character.toString(current)));
		}
		// if the ID string ended in an amino acid pattern, add it now
		if (foundAminoAcids.isEmpty() == false) {
			setSites(foundAminoAcids, modID);
			pattern.append("[");
			for (char aminoAcid : foundAminoAcids)
				pattern.append(aminoAcid);
			pattern.append("]");
		}
		// if the ID string contained no amino acid references,
		// then try to look up the sites in the ontology
		else if (this.sites == null) {
			Collection<String> sites =
				OntologyUtils.getOntologyModificationSites(getAccession());
			// if ontology sites were found, process them
			if (sites != null && sites.isEmpty() == false) {
				for (String site : sites) {
					// add any amino acid site found from the ontology lookup
					char residue = site.charAt(0);
					if (site.length() == 1 &&
						AMINO_ACID_MASSES.containsKey(residue))
						foundAminoAcids.add(residue);
					// otherwise, the value must represent a generic site
					// like N-term, so clear the found amino acids to trigger
					// a "*" addition of all known amino acids
					else {
						foundAminoAcids.clear();
						break;
					}
				}
			}
			// if the found amino acids set is still empty, then it's a generic
			// site, and all known amino acids should be added just like a "*"
			if (foundAminoAcids.isEmpty())
				for (char aminoAcid : AMINO_ACID_MASSES.keySet())
					foundAminoAcids.add(aminoAcid);
			setSites(foundAminoAcids, modID);
		}
		this.pattern = Pattern.compile(pattern.toString());
	}
	
	private void setSites(Collection<Character> sites, String modID) {
		if (sites == null)
			throw new NullPointerException(
				"Amino acid sites collection cannot be null.");
		// the modification's sites should be immutable, so
		// throw an error if a set has already been added
		else if (this.sites != null)
			throw new IllegalArgumentException(String.format(
				"Found multiple regions of amino acid " +
				"references in mod ID string [%s].", modID));
		else this.sites = new LinkedHashSet<Character>(sites);
	}
	
	private ImmutablePair<Integer, String> extractMod(String psm, String mod) {
		if (psm == null|| mod == null)
			return null;
		// determine mod substring's indices
		int start = psm.indexOf(mod);
		if (start < 0)
			return null;
		int end = start + mod.length();
		// iterate over the PSM string, counting only amino acid characters,
		// to generate the correct index within the unmodified peptide string
		int index = 0;
		for (int i=0; i<end; i++) {
			if (AMINO_ACID_MASSES.containsKey(psm.charAt(i))) {
				index++;
				// if we've already reached the mod region, then the
				// first amino acid we find is the affected site
				if (i >= start)
					break;
			}
		}
		// clean the substring by removing all non-amino acid characters
		StringBuffer cleaned = new StringBuffer();
		for (int i=0; i<mod.length(); i++) {
			char current = mod.charAt(i);
			if (AMINO_ACID_MASSES.containsKey(current))
				cleaned.append(current);
		}
		// splice the cleaned substring into the original string
		return new ImmutablePair<Integer, String>(index, String.format("%s%s%s",
			psm.substring(0, start), cleaned.toString(), psm.substring(end)));
	}
}
