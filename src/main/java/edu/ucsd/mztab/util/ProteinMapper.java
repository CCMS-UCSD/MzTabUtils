package edu.ucsd.mztab.util;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class ProteinMapper
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	public static final Set<String> REFERENCE_PROTEINS;
	public static final Map<String, String> REFERENCE_PROTEINS_BY_ACCESSION;
	public static final Map<String, String>
		REFERENCE_PROTEINS_BY_ID_PLUS_ACCESSION;
	public static final Map<String, String> REFERENCE_PROTEINS_BY_NAME;
	public static final Map<String, String>
		REFERENCE_PROTEINS_BY_ACCESSION_PLUS_NAME;
	public static final Map<String, String> REFERENCE_PROTEINS_BY_DESCRIPTION;
	public static final Map<String, Collection<String>>
		REFERENCE_PROTEINS_BY_SUBSTRING;
	static {
		try {
			// read reference proteins as a Java properties file
			Properties referenceProteins = new Properties();
			referenceProteins.load(
				ProteinMapper.class.getResourceAsStream(
					"reference_proteins.properties"));
			// set up reference protein data structures
			REFERENCE_PROTEINS = new HashSet<String>(referenceProteins.size());
			REFERENCE_PROTEINS_BY_ACCESSION =
				new HashMap<String, String>(referenceProteins.size());
			REFERENCE_PROTEINS_BY_ID_PLUS_ACCESSION =
					new HashMap<String, String>(referenceProteins.size());
			REFERENCE_PROTEINS_BY_NAME =
				new HashMap<String, String>(referenceProteins.size());
			REFERENCE_PROTEINS_BY_ACCESSION_PLUS_NAME =
					new HashMap<String, String>(referenceProteins.size());
			REFERENCE_PROTEINS_BY_DESCRIPTION =
				new HashMap<String, String>(referenceProteins.size());
			REFERENCE_PROTEINS_BY_SUBSTRING =
				new HashMap<String, Collection<String>>();
			// parse each property into its relevant mappings
			for (String protein : referenceProteins.stringPropertyNames()) {
				// each reference protein identifier should have as
				// its value a string with the following format:
				// <database_identifier>|<accession>|<name>
				String[] tokens = protein.split("\\|");
				if (tokens == null || tokens.length != 3)
					throw new IllegalArgumentException(String.format(
						"Protein mapping property key [%s] is invalid - " +
						"it should contain exactly three tokens separated " +
						"by a pipe (\"|\") character.", protein));
				// record full protein identifier
				REFERENCE_PROTEINS.add(protein);
				// map by accession
				REFERENCE_PROTEINS_BY_ACCESSION.put(tokens[1], protein);
				// map by concatenation of database identifier and accession
				REFERENCE_PROTEINS_BY_ID_PLUS_ACCESSION.put(
					String.format("%s|%s", tokens[0], tokens[1]), protein);
				// map by name
				REFERENCE_PROTEINS_BY_NAME.put(tokens[2], protein);
				// map by concatenation of accession and name
				REFERENCE_PROTEINS_BY_ACCESSION_PLUS_NAME.put(
					String.format("%s|%s", tokens[1], tokens[2]), protein);
				// map by description
				REFERENCE_PROTEINS_BY_NAME.put(
					referenceProteins.getProperty(protein), protein);
			}
		} catch (IOException error) {
			throw new RuntimeException(error);
		}
	}
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public static boolean isReferenceProtein(String identifier) {
		if (identifier == null)
			return false;
		else return REFERENCE_PROTEINS.contains(identifier);
	}
	
	public static String getReferenceProteinByAccession(String accession) {
		if (accession == null)
			return null;
		else return REFERENCE_PROTEINS_BY_ACCESSION.get(accession);
	}
	
	public static String getReferenceProteinByIDPlusAccession(
		String identifier
	) {
		if (identifier == null)
			return null;
		else return REFERENCE_PROTEINS_BY_ID_PLUS_ACCESSION.get(identifier);
	}
	
	public static String getReferenceProteinByName(String name) {
		if (name == null)
			return null;
		else return REFERENCE_PROTEINS_BY_NAME.get(name);
	}
	
	public static String getReferenceProteinByDescription(String description) {
		if (description == null)
			return null;
		else return REFERENCE_PROTEINS_BY_DESCRIPTION.get(description);
	}
	
	public static String getReferenceProtein(String fragment) {
		if (fragment == null)
			return null;
		// first check to see if the argument protein is in fact
		// already a valid full reference protein identifier
		else if (isReferenceProtein(fragment))
			return fragment;
		// try accession
		String protein = getReferenceProteinByAccession(fragment);
		if (protein != null)
			return protein;
		// try database id and accession
		protein = getReferenceProteinByIDPlusAccession(fragment);
		if (protein != null)
			return protein;
		// try name
		protein = getReferenceProteinByName(fragment);
		if (protein != null)
			return protein;
		// try description
		protein = getReferenceProteinByDescription(fragment);
		if (protein != null)
			return protein;
		// if all else fails, this is not a discernible fragment
		// of any reference protein, so return null
		return null;
	}
	
	public static Collection<String> getReferenceProteins(String fragment) {
		if (fragment == null)
			return null;
		// get all reference proteins for which this fragment is a substring
		Collection<String> matches =
			REFERENCE_PROTEINS_BY_SUBSTRING.get(fragment);
		// if this fragment has not been searched before, do so now
		if (matches == null) {
			// perform case-insensitive substring comparison
			String normalizedFragment = fragment.toUpperCase();
			matches = new LinkedHashSet<String>();
			// TODO: use more efficient algorith for substring comparison
			for (String protein : REFERENCE_PROTEINS)
				if (protein.toUpperCase().contains(normalizedFragment))
					matches.add(protein);
			// cache these matches to speed up future lookups
			REFERENCE_PROTEINS_BY_SUBSTRING.put(fragment, matches);
		}
		if (matches.isEmpty())
			return null;
		else return matches;
	}
}
