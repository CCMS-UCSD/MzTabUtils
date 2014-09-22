package edu.ucsd.util;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.ImmutablePair;

public class OntologyUtils
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final Pattern ONTOLOGY_PTM_PATTERN = Pattern.compile(
		"^\\[(.*)\\]\\s*\\[(.*)\\]\\s*(.*)\\|.*$");
	public static final Properties ONTOLOGY_MODIFICATIONS = new Properties();
	static {
		try {
			ONTOLOGY_MODIFICATIONS.load(
				OntologyUtils.class.getResourceAsStream(
					"ontology_modifications.properties"));
		} catch (IOException error) {
			throw new RuntimeException(error);
		}
	}
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public static ImmutablePair<Double, String> getOntologyModification(
		String accession
	) {
		if (accession == null)
			return null;
		String modDescriptor = ONTOLOGY_MODIFICATIONS.getProperty(accession);
		if (modDescriptor == null)
			return null;
		// mod entries that do not have a parseable
		// name and mass should return null
		Matcher matcher = ONTOLOGY_PTM_PATTERN.matcher(modDescriptor);
		if (matcher.matches() == false)
			return null;
		else try {
			return new ImmutablePair<Double, String>(
				Double.parseDouble(matcher.group(2)), matcher.group(3));
		} catch (NumberFormatException error) {
			// mod entries that do not have a double-formatted
			// parseable mass should return null
			return null;
		}
	}
	
	public static Double getOntologyModificationMass(String accession) {
		if (accession == null)
			return null;
		String modDescriptor = ONTOLOGY_MODIFICATIONS.getProperty(accession);
		if (modDescriptor == null)
			return null;
		// mod entries that do not have a parseable mass should return null
		Matcher matcher = ONTOLOGY_PTM_PATTERN.matcher(modDescriptor);
		if (matcher.matches() == false)
			return null;
		else try {
			return Double.parseDouble(matcher.group(2));
		} catch (NumberFormatException error) {
			return null;
		}
	}
	
	public static String getOntologyModificationName(String accession) {
		if (accession == null)
			return null;
		String modDescriptor = ONTOLOGY_MODIFICATIONS.getProperty(accession);
		if (modDescriptor == null)
			return null;
		// most mods conform to the usual pattern of [sites] [mass] [name]
		Matcher matcher = ONTOLOGY_PTM_PATTERN.matcher(modDescriptor);
		if (matcher.matches() != false)
			return matcher.group(3);
		// however, there are a couple of ontology entries that do not conform
		// to this pattern, e.g. "No PTMs are included in the dataset" and
		// "unknown modification"
		String[] tokens = modDescriptor.split("\\|");
		if (tokens == null || tokens.length < 1)
			return null;
		else return tokens[0];
	}
	
	public static Collection<String> getOntologyModificationSites(
		String accession
	) {
		if (accession == null)
			return null;
		String modDescriptor = ONTOLOGY_MODIFICATIONS.getProperty(accession);
		if (modDescriptor == null)
			return null;
		Matcher matcher = ONTOLOGY_PTM_PATTERN.matcher(modDescriptor);
		if (matcher.matches() == false)
			return null;
		else {
			String[] tokens = matcher.group(1).split(",");
			if (tokens == null || tokens.length < 1)
				return null;
			Collection<String> sites = new LinkedHashSet<String>();
			for (int i=0; i<tokens.length; i++) {
				String site = tokens[i].trim();
				if (site.isEmpty() == false)
					sites.add(site);
			}
			if (sites == null || sites.isEmpty())
				return null;
			else return sites;
		}
	}
}
