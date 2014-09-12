package edu.ucsd.util;

import java.io.IOException;
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
		"^\\[.*\\]\\s*\\[(.*)\\]\\s*(.*)\\|.*$");
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
				Double.parseDouble(matcher.group(1)), matcher.group(2));
		} catch (NumberFormatException error) {
			// mod entries that do not have a double-formatted
			// parseable mass should return null
			return null;
		}
	}
}
