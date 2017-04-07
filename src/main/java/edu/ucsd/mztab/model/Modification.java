package edu.ucsd.mztab.model;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.regex.Matcher;

import edu.ucsd.mztab.util.OntologyUtils;
import edu.ucsd.mztab.util.ProteomicsUtils;

public class Modification
{
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private String              name;
	private Double              mass;
	private Collection<Integer> positions;
	
	/*========================================================================
	 * Constructors
	 *========================================================================*/
	public Modification(String name) {
		this(name, null);
	}
	
	public Modification(String name, String positions) {
		// validate input modification name
		if (name == null)
			throw new NullPointerException(
				"Argument modification name cannot be null.");
		else this.name = name;
		// determine mass from modification identifier
		mass = getMass(name);
		// determine positions from positions string
		this.positions = getPositions(positions);
	}
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public String addToPeptide(String peptide) {
		return addToPeptide(peptide, getPosition());
	}
	
	public String addToPeptide(String peptide, Integer position) {
		if (peptide == null)
			return null;
		else if (mass == null || position == null)
			return peptide;
		else return ProteomicsUtils.addModToPeptide(peptide, mass, position);
	}
	
	public String toString() {
		if (positions.isEmpty())
			return name;
		else {
			StringBuilder prefix = new StringBuilder();
			for (Integer position : positions)
				if (position != null)
					prefix.append(position).append("|");
			// chomp trailing pipe ("|")
			if (prefix.charAt(prefix.length() - 1) == '|')
				prefix.setLength(prefix.length() - 1);
			return String.format("%s-%s", prefix, name);
		}
	}
	
	/*========================================================================
	 * Property accessor methods
	 *========================================================================*/
	public String getName() {
		return name;
	}
	
	public Double getMass() {
		return mass;
	}
	
	public String getFormattedMass() {
		return ProteomicsUtils.formatMass(mass);
	}
	
	public Collection<Integer> getPositions() {
		return new LinkedHashSet<Integer>(positions);
	}
	
	public Integer getPosition() {
		if (positions.size() != 1)
			return null;
		else return positions.iterator().next();
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private Double getMass(String identifier) {
		if (identifier == null)
			return null;
		else identifier = identifier.trim();
		// if this is a CHEMMOD, get the mass from the mod itself
		Matcher matcher =
			MzTabConstants.MZTAB_CHEMMOD_PATTERN.matcher(identifier);
		if (matcher.matches()) try {
			return Double.parseDouble(matcher.group(1));
		} catch (NumberFormatException error) {
			return null;
		}
		// if this is a CV parameter, get the name and value from the tuple
		String name = identifier;
		String value = null;
		matcher = MzTabConstants.CV_TERM_PATTERN.matcher(identifier);
		if (matcher.matches()) {
			name = matcher.group(2);
			value = matcher.group(4);
		}
		// treat the name as a CV accession and
		// try to get it from the PTM ontology
		Double mass = OntologyUtils.getOntologyModificationMass(name);
		// if the term cannot be found in the PTM ontology, try the parsed
		// CV parameter value (if present) as a last-ditch effort
		if (mass == null && value != null) try {
			return Double.parseDouble(value);
		} catch (NumberFormatException error) {
			return null;
		}
		// masses of 0 correspond to unknown mods
		if (mass == null || mass == 0.0)
			return null;
		else return mass;
	}
	
	private Collection<Integer> getPositions(String position) {
		Collection<Integer> positions = new LinkedHashSet<Integer>();
		if (position == null)
			return positions;
		Matcher matcher =
			MzTabConstants.MZTAB_POSITION_PATTERN.matcher(position);
		while (matcher.find()) try {
			positions.add(Integer.parseInt(matcher.group(1)));
		} catch (NumberFormatException error) {}
		return positions;
	}
}
