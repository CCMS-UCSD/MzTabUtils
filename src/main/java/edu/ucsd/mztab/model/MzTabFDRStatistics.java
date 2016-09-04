package edu.ucsd.mztab.model;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutablePair;

import edu.ucsd.mztab.model.MzTabConstants.FDRType;

public class MzTabFDRStatistics
{
	/*========================================================================
	 * Properties
	 *========================================================================*/
	// element type ("targetXYZ"/"decoyXYZ") -> set of distinct elements
	private Map<String, Set<String>>                     elements;
	// protein accession -> matched peptides
	private Map<String, Set<String>>                     proteinPeptides;
	// identifier -> passThreshold/isDecoy
	private Map<String, ImmutablePair<Boolean, Boolean>> peptides;
	private Map<String, ImmutablePair<Boolean, Boolean>> proteins;
	// highest found Q-values
	private Map<FDRType, Double>                         maxQValues;
	
	/*========================================================================
	 * Constructor
	 *========================================================================*/
	public MzTabFDRStatistics() {
		// initialize all properties
		elements = new HashMap<String, Set<String>>();
		proteinPeptides = new HashMap<String, Set<String>>();
		peptides = new HashMap<String, ImmutablePair<Boolean, Boolean>>();
		proteins = new HashMap<String, ImmutablePair<Boolean, Boolean>>();
		maxQValues = new HashMap<FDRType, Double>();
	}
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public void addElement(String type, String identifier) {
		if (type == null || identifier == null)
			return;
		Set<String> theseElements = elements.get(type);
		if (theseElements == null)
			theseElements = new HashSet<String>();
		theseElements.add(identifier);
		elements.put(type, theseElements);
	}
	
	public void removeElement(String type, String identifier) {
		if (type == null || identifier == null)
			return;
		Set<String> theseElements = elements.get(type);
		if (theseElements == null)
			return;
		else theseElements.remove(identifier);
	}
	
	public boolean containsElement(String type, String identifier) {
		if (type == null || identifier == null)
			return false;
		Set<String> theseElements = elements.get(type);
		if (theseElements == null)
			return false;
		else return theseElements.contains(identifier);
	}
	
	public Set<String> getElements(String type) {
		if (type == null)
			return null;
		else return new HashSet<String>(elements.get(type));
	}
	
	public int getElementCount(String type) {
		if (type == null)
			return 0;
		Set<String> theseElements = elements.get(type);
		if (theseElements == null)
			return 0;
		else return theseElements.size();
	}
	
	public void addPeptide(
		String sequence, Boolean passThreshold, Boolean isDecoy
	) {
		addFDRAttributes(true, sequence, passThreshold, isDecoy);
	}
	
	public Set<String> getPeptides() {
		return new HashSet<String>(peptides.keySet());
	}
	
	public ImmutablePair<Boolean, Boolean> getPeptide(String sequence) {
		if (sequence == null)
			return null;
		else return peptides.get(sequence);
	}
	
	public void removePeptide(String sequence) {
		if (sequence == null)
			return;
		else peptides.remove(sequence);
	}
	
	public boolean containsPeptide(String sequence) {
		if (sequence == null)
			return false;
		else return peptides.containsKey(sequence);
	}
	
	public void addProtein(
		String accession, Boolean passThreshold, Boolean isDecoy
	) {
		addFDRAttributes(false, accession, passThreshold, isDecoy);
	}
	
	public Set<String> getProteins() {
		return new HashSet<String>(proteins.keySet());
	}
	
	public ImmutablePair<Boolean, Boolean> getProtein(String accession) {
		if (accession == null)
			return null;
		else return proteins.get(accession);
	}
	
	public void removeProtein(String accession) {
		if (accession == null)
			return;
		else proteins.remove(accession);
	}
	
	public boolean containsProtein(String accession) {
		if (accession == null)
			return false;
		else return proteins.containsKey(accession);
	}
	
	public void addProteinPeptide(String accession, String sequence) {
		if (accession == null || sequence == null)
			return;
		Set<String> peptides = proteinPeptides.get(accession);
		if (peptides == null)
			peptides = new HashSet<String>();
		peptides.add(sequence);
		proteinPeptides.put(accession, peptides);
	}
	
	public Set<String> getPeptideMappedProteins() {
		return new HashSet<String>(proteinPeptides.keySet());
	}
	
	public Set<String> getProteinPeptides(String accession) {
		if (accession == null)
			return null;
		else return proteinPeptides.get(accession);
	}
	
	public void recordQValue(FDRType type, Double qValue) {
		if (type == null || qValue == null)
			return;
		Double currentMaxQValue = maxQValues.get(type);
		if (currentMaxQValue == null || currentMaxQValue < qValue)
			maxQValues.put(type, qValue);
	}
	
	public Double getMaxQValue(FDRType type) {
		if (type == null)
			return null;
		else return maxQValues.get(type);
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private void addFDRAttributes(
		boolean type, String identifier, Boolean passThreshold, Boolean isDecoy
	) {
		if (identifier == null)
			return;
		Map<String, ImmutablePair<Boolean, Boolean>> map = null;
		if (type)
			map = peptides;
		else map = proteins;
		ImmutablePair<Boolean, Boolean> attributes = map.get(identifier);
		if (attributes != null) {
			Boolean savedPassThreshold = attributes.getLeft();
			Boolean savedIsDecoy = attributes.getRight();
			// overwrite this passThreshold with the saved one only if this
			// one is null, or if the saved one is true; consequently:
			// 1. null is always overwritten, so passThreshold will end up
			//    null only if every child of this element has a value of null
			// 2. false overwrites null, so if there is one false among
			//    many nulls, the element will NOT pass threshold
			// 3. true overwrites everything, so if there is even
			//    one true, the element WILL pass threshold
			if (passThreshold == null ||
				(savedPassThreshold != null && savedPassThreshold == true))
				passThreshold = savedPassThreshold;
			// overwrite this isDecoy with the saved one only if this
			// one is null, or if the saved one is false; consequently:
			// 1. null is always overwritten, so isDecoy will end up null
			//    only if every child of this element has a value of null
			// 2. true overwrites null, so if there is one true among
			//    many nulls, the element will be a decoy
			// 3. false overwrites everything, so if there is even
			//    one false, the element will be a target
			if (isDecoy == null ||
				(savedIsDecoy != null && savedIsDecoy == false))
				isDecoy = savedIsDecoy;
		}
		// save these attributes for this identifier
		attributes =
			new ImmutablePair<Boolean, Boolean>(passThreshold, isDecoy);
		map.put(identifier, attributes);
	}
}
