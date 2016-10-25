package edu.ucsd.mztab.model;

import java.util.Collection;

import uk.ac.ebi.pride.jmztab.model.Modification;

public class ModificationParse
{
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private Collection<Modification> modificationOccurrences;
	private String                   parsedPeptide;
	private String                   error;
	
	/*========================================================================
	 * Constructors
	 *========================================================================*/
	public ModificationParse(
		Collection<Modification> modificationOccurrences, String parsedPeptide
	) {
		this(modificationOccurrences, parsedPeptide, null);
	}
	
	public ModificationParse(
		Collection<Modification> modificationOccurrences,
		String parsedPeptide, String error
	) {
		setModificationOccurrences(modificationOccurrences);
		setParsedPeptide(parsedPeptide);
		setError(error);
	}
	
	/*========================================================================
	 * Property accessor methods
	 *========================================================================*/
	public Collection<Modification> getModificationOccurrences() {
		return modificationOccurrences;
	}
	
	public void setModificationOccurrences(
			Collection<Modification> modificationOccurrences) {
		this.modificationOccurrences = modificationOccurrences;
	}
	
	public String getParsedPeptide() {
		return parsedPeptide;
	}
	
	public void setParsedPeptide(String parsedPeptide) {
		this.parsedPeptide = parsedPeptide;
	}
	
	public String getError() {
		return error;
	}
	
	public void setError(String error) {
		this.error = error;
	}
	
	public boolean hasError() {
		return error != null;
	}
}
