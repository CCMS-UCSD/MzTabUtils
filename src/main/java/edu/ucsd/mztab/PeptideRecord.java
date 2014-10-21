package edu.ucsd.mztab;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import uk.ac.ebi.pride.jmztab.model.Modification;

public class PeptideRecord
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String NO_ACCESSION = "#NO_ACCESSION#";
	
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private String                         sequence;
	private Map<String, Set<String>>       accessions;
	private Map<String, Set<Modification>> modifications;
	
	/*========================================================================
	 * Constructor
	 *========================================================================*/
	public PeptideRecord(String sequence) {
		if (sequence == null)
			throw new NullPointerException("Peptide sequence cannot be null.");
		else this.sequence = sequence;
	}
	
	/*========================================================================
	 * Property accessor methods
	 *========================================================================*/
	public String getSequence() {
		return sequence;
	}
	
	public Set<String> getAccessions() {
		if (accessions == null)
			return null;
		else return accessions.keySet();
	}
	
	public Set<String> getSpectraRefs(String accession) {
		if (accessions == null)
			return null;
		else if (accession == null)
			accession = NO_ACCESSION;
		return accessions.get(accession);
	}
	
	public boolean isProteinEvidence(String accession) {
		if (accession == null || accession.equals(NO_ACCESSION) ||
			accessions == null)
			return false;
		else return accessions.keySet().contains(accession);
	}
	
	public void addSpectraRef(String accession, String spectraRef) {
		if (accessions == null)
			accessions = new LinkedHashMap<String, Set<String>>();
		if (accession == null)
			accession = NO_ACCESSION;
		Set<String> spectraRefs = accessions.get(accession);
		if (spectraRefs == null)
			spectraRefs = new LinkedHashSet<String>();
		if (spectraRef != null)
			spectraRefs.add(spectraRef);
		accessions.put(accession, spectraRefs);
	}
	
	public Set<Modification> getModifications(String accession) {
		if (modifications == null)
			return null;
		else if (accession == null)
			accession = NO_ACCESSION;
		return modifications.get(accession);
	}
	
	public void addModification(String accession, Modification modification) {
		if (modifications == null || modification == null)
			modifications = new LinkedHashMap<String, Set<Modification>>();
		if (accession == null)
			accession = NO_ACCESSION;
		Set<Modification> theseModifications = modifications.get(accession);
		if (theseModifications == null)
			theseModifications = new LinkedHashSet<Modification>();
		theseModifications.add(modification);
		modifications.put(accession, theseModifications);
	}
}
