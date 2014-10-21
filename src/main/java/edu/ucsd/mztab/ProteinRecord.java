package edu.ucsd.mztab;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import uk.ac.ebi.pride.jmztab.model.Modification;

public class ProteinRecord
{
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private String                    accession;
	private Map<Integer, Integer>     psmCounts;
	private Map<Integer, Set<String>> peptides;
	private Set<Modification>         modifications;
	
	/*========================================================================
	 * Constructor
	 *========================================================================*/
	public ProteinRecord(String accession) {
		if (accession == null)
			throw new NullPointerException("Protein accession cannot be null.");
		else this.accession = accession;
	}
	
	/*========================================================================
	 * Property accessor methods
	 *========================================================================*/
	public String getAccession() {
		return accession;
	}
	
	public int getPSMCount(int msRun) {
		if (psmCounts == null)
			return 0;
		Integer psmCount = psmCounts.get(msRun);
		if (psmCount == null)
			return 0;
		else return psmCount;
	}
	
	public int getTotalPSMCount() {
		if (psmCounts == null)
			return 0;
		int psmCount = 0;
		for (int msRun : psmCounts.keySet())
			psmCount += getPSMCount(msRun);
		return psmCount;
	}
	
	public void addPSM(int msRun) {
		if (psmCounts == null)
			psmCounts = new LinkedHashMap<Integer, Integer>();
		Integer psmCount = psmCounts.get(msRun);
		if (psmCount == null)
			psmCounts.put(msRun, 1);
		else psmCounts.put(msRun, psmCount + 1);
	}
	
	public Set<String> getPeptides(int msRun) {
		if (peptides == null)
			return null;
		else return peptides.get(msRun);
	}
	
	public Set<String> getAllPeptides() {
		if (peptides == null)
			return null;
		Set<String> allPeptides = new LinkedHashSet<String>();
		for (int msRun : peptides.keySet()) {
			Set<String> thesePeptides = peptides.get(msRun);
			if (thesePeptides != null)
				allPeptides.addAll(thesePeptides);
		}
		if (allPeptides.isEmpty())
			return null;
		else return allPeptides;
	}
	
	public void addPeptide(int msRun, String peptide) {
		if (peptide == null)
			return;
		else if (peptides == null)
			peptides = new LinkedHashMap<Integer, Set<String>>();
		Set<String> thesePeptides = peptides.get(msRun);
		if (thesePeptides == null)
			thesePeptides = new LinkedHashSet<String>();
		thesePeptides.add(peptide);
		peptides.put(msRun, thesePeptides);
	}
	
	public int getPeptideCount(int msRun) {
		if (peptides == null)
			return 0;
		Set<String> thesePeptides = peptides.get(msRun);
		if (thesePeptides == null)
			return 0;
		else return thesePeptides.size();
	}
	
	public int getTotalPeptideCount() {
		if (peptides == null)
			return 0;
		int peptideCount = 0;
		for (int msRun : peptides.keySet())
			peptideCount += getPeptideCount(msRun);
		return peptideCount;
	}
	
	public Set<Modification> getModifications() {
		return modifications;
	}
	
	public void addModification(Modification modification) {
		if (modification == null)
			return;
		else if (modifications == null)
			modifications = new LinkedHashSet<Modification>();
		modifications.add(modification);
	}
}
