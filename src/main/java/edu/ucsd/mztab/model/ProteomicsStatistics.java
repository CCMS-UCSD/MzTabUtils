package edu.ucsd.mztab.model;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class ProteomicsStatistics
{
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private int         psms;
	private Set<String> peptides;
	private Set<String> variants;
	private Set<String> proteins;
	
	/*========================================================================
	 * Constructor
	 *========================================================================*/
	public ProteomicsStatistics() {
		// initialize all properties
		psms = 0;
		peptides = new HashSet<String>();
		variants = new HashSet<String>();
		proteins = new HashSet<String>();
	}
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public int getPSMs() {
		return psms;
	}
	
	public void addPSMs(int psms) {
		if (psms < 1)
			return;
		else this.psms += psms;
	}
	
	public int getPeptides() {
		return peptides.size();
	}
	
	public void addPeptide(String peptide) {
		if (peptide != null)
			this.peptides.add(peptide);
	}
	
	public void addPeptides(Collection<String> peptides) {
		if (peptides != null)
			this.peptides.addAll(peptides);
	}
	
	public int getVariants() {
		return variants.size();
	}
	
	public void addVariant(String variant) {
		if (variant != null)
			this.variants.add(variant);
	}
	
	public void addVariants(Collection<String> variants) {
		if (variants != null)
			this.variants.addAll(variants);
	}
	
	public int getProteins() {
		return proteins.size();
	}
	
	public void addProtein(String protein) {
		if (protein != null)
			this.proteins.add(protein);
	}
	
	public void addProteins(Collection<String> proteins) {
		if (proteins != null)
			this.proteins.addAll(proteins);
	}
}
