package edu.ucsd.mztab.converters.impl;

import java.util.List;

import uk.ac.ebi.pride.jmztab.model.MZTabColumnFactory;
import uk.ac.ebi.pride.jmztab.model.PSM;
import uk.ac.ebi.pride.utilities.data.controller.impl.ControllerImpl.MzIdentMLControllerImpl;
import uk.ac.ebi.pride.utilities.data.core.Peptide;
import uk.ac.ebi.pride.utilities.data.core.Protein;
import uk.ac.ebi.pride.utilities.data.core.SpectrumIdentification;
import uk.ac.ebi.pride.utilities.data.exporters.MzIdentMLMzTabConverter;

public class CCMSMzIdentMLMzTabConverter
extends MzIdentMLMzTabConverter
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String PASS_THRESHOLD_COLUMN = "pass_threshold";
	
	/*========================================================================
	 * Constructor
	 *========================================================================*/
	public CCMSMzIdentMLMzTabConverter(MzIdentMLControllerImpl controller) {
		super(controller);
	}
	
	/*========================================================================
	 * ConvertProvider methods
	 *========================================================================*/
	@Override
    protected MZTabColumnFactory convertPSMColumnFactory() {
		psmColumnFactory = super.convertPSMColumnFactory();
		// add "passThreshold" column
		psmColumnFactory.addOptionalColumn(PASS_THRESHOLD_COLUMN, String.class);
		return psmColumnFactory;
	}
	
	@Override
	protected List<PSM> loadPSMs(Protein protein, List<Peptide> peptides)  {
		List<PSM> psmList = super.loadPSMs(protein, peptides);
		// iterate over all PSMs, propate "passThreshold" attribute
		for (PSM psm : psmList)
			psm.setOptionColumnValue(
				PASS_THRESHOLD_COLUMN, passesThreshold(psm, peptides));
		return psmList;
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private Boolean passesThreshold(PSM psm, List<Peptide> peptides) {
		if (psm == null || peptides == null)
			return null;
		for (Peptide peptide : peptides) {
			SpectrumIdentification match = peptide.getSpectrumIdentification();
			if (match != null &&
				match.getId().toString().equals(psm.getPSM_ID()))
				return match.isPassThreshold();
		}
		return null;
	}
}
