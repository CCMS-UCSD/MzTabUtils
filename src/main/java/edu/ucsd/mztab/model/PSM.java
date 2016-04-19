package edu.ucsd.mztab.model;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.regex.Matcher;

import org.apache.commons.lang3.StringEscapeUtils;

public class PSM
{
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private Integer                  id;
	private Integer                  msRun;
	private String                   nativeID;
	private String                   sequence;
	private Integer                  charge;
	private Double                   massToCharge;
	private Collection<Modification> modifications;
	
	/*========================================================================
	 * Constructor
	 *========================================================================*/
	public PSM(
		String id, String spectraRef, String sequence, String charge,
		String massToCharge, Collection<Modification> modifications
	) {
		// validate PSM_ID string
		if (id == null)
			throw new NullPointerException(
				"Argument \"PSM_ID\" string cannot be null.");
		else try {
			this.id = Integer.parseInt(id);
		} catch (NumberFormatException error) {
			throw new IllegalArgumentException(String.format(
				"Invalid \"PSM_ID\" string [%s]: \"PSM_ID\" must " +
				"be an integer greater than or equal to 1.", id));
		}
		// validate spectra_ref string
		if (spectraRef == null || spectraRef.trim().equalsIgnoreCase("null"))
			throw new NullPointerException(
				"Argument \"spectra_ref\" string cannot be null.");
		Matcher matcher =
			MzTabConstants.SPECTRA_REF_PATTERN.matcher(spectraRef);
		if (matcher.matches() == false)
			throw new IllegalArgumentException(String.format(
				"Invalid \"spectra_ref\" string [%s]: this value is " +
				"expected to conform to string format [%s].", spectraRef,
				"ms_run[1-n]:<nativeID-formatted identifier string>"));
		// extract and validate ms_run index
		try {
			msRun = Integer.parseInt(matcher.group(1));
		} catch (NumberFormatException error) {
			throw new IllegalArgumentException(String.format(
				"Invalid \"spectra_ref\" string [%s]: \"ms_run\" index " +
				"must be an integer greater than or equal to 1.", spectraRef));
		}
		if (msRun <= 0)
			throw new IllegalArgumentException(String.format(
				"Invalid \"spectra_ref\" string [%s]: " +
				"ms_run index %d is invalid; ms_run indices should start at 1.",
				spectraRef, msRun));
		// extract nativeID
		nativeID = matcher.group(2);
		// validate sequence
		if (sequence == null || sequence.trim().equalsIgnoreCase("null"))
			throw new NullPointerException(
				"Argument peptide sequence string cannot be null.");
		else this.sequence = sequence;
		// validate charge
		if (charge == null || charge.trim().equalsIgnoreCase("null"))
			throw new NullPointerException(
				"Argument charge value cannot be null.");
		else try {
			this.charge = Integer.parseInt(charge);
			if (this.charge < 1)
				throw new IllegalStateException();
		} catch (Throwable error) {
			throw new IllegalArgumentException(String.format(
				"Invalid charge value [%s]: charge must be a positive integer.",
				charge));
		}
		// validate mass-to-charge; can be null
		if (massToCharge == null ||
			massToCharge.trim().equalsIgnoreCase("null"))
			this.massToCharge = null;
		else try {
			this.massToCharge = Double.parseDouble(massToCharge);
		} catch (NumberFormatException error) {
			throw new IllegalArgumentException(String.format(
				"Invalid mass-to-charge ratio [%s]: " +
				"mass-to-charge must be a real number.", charge));
		}
		// extract modifications from argument string
		this.modifications = new LinkedHashSet<Modification>();
		// it's ok for the modifications column to contain a "null" value
		if (modifications == null)
			this.modifications = new LinkedHashSet<Modification>();
		else this.modifications = modifications;
	}
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	@Override
	public String toString() {
		// build modification string
		StringBuilder mods = new StringBuilder("[");
		for (Modification modification : modifications) {
			mods.append("\"");
			mods.append(StringEscapeUtils.escapeJson(modification.toString()));
			mods.append("\",");
		}
		// chomp trailing comma
		if (mods.charAt(mods.length() - 1) == ',')
			mods.setLength(mods.length() - 1);
		mods.append("]");
		// return complete JSON string
		return String.format(
			"{id:%d,spectra_ref:\"ms_run[%d]:%s\",sequence:\"%s\"," +
			"charge:%d,exp_mass_to_charge:\"%s\",modifications:%s}",
			id, msRun, nativeID, sequence, charge,
			massToCharge == null ? "null" : Double.toString(massToCharge),
			mods.toString());
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof PSM == false)
			return false;
		else return toString().equals(obj.toString());
	}
	
	@Override
	public int hashCode() {
		return toString().hashCode();
	}
	
	/*========================================================================
	 * Property accessor methods
	 *========================================================================*/
	public Integer getID() {
		return id;
	}
	
	public Integer getMsRun() {
		return msRun;
	}
	
	public String getNativeID() {
		return nativeID;
	}
	
	public String getSequence() {
		return sequence;
	}
	
	public String getModifiedSequence() {
		String modifiedSequence = getSequence();
		for (Modification modification : modifications)
			modifiedSequence = modification.addToPeptide(modifiedSequence);
		return modifiedSequence;
	}
	
	public Integer getCharge() {
		return charge;
	}
	
	public Double getMassToCharge() {
		return massToCharge;
	}
}
