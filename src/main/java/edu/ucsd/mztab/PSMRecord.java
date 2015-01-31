package edu.ucsd.mztab;

import java.util.Collection;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringEscapeUtils;

public class PSMRecord
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final Pattern MZTAB_MODIFICATION_PATTERN = Pattern.compile(
		"(\\d+.*?(?:\\|\\d+.*?)*)-" +
		"((?:\\[[^,]*,\\s*[^,]*,\\s*\"?[^\"]*\"?,\\s*[^,]*\\])|" +	// CV param
		"(?:[^,]*))");
	private static final String MZTAB_MODIFICATION_STRING_FORMAT =
		"{position}{Parameter}-{Modification or Substitution identifier}" +
		"|{neutral loss}";
	
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private Integer             msRun;
	private String              nativeID;
	private String              sequence;
	private Collection<String>  modifications;
	
	/*========================================================================
	 * Constructor
	 *========================================================================*/
	public PSMRecord(
		int msRun, String nativeID, String sequence, String modifications
	) throws InvalidPSMException {
		if (msRun <= 0)
			throw new InvalidPSMException(String.format(
				"ms_run index %d is invalid: ms_run indices should start at 1.",
				msRun));
		else this.msRun = msRun;
		// validate string arguments
		if (nativeID == null)
			throw new NullPointerException(
				"Argument nativeID string cannot be null.");
		else this.nativeID = nativeID;
		if (sequence == null)
			throw new NullPointerException(
				"Argument peptide sequence string cannot be null.");
		else this.sequence = sequence;
		// validate modification string
		if (modifications == null)
			throw new NullPointerException(
				"Argument modification string cannot be null.");
		else {
			this.modifications = new TreeSet<String>();
			// it's ok for the modifications column to contain a "null" value
			if (modifications.trim().equalsIgnoreCase("null") == false) {
				Matcher matcher =
					MZTAB_MODIFICATION_PATTERN.matcher(modifications);
				if (matcher.find() == false)
					throw new InvalidPSMException(String.format(
						"Argument modification string [%s] is invalid: mzTab " +
						"modification strings should be comma-delimited " +
						"lists of strings each conforming to the following " +
						"format:\n%s", modifications,
						MZTAB_MODIFICATION_STRING_FORMAT));
				else do {
					this.modifications.add(String.format(
						"%s-%s", matcher.group(1), matcher.group(2)));
				} while (matcher.find());
			}
		}
	}
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	@Override
	public String toString() {
		// build modification string
		StringBuffer mods = new StringBuffer("[");
		for (String modification : modifications)
			mods.append("\"").append(StringEscapeUtils.escapeJson(modification))
				.append("\",");
		// chomp trailing comma
		if (mods.charAt(mods.length() - 1) == ',')
			mods.setLength(mods.length() - 1);
		mods.append("]");
		// return complete JSON string
		return String.format(
			"{spectra_ref:\"ms_run[%d]:%s\",sequence:\"%s\",modifications:%s}",
			msRun, nativeID, sequence, mods.toString());
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof PSMRecord == false)
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
	public String getNativeID() {
		return nativeID;
	}
}
