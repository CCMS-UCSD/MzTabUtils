package edu.ucsd.mztab.ui;

import java.io.File;

public class CheckMzTabFileImportable
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String USAGE =
		"java -cp MzTabUtils.jar edu.ucsd.mztab.ui.CheckMzTabFileImportable " +
		"<MzTabFile>";
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public static void main(String[] args) {
		if (args == null || args.length != 1)
			die(USAGE);
		File mzTabFile = new File(args[0]);
		try {
			if (MzTabPROXIImporter.isImportable(mzTabFile, true))
				System.out.println(String.format(
					"MzTab file [%s] is importable to MassIVE search.",
					mzTabFile.getName()));
			else System.out.println(String.format(
				"Argument [%s] does not correspond to an mzTab file that " +
				"is importable to MassIVE search.", mzTabFile.getName()));
		} catch (Throwable error) {
			die(null, error);
		}
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private static void die(String message) {
		die(message, null);
	}
	
	private static void die(String message, Throwable error) {
		if (message == null)
			message = "There was an error parsing an mzTab file to determine " +
				"whether or not it is importable to MassIVE search";
		if (error == null) {
			if (message.endsWith(".") == false)
				message += ".";
		} else {
			if (message.endsWith("."))
				message = message.substring(0, message.length() - 1);
			if (message.endsWith(":") == false)
				message += ":";
		}
		System.err.println(message);
		if (error != null)
			error.printStackTrace();
		System.exit(1);
	}
}
