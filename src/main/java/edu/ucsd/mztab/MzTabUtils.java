package edu.ucsd.mztab;

import java.io.File;

public class MzTabUtils
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String USAGE = "java -jar MassIVEUtils.jar" +
		"\n\t<ParameterFile>" +
		"\n\t<MzTabDirectory>" +
		"\n\t<ScansDirectory>";
					
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public static void main(String[] args) {
		if (args == null || args.length < 3)
			die(USAGE);
		MassIVEMzTabContext context = null;
		try {
			context = new MassIVEMzTabContext(
				new File(args[0]), new File(args[1]), new File(args[2]));
			System.out.println(context.toJSON());
		} catch (Throwable error) {
			die(USAGE, error);
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
			message = "There was an error validating " +
				"the mzTab files of a MassIVE dataset";
		if (error == null)
			message += ".";
		else message += ":";
		System.err.println(message);
		if (error != null)
			error.printStackTrace();
		System.exit(1);
	}
}
