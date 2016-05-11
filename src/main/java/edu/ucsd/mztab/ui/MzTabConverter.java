package edu.ucsd.mztab.ui;

import java.io.File;

import edu.ucsd.mztab.converters.MzIdToMzTabConverter;
import edu.ucsd.mztab.converters.PRIDEXMLToMzTabConverter;

public class MzTabConverter
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String USAGE =
		"java -cp MzTabUtils.jar edu.ucsd.mztab.ui.MzTabConverter" +
		"\n\t-input  <InputFile>" +
		"\n\t-output <OutputMzTabFile>" +
		"\n\t-format MZIDENTML|PRIDEXML";
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public static void main(String[] args) {
		MzTabConvertOperation convert = extractArguments(args);
		if (convert == null)
			die(USAGE);
		// dispatch conversion based on argument format
		try {
			if (convert.format.equals(
				MzTabConvertOperation.MzTabConversionFormat.MZIDENTML))
				MzIdToMzTabConverter.convert(
					convert.inputFile, convert.mzTabFile);
			else if (convert.format.equals(
				MzTabConvertOperation.MzTabConversionFormat.PRIDEXML))
				PRIDEXMLToMzTabConverter.convert(
					convert.inputFile, convert.mzTabFile);
		} catch (Throwable error) {
			die(String.format("There was an error converting input " +
				"file [%s] (format \"%s\") to mzTab format.",
				convert.inputFile.getAbsolutePath(),
				convert.format.toString()), error);
		}
	}
	
	/*========================================================================
	 * Convenience classes
	 *========================================================================*/
	/**
	 * Struct to maintain context data for each mzTab conversion operation.
	 */
	private static class MzTabConvertOperation {
		/*====================================================================
		 * Constants
		 *====================================================================*/
		private static enum MzTabConversionFormat { MZIDENTML, PRIDEXML }
		
		/*====================================================================
		 * Properties
		 *====================================================================*/
		private File                  inputFile;
		private File                  mzTabFile;
		private MzTabConversionFormat format;
		
		/*====================================================================
		 * Constructors
		 *====================================================================*/
		public MzTabConvertOperation(
			File inputFile, File mzTabFile, String format
		) {
			// validate input file
			if (inputFile == null)
				throw new NullPointerException(
					"Argument input file cannot be null.");
			else if (inputFile.isFile() == false)
				throw new IllegalArgumentException(String.format(
					"Argument input file [%s] must be a regular file.",
					inputFile.getAbsolutePath()));
			else if (inputFile.canRead() == false)
				throw new IllegalArgumentException(String.format(
					"Argument input file [%s] must be readable.",
					inputFile.getAbsolutePath()));
			else this.inputFile = inputFile;
			// validate output mzTab file
			if (mzTabFile == null)
				throw new NullPointerException(
					"Argument mzTab output file cannot be null.");
			else if (mzTabFile.exists() && mzTabFile.isFile() == false)
				throw new IllegalArgumentException(String.format(
					"Argument mzTab output file [%s] must be a regular file.",
					mzTabFile.getAbsolutePath()));
			else this.mzTabFile = mzTabFile;
			// validate input file format
			if (format == null)
				throw new NullPointerException(
					"MzTab conversion source file format cannot be null.");
			else this.format =
				MzTabConversionFormat.valueOf(format.toUpperCase());
		}
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private static MzTabConvertOperation extractArguments(String[] args) {
		if (args == null || args.length < 1)
			return null;
		File inputFile = null;
		File mzTabFile = null;
		String format = null;
		for (int i=0; i<args.length; i++) {
			String argument = args[i];
			if (argument == null)
				return null;
			else {
				i++;
				if (i >= args.length)
					return null;
				String value = args[i];
				if (argument.equals("-input"))
					inputFile = new File(value);
				else if (argument.equals("-output"))
					mzTabFile = new File(value);
				else if (argument.equals("-format"))
					format = value;
				else return null;
			}
		}
		try {
			return new MzTabConvertOperation(inputFile, mzTabFile, format);
		} catch (Throwable error) {
			System.err.println(error.getMessage());
			return null;
		}
	}
	
	private static void die(String message) {
		die(message, null);
	}
	
	private static void die(String message, Throwable error) {
		if (message == null)
			message = "There was an error converting an " +
				"input result file to mzTab format";
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
