package edu.ucsd.mztab.ui;

import java.io.File;
import java.util.Arrays;

import edu.ucsd.mztab.MzTabReader;
import edu.ucsd.mztab.model.MzTabFile;
import edu.ucsd.mztab.processors.ValidityProcessor;

public class MzTabPreprocessor
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String USAGE =
		"java -cp MzTabUtils.jar edu.ucsd.mztab.ui.MzTabPreprocessor" +
		"\n\t-mztab  <MzTabDirectory>" +
		"\n\t-output <ValidatedMzTabDirectory>";
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public static void main(String[] args) {
		MzTabPreprocessingOperation preprocessing = extractArguments(args);
		if (preprocessing == null)
			die(USAGE);
		// get all mzTab files in argument directory
		File[] files = preprocessing.mzTabDirectory.listFiles();
		if (files == null || files.length < 1)
			return;
		// sort files alphabetically
		Arrays.sort(files);
		// pre-process all mzTab files
		for (File file : files) {
			// get this mzTab file
			MzTabFile mzTabFile = new MzTabFile(file);
			// set up output file
			File outputFile =
				new File(preprocessing.outputDirectory, file.getName());
			// set up mzTab reader
			MzTabReader reader = new MzTabReader(mzTabFile, outputFile);
			// ensure this file contains the proper validity columns
			reader.addProcessor(new ValidityProcessor());
			// pre-process this file
			reader.read();
		}
	}
	
	/*========================================================================
	 * Convenience classes
	 *========================================================================*/
	/**
	 * Struct to maintain context data for each mzTab pre-processing operation.
	 */
	private static class MzTabPreprocessingOperation {
		/*====================================================================
		 * Properties
		 *====================================================================*/
		private File mzTabDirectory;
		private File outputDirectory;
		
		/*====================================================================
		 * Constructors
		 *====================================================================*/
		public MzTabPreprocessingOperation(
			File mzTabDirectory, File outputDirectory
		) {
			// validate mzTab directory
			if (mzTabDirectory.isDirectory() == false)
				throw new IllegalArgumentException(String.format(
					"MzTab directory [%s] must be a directory.",
					mzTabDirectory.getAbsolutePath()));
			else if (mzTabDirectory.canRead() == false)
				throw new IllegalArgumentException(String.format(
					"MzTab directory [%s] must be readable.",
					mzTabDirectory.getAbsolutePath()));
			else this.mzTabDirectory = mzTabDirectory;
			// validate processed mzTab output directory
			if (outputDirectory == null)
				throw new NullPointerException(
					"Output directory cannot be null.");
			else if (outputDirectory.isDirectory() == false)
				throw new IllegalArgumentException(
					String.format("Output directory [%s] must be a directory.",
						outputDirectory.getAbsolutePath()));
			else if (outputDirectory.canWrite() == false)
				throw new IllegalArgumentException(
					String.format("Output directory [%s] must be writable.",
						outputDirectory.getAbsolutePath()));
			else this.outputDirectory = outputDirectory;
		}
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private static MzTabPreprocessingOperation extractArguments(String[] args) {
		if (args == null || args.length < 1)
			return null;
		File mzTabDirectory = null;
		File outputDirectory = null;
		for (int i=0; i<args.length; i++) {
			String argument = args[i];
			if (argument == null)
				return null;
			else {
				i++;
				if (i >= args.length)
					return null;
				String value = args[i];
				if (argument.equals("-mztab"))
					mzTabDirectory = new File(value);
				else if (argument.equals("-output"))
					outputDirectory = new File(value);
				else return null;
			}
		}
		try {
			return new MzTabPreprocessingOperation(
				mzTabDirectory, outputDirectory);
		} catch (Throwable error) {
			die("There was an error reading command line parameters " +
				"to set up mzTab pre-processing operation.", error);
			return null;
		}
	}
	
	private static void die(String message) {
		die(message, null);
	}
	
	private static void die(String message, Throwable error) {
		if (message == null)
			message = "There was an error pre-processing mzTab files.";
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
