package edu.ucsd.mztab.converters;

import java.io.File;
import java.io.FileOutputStream;

import edu.ucsd.mztab.converters.impl.CCMSMzIdentMLMzTabConverter;
import uk.ac.ebi.pride.utilities.data.controller.impl.ControllerImpl.MzIdentMLControllerImpl;

public class MzIdToMzTabConverter
{
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public static void convert(File mzIdFile, File mzTabFile) {
		// validate mzIdentML file
		if (mzIdFile == null)
			throw new NullPointerException(
				"Argument mzIdentML file cannot be null.");
		else if (mzIdFile.isFile() == false)
			throw new IllegalArgumentException(String.format(
				"Argument mzIdentML file [%s] must be a regular file.",
				mzIdFile.getAbsolutePath()));
		else if (mzIdFile.canRead() == false)
			throw new IllegalArgumentException(String.format(
				"Argument mzIdentML file [%s] must be readable.",
				mzIdFile.getAbsolutePath()));
		// validate output mzTab file
		if (mzTabFile == null)
			throw new NullPointerException(
				"Argument mzTab output file cannot be null.");
		else if (mzTabFile.exists()) {
			if (mzTabFile.isFile() == false)
				throw new IllegalArgumentException(String.format(
					"Argument mzTab output file [%s] must be a regular file.",
					mzTabFile.getAbsolutePath()));
			else if (mzTabFile.delete() == false)
				throw new RuntimeException(String.format(
					"Argument mzTab output file [%s] could not be deleted " +
					"for subsequent writing.", mzTabFile.getAbsolutePath()));
		}
		// create output mzTab file
		try {
			if (mzTabFile.createNewFile() == false)
				throw new RuntimeException(String.format(
					"Argument mzTab output file [%s] could not be created.",
					mzTabFile.getAbsolutePath()));
		} catch (RuntimeException error) {
			throw error;
		} catch (Throwable error) {
			throw new RuntimeException(error);
		}
		// convert file
		FileOutputStream output = null;
		try {
	    	output = new FileOutputStream(mzTabFile);
	    	new CCMSMzIdentMLMzTabConverter(
	    		new MzIdentMLControllerImpl(mzIdFile))
	    		.getMZTabFile().printMZTab(output);
		} catch (RuntimeException error) {
			throw error;
		} catch (Throwable error) {
			throw new RuntimeException(error);
		} finally {
			try { output.close(); } catch (Throwable error) {}
		}
	}
}
