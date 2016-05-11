package edu.ucsd.mztab.converters;

import java.io.File;
import java.io.FileOutputStream;

import uk.ac.ebi.pride.utilities.data.controller.impl.ControllerImpl.PrideXmlControllerImpl;
import uk.ac.ebi.pride.utilities.data.exporters.PRIDEMzTabConverter;

public class PRIDEXMLToMzTabConverter
{
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public static void convert(File PRIDEXMLFile, File mzTabFile) {
		// validate PRIDE XML file
		if (PRIDEXMLFile == null)
			throw new NullPointerException(
				"Argument PRIDE XML file cannot be null.");
		else if (PRIDEXMLFile.isFile() == false)
			throw new IllegalArgumentException(String.format(
				"Argument PRIDE XML file [%s] must be a regular file.",
				PRIDEXMLFile.getAbsolutePath()));
		else if (PRIDEXMLFile.canRead() == false)
			throw new IllegalArgumentException(String.format(
				"Argument PRIDE XML file [%s] must be readable.",
				PRIDEXMLFile.getAbsolutePath()));
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
	    	new PRIDEMzTabConverter(new PrideXmlControllerImpl(PRIDEXMLFile))
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
