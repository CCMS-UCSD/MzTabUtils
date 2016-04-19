package edu.ucsd.mztab;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import edu.ucsd.mztab.model.MzTabFile;
import edu.ucsd.mztab.model.MzTabProcessor;

public class MzTabReader
{
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private MzTabFile            mzTabFile;
	private List<MzTabProcessor> processors;
	
	/*========================================================================
	 * Constructor
	 *========================================================================*/
	public MzTabReader(File file) {
		// validate input mzTab file
		if (file == null)
			throw new NullPointerException(
				"Argument mzTab file cannot be null.");
		else if (file.isFile() == false || file.canRead() == false)
			throw new NullPointerException(
				"Argument mzTab file must be a valid readable file.");
		else mzTabFile = new MzTabFile(file);
		// initialize processor chain
		processors = new ArrayList<MzTabProcessor>();
	}
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public void read() {
		// set up all processors
		for (MzTabProcessor processor : processors)
			processor.setUp(mzTabFile);
		// read through mzTab file line by line, processing each accordingly
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(mzTabFile.getFile()));
			String line = null;
			int lineNumber = 0;
			while (true) {
				line = reader.readLine();
				if (line == null)
					break;
				lineNumber++;
				for (MzTabProcessor processor : processors)
					line = processor.processMzTabLine(line, lineNumber);
			}
		} catch (RuntimeException error) {
			throw error;
		} catch (Throwable error) {
			throw new RuntimeException(error);
		} finally {
			try { reader.close(); }
			catch (Throwable error) {}
		}
		// tear down all processors
		for (MzTabProcessor processor : processors)
			processor.tearDown();
	}
	
	/*========================================================================
	 * Property accessor methods
	 *========================================================================*/
	public void addProcessor(MzTabProcessor processor) {
		if (processor != null)
			processors.add(processor);
	}
}
