package edu.ucsd.mztab;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
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
	private File                 outputFile;
	
	/*========================================================================
	 * Constructor
	 *========================================================================*/
	public MzTabReader(MzTabFile mzTabFile) {
		this(mzTabFile, null);
	}
	
	public MzTabReader(MzTabFile mzTabFile, File outputFile) {
		// validate input mzTab file
		if (mzTabFile == null)
			throw new NullPointerException(
				"Argument mzTab file cannot be null.");
		else this.mzTabFile = mzTabFile;
		// initialize processor chain
		processors = new ArrayList<MzTabProcessor>();
		// validate output file (can be null)
		if (outputFile == null)
			this.outputFile = null;
		else if (outputFile.exists() &&
			(outputFile.isFile() == false || outputFile.canWrite() == false))
			throw new IllegalArgumentException(String.format(
				"Argument output file [%s] must be a writable file."));
		else this.outputFile = outputFile;
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
		PrintWriter writer = null;
		try {
			reader = new BufferedReader(new FileReader(mzTabFile.getFile()));
			// if an output file was specified, set up its writer
			if (outputFile != null)
				writer = new PrintWriter(
					new BufferedWriter(new FileWriter(outputFile, false)));
			String line = null;
			int lineNumber = 0;
			while (true) {
				line = reader.readLine();
				if (line == null)
					break;
				lineNumber++;
				for (MzTabProcessor processor : processors)
					line = processor.processMzTabLine(line, lineNumber);
				// if an output file was specified, write
				// the fully processed line to it
				if (writer != null)
					writer.println(line);
			}
		} catch (RuntimeException error) {
			throw error;
		} catch (Throwable error) {
			throw new RuntimeException(error);
		} finally {
			try { reader.close(); } catch (Throwable error) {}
			try { writer.close(); } catch (Throwable error) {}
		}
		// tear down all processors
		for (MzTabProcessor processor : processors)
			processor.tearDown();
	}
	
	/*========================================================================
	 * Property accessor methods
	 *========================================================================*/
	public MzTabFile getMzTabFile() {
		return mzTabFile;
	}
	
	public void addProcessor(MzTabProcessor processor) {
		if (processor != null)
			processors.add(processor);
	}
}
