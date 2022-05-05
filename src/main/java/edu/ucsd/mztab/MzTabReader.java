package edu.ucsd.mztab;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.ucsd.mztab.model.MzTabFile;
import edu.ucsd.mztab.model.MzTabProcessor;
import edu.ucsd.mztab.model.TimingRecord;
import edu.ucsd.mztab.util.CommonUtils;

public class MzTabReader
{
    /*========================================================================
     * Constants
     *========================================================================*/
    private static final String DEBUG_LOG_HEADER =
        "category\tcount\ttotal_time_ns\tmax_time_ns\tmin_time_ns\taverage_time_ns";

	/*========================================================================
	 * Properties
	 *========================================================================*/
	private MzTabFile                 mzTabFile;
	private List<MzTabProcessor>      processors;
	private File                      outputFile;
	private File                      debugLogFile;
	private Map<String, TimingRecord> timing;
	
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
		// initialize timing record map
		timing = new LinkedHashMap<String, TimingRecord>();
	}
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public void read() {
		long start = System.nanoTime();
		// set up all processors
		long checkpoint = start;
		for (MzTabProcessor processor : processors) {
			processor.setUp(mzTabFile);
			long end = System.nanoTime();
			addTiming(String.format("%s_processor_setup", processor.getClass().getName()),
				end - checkpoint);
			checkpoint = end;
		}
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
			long readStart = System.nanoTime();
			checkpoint = readStart;
			while (true) {
				line = reader.readLine();
				if (line == null)
					break;
				long end = System.nanoTime();
				addTiming("line_read", end - checkpoint);
				checkpoint = end;
				lineNumber++;
				for (MzTabProcessor processor : processors) {
					line = processor.processMzTabLine(line, lineNumber);
					end = System.nanoTime();
					addTiming(String.format("%s_processor_read", processor.getClass().getName()),
						end - checkpoint);
					checkpoint = end;
				}
				// if an output file was specified, write
				// the fully processed line to it
				if (writer != null) {
					writer.println(line);
					end = System.nanoTime();
					addTiming("line_write", end - checkpoint);
					checkpoint = end;
				}
			}
			addTiming("all_lines_read", checkpoint - readStart);
		} catch (RuntimeException error) {
			throw error;
		} catch (Throwable error) {
			throw new RuntimeException(error);
		} finally {
			try { reader.close(); } catch (Throwable error) {}
			try { writer.close(); } catch (Throwable error) {}
		}
		// tear down all processors
		for (MzTabProcessor processor : processors) {
			processor.tearDown();
			long end = System.nanoTime();
			addTiming(String.format("%s_processor_teardown", processor.getClass().getName()),
				end - checkpoint);
			checkpoint = end;
		}
		addTiming("total_read", checkpoint - start);
		// TODO: implement debug flag, only do this if set
		dumpTiming();
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

	public void setDebugLogFile(File debugLogFile) {
		this.debugLogFile = debugLogFile;
	}

    /*========================================================================
     * Convenience methods
     *========================================================================*/
    private void addTiming(String type, long milliseconds) {
        if (type == null)
            return;
        TimingRecord timingRecord = timing.get(type);
        if (timingRecord == null)
            timingRecord = new TimingRecord();
        timingRecord.add(milliseconds);
        timing.put(type, timingRecord);
    }

    private void dumpTiming() {
        // write full timing details to debug log file, if specified
        PrintWriter writer = null;
        if (debugLogFile != null) try {
            writer = new PrintWriter(new FileWriter(debugLogFile));
            writer.println(DEBUG_LOG_HEADER);
            for (Entry<String, TimingRecord> entry : timing.entrySet())
                writer.println(String.format(
                    "%s\t%s", entry.getKey(), entry.getValue().toString()));
        } catch (Throwable error) {
            System.err.println(String.format(
                "Could not write mzTab reader timing details to debug log file [%s].",
                debugLogFile.getAbsolutePath()));
            error.printStackTrace();
        } finally {
            if (writer != null) try {
                writer.close();
            } catch (Throwable error) {}
        }
        // write timing summary to console
        StringBuilder message = new StringBuilder("mzTab reader timing summary:\n----------");
        TimingRecord record = timing.get("total_read");
        if (record != null)
            message.append("\nOverall time:\t").append(
                CommonUtils.formatNanoseconds(record.getTotal()));
        record = timing.get("line_read");
        if (record != null) {
            message.append("\nFile read time:\t").append(
                CommonUtils.formatNanoseconds(record.getTotal()));
            message.append("\n  Lines read:\t").append(String.format("%,d", record.getCount()));
            message.append("\n  Maximum line read time:\t").append(
                CommonUtils.formatNanoseconds(record.getMax()));
            message.append("\n  Minimum line read time:\t").append(
                CommonUtils.formatNanoseconds(record.getMin()));
            message.append("\n  Average line read time:\t").append(
                CommonUtils.formatNanoseconds(Math.round(record.getAverage())));
        }
        record = timing.get("all_lines_read");
        if (record != null)
            message.append("\nFile processing time:\t").append(
                CommonUtils.formatNanoseconds(record.getTotal()));
        // organize per-processor timing records
        Map<String, Map<String, TimingRecord>> processorRecords =
            new HashMap<String, Map<String, TimingRecord>>();
        for (Entry<String, TimingRecord> entry : timing.entrySet()) {
            String type = entry.getKey();
            String processorName = null;
            if (type.endsWith("_processor_setup"))
                processorName = type.substring(0, type.length() - 16);
            else if (type.endsWith("_processor_read"))
                processorName = type.substring(0, type.length() - 15);
            else if (type.endsWith("_processor_teardown"))
                processorName = type.substring(0, type.length() - 19);
            if (processorName == null)
                continue;
            Map<String, TimingRecord> processorRecord = processorRecords.get(processorName);
            if (processorRecord == null)
                processorRecord = new HashMap<String, TimingRecord>(3);
            if (type.endsWith("_processor_setup"))
                processorRecord.put("setup", entry.getValue());
            else if (type.endsWith("_processor_read"))
                processorRecord.put("read", entry.getValue());
            else if (type.endsWith("_processor_teardown"))
                processorRecord.put("teardown", entry.getValue());
            processorRecords.put(processorName, processorRecord);
        }
        // write timing summary for each processor
        for (Entry<String, Map<String, TimingRecord>> entry : processorRecords.entrySet()) {
            message.append("\nProcessor [").append(entry.getKey()).append("]:");
            Map<String, TimingRecord> processorRecord = entry.getValue();
            record = processorRecord.get("setup");
            if (record != null)
                message.append("\n  Setup time:\t").append(
                    CommonUtils.formatNanoseconds(record.getTotal()));
            record = processorRecord.get("read");
            if (record != null) {
                message.append("\n  Processing time:\t").append(
                    CommonUtils.formatNanoseconds(record.getTotal()));
                message.append("\n    Lines processed:\t").append(
                    String.format("%,d", record.getCount()));
                message.append("\n    Maximum line processing time:\t").append(
                    CommonUtils.formatNanoseconds(record.getMax()));
                message.append("\n    Minimum line processing time:\t").append(
                    CommonUtils.formatNanoseconds(record.getMin()));
                message.append("\n    Average line processing time:\t").append(
                    CommonUtils.formatNanoseconds(Math.round(record.getAverage())));
            }
            record = processorRecord.get("teardown");
            if (record != null)
                message.append("\n  Teardown time:\t").append(
                    CommonUtils.formatNanoseconds(record.getTotal()));
        }
        message.append("\n----------");
        System.out.println(message.toString());
    }
}
