package edu.ucsd.mztab.ui;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

import org.apache.commons.io.FilenameUtils;

import edu.ucsd.mztab.MzTabReader;
import edu.ucsd.mztab.TaskMzTabContext;
import edu.ucsd.mztab.model.MzTabConstants;
import edu.ucsd.mztab.model.MzTabFile;
import edu.ucsd.mztab.processors.CountProcessor;

public class MzTabCounter
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String USAGE =
		"java -cp MzTabUtils.jar edu.ucsd.mztab.ui.MzTabCounter" +
		"\n\t-mztab  <MzTabDirectory>" +
		"\n\t-params <ProteoSAFeParametersFile>" +
		"\n\t-output <OutputFile>";
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public static void main(String[] args) {
		MzTabCountOperation count = extractArguments(args);
		if (count == null)
			die(USAGE);
		PrintWriter writer = null;
		try {
			// parse out file mapping context for this task from params.xml
			TaskMzTabContext context = new TaskMzTabContext(
				count.mzTabDirectory, null, count.parameters);
			// set up output file writer
			if (count.outputFile.exists() == false &&
				count.outputFile.createNewFile() == false)
				die(String.format("Could not create output file [%s]",
					count.outputFile.getAbsolutePath()));
			writer = new PrintWriter(new BufferedWriter(
				new FileWriter(count.outputFile, false)));
			writer.println("MzTab_file\tUploaded_file\t" +
				"PSM_rows\tInvalid_PSM_rows\tFound_PSMs\tPSM_FDR\t" +
				"Peptide_rows\tFound_Peptides\tPeptide_FDR\t" +
				"Protein_rows\tFound_Proteins\tProtein_FDR\tFound_Mods");
			// read through all mzTab files, write counts to output file
			File[] files = count.mzTabDirectory.listFiles();
			// sort files alphabetically
			Arrays.sort(files);
			for (File file : files) {
				Map<String, Integer> counts = new HashMap<String, Integer>(7);
				MzTabFile mzTabFile = context.getMzTabFile(file);
				MzTabReader reader = new MzTabReader(mzTabFile);
				reader.addProcessor(new CountProcessor(counts));
				reader.read();
				// get relevant file names to print to output file
				String uploadedFilename = FilenameUtils.getName(
					mzTabFile.getUploadedResultPath());
				if (uploadedFilename == null)
					uploadedFilename = mzTabFile.getMzTabFilename();
				// extract global FDR values to print to output file
				String[] fdr = extractGlobalFDRValues(file);
				writer.println(String.format(
					"%s\t%s\t%d\t%d\t%d\t%s\t%d\t%d\t%s\t%d\t%d\t%s\t%d",
					file.getName(), uploadedFilename,
					counts.get("PSM"), counts.get("invalid_PSM"),
					counts.get("PSM_ID"), fdr[0],
					counts.get("PEP"), counts.get("sequence"), fdr[1],
					counts.get("PRT"), counts.get("accession"), fdr[2],
					counts.get("modification")));
			}
		} catch (Throwable error) {
			die(error.getMessage(), error);
		} finally {
			try { writer.close(); }
			catch (Throwable error) {}
		}
	}
	
	/*========================================================================
	 * Convenience classes
	 *========================================================================*/
	/**
	 * Struct to maintain context data for each mzTab count operation.
	 */
	private static class MzTabCountOperation {
		/*====================================================================
		 * Properties
		 *====================================================================*/
		private File mzTabDirectory;
		private File parameters;
		private File outputFile;
		
		/*====================================================================
		 * Constructors
		 *====================================================================*/
		public MzTabCountOperation(
			File mzTabDirectory, File parameters,File outputFile
		) {
			// validate mzTab directory
			if (mzTabDirectory == null)
				throw new NullPointerException(
					"MzTab directory cannot be null.");
			else if (mzTabDirectory.isDirectory() == false)
				throw new IllegalArgumentException(
					String.format("MzTab directory [%s] must be a directory.",
						mzTabDirectory.getAbsolutePath()));
			else if (mzTabDirectory.canRead() == false)
				throw new IllegalArgumentException(
					String.format("MzTab directory [%s] must be readable.",
						mzTabDirectory.getAbsolutePath()));
			else this.mzTabDirectory = mzTabDirectory;
			// validate params.xml file
			if (parameters == null)
				throw new NullPointerException(
					"Argument params.xml file cannot be null.");
			else if (parameters.isFile() == false ||
				parameters.canRead() == false)
				throw new IllegalArgumentException(
					"Argument params.xml file must be a readable file.");
			else this.parameters = parameters;
			// validate output file
			if (outputFile == null)
				throw new NullPointerException("Output file cannot be null.");
			else if (outputFile.isDirectory())
				throw new IllegalArgumentException(String.format(
					"Output file [%s] must be a normal (non-directory) file.",
					outputFile.getAbsolutePath()));
			else this.outputFile = outputFile;
		}
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private static MzTabCountOperation extractArguments(String[] args) {
		if (args == null || args.length < 1)
			return null;
		File mzTabDirectory = null;
		File parameters = null;
		File output = null;
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
				else if (argument.equals("-params"))
					parameters = new File(value);
				else if (argument.equals("-output"))
					output = new File(value);
				else return null;
			}
		}
		try {
			return new MzTabCountOperation(mzTabDirectory, parameters, output);
		} catch (Throwable error) {
			System.err.println(error.getMessage());
			return null;
		}
	}
	
	private static String[] extractGlobalFDRValues(File mzTabFile) {
		if (mzTabFile == null || mzTabFile.isFile() == false ||
			mzTabFile.canRead() == false)
			return null;
		// read through input mzTab file, pull out global FDR values
		// from the "false_discovery_rate" MTD row, if present
		BufferedReader reader = null;
		String[] fdr = new String[3];
		try {
			reader = new BufferedReader(new FileReader(mzTabFile));
			String line = null;
			while (true) {
				line = reader.readLine();
				if (line == null)
					break;
				// if the next line is a comment or is blank, skip it
				if (line.startsWith("COM") || line.trim().equals(""))
					continue;
				// if the next line is past the MTD section, then we're done
				else if (line.startsWith("MTD") == false)
					break;
				else {
					// if this is not the FDR line, skip it
					Matcher matcher =
						MzTabConstants.FDR_LINE_PATTERN.matcher(line);
					if (matcher.matches() == false)
						continue;
					// parse out FDR values from the FDR line and validate them
					String fdrList = matcher.group(1);
					if (fdrList == null)
						return fdr;
					fdrList = fdrList.trim();
					if (fdrList.equals("") || fdrList.equalsIgnoreCase("null"))
						return fdr;
					String[] fdrCVTerms = fdrList.split("\\|");
					if (fdrCVTerms == null || fdrCVTerms.length < 1)
						return fdr;
					for (String fdrCVTerm : fdrCVTerms) {
						matcher =
							MzTabConstants.CV_TERM_PATTERN.matcher(fdrCVTerm);
						if (matcher.matches()) {
							// parse out the CV term and match it up to the
							// correct FDR index in the returned array
							String accession = matcher.group(2);
							if (accession == null)
								continue;
							accession = accession.trim();
							Integer index = null;
							// PSM-level FDR
							if (accession.equals("MS:1002350"))
								index = 0;
							// Peptide-level FDR
							else if (accession.equals("MS:1001364"))
								index = 1;
							// Protein-level FDR
							else if (accession.equals("MS:1001214"))
								index = 2;
							// some other CV term
							else continue;
							// ensure CV term value parses to double
							String value = matcher.group(4);
							try { Double.parseDouble(value); }
							catch (Throwable error) { continue; }
							// write validated CV term value to correct slot
							fdr[index] = value;
						}
					}
				}
			}
		} catch (RuntimeException error) {
			throw error;
		} catch (Throwable error) {
			throw new RuntimeException(error);
		} finally {
			try { reader.close(); } catch (Throwable error) {}
		}
		return fdr;
	}
	
	private static void die(String message) {
		die(message, null);
	}
	
	private static void die(String message, Throwable error) {
		if (message == null)
			message = "There was an error generating statistics " +
				"on this set of mzTab files";
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
