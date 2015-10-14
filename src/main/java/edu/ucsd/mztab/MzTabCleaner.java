package edu.ucsd.mztab;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.ucsd.util.CommonUtils;

public class MzTabCleaner
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	private static final String USAGE =
		"java -cp MassIVEUtils.jar edu.ucsd.mztab.MzTabCleaner" +
		"\n\t-params <ParameterFile>" +
		"\n\t-mztab  <MzTabDirectory>" +
		"\n\t-id     <DatasetIDFile>" +
		"\n\t-output <CleanedMzTabDirectory>";
	private static final Pattern FILE_LINE_PATTERN =
		Pattern.compile("^MTD\\s+ms_run\\[(\\d+)\\]-location\\s+(.+)$");
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public static void main(String[] args) {
		MzTabCleanupOperation cleanup = extractArguments(args);
		if (cleanup == null)
			die(USAGE);
		try {
			// read all mzTab files, convert all peak list file references
			// (i.e. "ms_run[*]-location" values) to the proper MassIVE FTP URLs
			Collection<File> mzTabFiles = cleanup.context.getMzTabFiles();
			if (mzTabFiles == null || mzTabFiles.isEmpty()) {
				System.out.println("No files were submitted in the " +
					"\"RESULT\" category for this dataset, so no mzTab " +
					"file cleanup is necessary.");
			} else for (File mzTabFile : mzTabFiles) {
				cleanMzTabFile(mzTabFile, cleanup);
			}
		} catch (Throwable error) {
			die(getRootCause(error).getMessage());
		}
	}
	
	/*========================================================================
	 * Convenience classes
	 *========================================================================*/
	/**
	 * Struct to maintain context data for each mzTab file cleanup operation.
	 */
	private static class MzTabCleanupOperation {
		/*====================================================================
		 * Properties
		 *====================================================================*/
		private MassIVEMzTabContext context;
		private File                outputDirectory;
		private String              datasetID;
		
		/*====================================================================
		 * Constructors
		 *====================================================================*/
		public MzTabCleanupOperation(
			File parameters, File mzTabDirectory,
			File outputDirectory, File datasetIDFile
		) {
			// validate parameters file
			if (parameters == null)
				throw new NullPointerException(
					"Parameters file cannot be null.");
			else if (parameters.isFile() == false)
				throw new IllegalArgumentException(
					String.format(
						"Parameters file [%s] must be a regular file.",
						parameters.getAbsolutePath()));
			else if (parameters.canRead() == false)
				throw new IllegalArgumentException(
					String.format("Parameters file [%s] must be readable.",
						parameters.getAbsolutePath()));
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
			// build mzTab file context
			context = new MassIVEMzTabContext(parameters, mzTabDirectory, null);
			// validate output directory
			if (outputDirectory == null)
				throw new NullPointerException(
					"Output mzTab directory cannot be null.");
			else if (outputDirectory.exists() == false &&
				outputDirectory.mkdirs() == false)
				throw new IllegalArgumentException(String.format(
					"Output mzTab directory [%s] could not be created.",
					outputDirectory.getAbsolutePath()));
			else if (outputDirectory.isDirectory() == false)
				throw new IllegalArgumentException(String.format(
					"Output mzTab directory [%s] must be a directory.",
					outputDirectory.getAbsolutePath()));
			this.outputDirectory = outputDirectory;
			// validate dataset ID file
			if (datasetIDFile == null)
				throw new NullPointerException(
					"Dataset ID file cannot be null.");
			else if (datasetIDFile.isFile() == false)
				throw new IllegalArgumentException(
					String.format(
						"Dataset ID file [%s] must be a regular file.",
						datasetIDFile.getAbsolutePath()));
			else if (datasetIDFile.canRead() == false)
				throw new IllegalArgumentException(
					String.format("Dataset ID file [%s] must be readable.",
						datasetIDFile.getAbsolutePath()));
			// read file to get dataset ID
			RandomAccessFile input = null;
			try {
				input = new RandomAccessFile(datasetIDFile, "r");
				datasetID = input.readLine();
			} catch (Throwable error) {
				throw new IllegalArgumentException(
					String.format("Could not read dataset ID from file [%s].",
						datasetIDFile.getAbsolutePath()), error);
			} finally {
				try { input.close(); } catch (Throwable error) {}
			}
		}
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private static MzTabCleanupOperation extractArguments(String[] args) {
		if (args == null || args.length < 1)
			return null;
		File params = null;
		File mzTabDirectory = null;
		File outputDirectory = null;
		File datasetIDFile = null;
		for (int i=0; i<args.length; i++) {
			String argument = args[i];
			if (argument == null)
				return null;
			else {
				i++;
				if (i >= args.length)
					return null;
				String value = args[i];
				if (argument.equals("-params"))
					params = new File(value);
				else if (argument.equals("-mztab"))
					mzTabDirectory = new File(value);
				else if (argument.equals("-output"))
					outputDirectory = new File(value);
				else if (argument.equals("-id"))
					datasetIDFile = new File(value);
				else return null;
			}
		}
		try {
			return new MzTabCleanupOperation(
				params, mzTabDirectory, outputDirectory, datasetIDFile);
		} catch (Throwable error) {
			System.err.println(error.getMessage());
			return null;
		}
	}
	
	private static void cleanMzTabFile(
		File mzTabFile, MzTabCleanupOperation cleanup
	) throws IOException {
		if (mzTabFile == null)
			return;
		else if (cleanup == null)
			throw new NullPointerException(String.format(
				"Cannot clean mzTab file [%s], since the provided dataset " +
				"mzTab context object is null.", mzTabFile.getAbsolutePath()));
		// read mzTab file, update all "ms_run[*]-location" lines
		File cleanedMzTabFile =
			new File(cleanup.outputDirectory, mzTabFile.getName());
		BufferedReader reader = null;
		PrintWriter writer = null;
		try {
			reader = new BufferedReader(new FileReader(mzTabFile));
			writer = new PrintWriter(new BufferedWriter(
				new FileWriter(cleanedMzTabFile, false)));
			String line = null;
			while (true) {
				line = reader.readLine();
				if (line == null)
					break;
				// simply copy the line to the output file
				// verbatim if it's past the metadata section
				else if (line.startsWith("MTD") == false &&
					line.startsWith("COM") == false &&
					line.trim().equals("") == false) {
					writer.println(line);
					continue;
				}
				// if this is a file location line, update it
				Matcher matcher = FILE_LINE_PATTERN.matcher(line);
				if (matcher.matches()) {
					// get dataset repository relative path for
					// this referenced peak list file
					String datasetRelativePath =
						cleanup.context.getUploadedPeakListFilename(
							mzTabFile.getName(),
							CommonUtils.cleanFileURL(matcher.group(2)));
					if (datasetRelativePath == null)
						throw new IllegalArgumentException(String.format(
							"Cannot clean mzTab file [%s], since no mapping " +
							"could be found for \"ms_run[%s]-location\" " +
							"value [%s]:\n%s", mzTabFile.getAbsolutePath(),
							matcher.group(1), matcher.group(2),
							cleanup.context.toJSON()));
					else writer.println(String.format(
						"MTD\tms_run[%s]-location\t" +
						"ftp://%s@massive.ucsd.edu/peak/%s", matcher.group(1),
						cleanup.datasetID, datasetRelativePath));
				}
				// otherwise simply copy the line to the output file verbatim
				else writer.println(line);
			}
		} catch (IOException error) {
			throw error;
		} catch (RuntimeException error) {
			throw error;
		} catch (Throwable error) {
			throw new RuntimeException(error);
		} finally {
			try { reader.close(); } catch (Throwable error) {}
			try { writer.close(); } catch (Throwable error) {}
		}
	}
	
	private static void die(String message) {
		die(message, null);
	}
	
	private static void die(String message, Throwable error) {
		if (message == null)
			message = "There was an error cleaning " +
				"the mzTab files of a MassIVE dataset";
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
	
	private static Throwable getRootCause(Throwable error) {
		if (error == null)
			return null;
		Throwable cause = error.getCause();
		if (cause == null)
			return error;
		else return getRootCause(cause);
	}
}
