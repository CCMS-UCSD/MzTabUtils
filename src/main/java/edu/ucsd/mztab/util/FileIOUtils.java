package edu.ucsd.mztab.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import edu.ucsd.mztab.model.MzTabConstants;

public class FileIOUtils
{
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public static final String readFile(File file)
	throws IOException {
		if (file == null)
			return null;
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			StringBuffer contents = new StringBuffer();
			String line = null;
			while ((line = reader.readLine()) != null) {
				contents.append(line);
				contents.append("\n");
			}
			return contents.toString();
		} catch (IOException error) {
			throw new IOException(String.format("Error reading file \"%s\"",
				file.getAbsolutePath()), error);
		} finally {
			if (reader != null) try {
				reader.close();
			} catch (IOException error) {}
		}
	}
	
	public static final boolean writeFile(
		File file, String contents, boolean append
	) throws IOException {
		if (file == null || contents == null)
			return false;
		// write document to file
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(file, append));
			writer.write(contents);
			return true;
		} catch (IOException error) {
			throw new IOException(String.format("Error writing to file \"%s\"",
				file.getAbsolutePath()), error);
		} finally {
			if (writer != null) try {
				writer.close();
			} catch (IOException error) {}
		}
	}
	
	public static final File copyFile(
		File source, File destinationFolder, String newFilename
	) throws IOException {
		if (source == null || destinationFolder == null)
			return null;
		// verify source file
		else if (source.canRead() == false)
			throw new IllegalArgumentException(String.format(
				"Error copying dataset file [%s]: file cannot be read.",
				source.getAbsolutePath()));
		// verify destination directory
		else if (destinationFolder.exists() == false &&
			destinationFolder.mkdirs() == false)
			throw new IllegalArgumentException(String.format("Error copying " +
				"dataset file: Destination directory [%s] does not exist " +
				"and could not be created.",
				destinationFolder.getAbsolutePath()));
		else if (destinationFolder.isDirectory() == false)
			throw new IllegalArgumentException(String.format("Error copying " +
				"dataset file: Destination directory [%s] is not a valid " +
				"directory.", destinationFolder.getAbsolutePath()));
		else if (destinationFolder.canWrite() == false)
			throw new IllegalArgumentException(String.format("Error copying " +
				"dataset file: Destination directory [%s] is not writable.",
				destinationFolder.getAbsolutePath()));
		// if no new filename is specified, use existing source filename
		if (newFilename == null)
			newFilename = source.getName();
		// derive source file verification data
		long sourceSize = source.length();
		long sourceChecksum = FileUtils.checksumCRC32(source);
		// copy file to specified destination
		File destination = new File(destinationFolder, newFilename);
		// if file already exists, check to be sure it was copied properly;
		// if it doesn't exist, or its stats don't match, then copy now
		if (destination.exists()) {
			long destinationSize = destination.length();
			long destinationChecksum = FileUtils.checksumCRC32(destination);
			if (destinationSize != sourceSize ||
				destinationChecksum != sourceChecksum)
				FileUtils.copyFile(source, destination);
			// if file already exists and its stats match, then we're done
			else return destination;
		} else FileUtils.copyFile(source, destination);
		// verify copied file
		long destinationSize = destination.length();
		if (destinationSize != sourceSize)
			throw new IOException(String.format("Error copying dataset " +
				"file [%s] to destination [%s]: file size comparison " +
				"failed (expected %d, found %d).",
				source.getAbsolutePath(), destination.getAbsolutePath(),
				sourceSize, destinationSize));
		long destinationChecksum = FileUtils.checksumCRC32(destination);
		if (destinationChecksum != sourceChecksum)
			throw new IOException(String.format("Error copying dataset " +
				"file [%s] to destination [%s]: checksum comparison " +
				"failed (expected %d, found %d).",
				source.getAbsolutePath(), destination.getAbsolutePath(),
				sourceChecksum, destinationChecksum));
		return destination;
	}
	
	public static final Document parseXML(File file)
	throws IOException {
		if (file == null || file.canRead() == false)
			return null;
		else {
			// read XML file contents into string
			String contents = readFile(file);
			// build XML document from parameters file
			return parseXML(contents);
		}
	}
	
	public static final Document parseXML(String contents)
	throws IOException {
		if (contents == null)
			return null;
		else {
			// get document builder
			DocumentBuilderFactory factory =
				DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = null;
			try {
				builder = factory.newDocumentBuilder();
			} catch (ParserConfigurationException error) {
				throw new IOException(
					"Error instantiating XML DocumentBuilder", error);
			}
			// parse XML string into document
			Document document = null;
			try {
				document = builder.parse(
					new ByteArrayInputStream(contents.getBytes()));
			} catch (IOException error) {
				throw new IOException("Error parsing XML document", error);
			} catch (SAXException error) {
				throw new IOException("Error parsing XML document", error);
			}
			return document;
		}
	}
	
	public static final String printXML(Document document)
	throws IOException {
		if (document == null)
			return null;
		else {
			Transformer transformer = null;
			try {
				transformer =
					TransformerFactory.newInstance().newTransformer();
			} catch (TransformerConfigurationException error) {
				throw new IOException(
					"Error instantiating XML Transformer", error);
			}
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			StreamResult result = new StreamResult(new StringWriter());
			DOMSource source = new DOMSource(document);
			try {
				transformer.transform(source, result);
			} catch (TransformerException error) {
				throw new IOException("Error transforming XML", error);
			}
			return result.getWriter().toString();
		}
	}
	
	public static void makeLink(File source, File link) {
		if (source == null)
			throw new IllegalArgumentException(
				"Link source file cannot be null.");
		else if (link == null)
			throw new NullPointerException(
				"Link destination file cannot be null.");
		else if (link.isDirectory())
			link = new File(link, source.getName());
		// ensure that destination directory is present
		if (link.getParentFile().exists() == false)
			link.getParentFile().mkdirs();
		// build link command
		String[] command = getLinkCommand(source, link);
		StringBuffer message = new StringBuffer();
		for (String token : command)
			message.append(token).append(" ");
		// execute link command
		Process process = null;
		try {
			process = Runtime.getRuntime().exec(command);
			process.waitFor();
		} catch (Throwable error) {
			throw new RuntimeException(
				"Could not execute requested link operation: " +
				error.getMessage(), error);
		} finally {
			try { process.getInputStream().close(); } catch (Throwable error) {}
			try { process.getOutputStream().close(); } catch (Throwable error) {}
			try { process.getErrorStream().close(); } catch (Throwable error) {}
			try { process.destroy(); } catch (Throwable error) {}
		}
	}
	
	public static Collection<File> findFiles(File directory) {
		if (directory == null || directory.canRead() == false ||
			directory.isDirectory() == false)
			return null;
		File[] files = directory.listFiles();
		if (files == null || files.length < 1)
			return null;
		// sort files alphabetically
		Arrays.sort(files);
		// add all found files to collection
		Collection<File> foundFiles = new ArrayList<File>();
		for (File file : files) {
			// recurse into subdirectories
			if (file.isDirectory()) {
				Collection<File> descendantFiles = findFiles(file);
				if (descendantFiles != null &&
					descendantFiles.isEmpty() == false)
					foundFiles.addAll(descendantFiles);
			} else foundFiles.add(file);
		}
		return foundFiles;
	}
	
	public static File findFileInDataset(
		String filePath, String datasetID, Collection<File> datasetFiles
	) {
		if (filePath == null || datasetID == null)
			return null;
		// otherwise, look through all the dataset's
		// files to find find the best match
		if (datasetFiles == null) {
			// get dataset directory
			File datasetDirectory =
				new File(MzTabConstants.DATASET_FILES_ROOT, datasetID);
			// if dataset directory does not yet exist, then this is an original
			// submission and obviously it doesn't contain any files yet
			if (datasetDirectory.isDirectory() == false)
				return null;
			// get all of this dataset's files
			datasetFiles = FileIOUtils.findFiles(datasetDirectory);
		}
		// if the dataset has no files, then obviously this one isn't there
		if (datasetFiles == null || datasetFiles.isEmpty())
			return null;
		// find both exact path matches and leaf (filename) matches
		Collection<File> exactMatches = new HashSet<File>();
		Collection<File> leafMatches = new HashSet<File>();
		// be sure relative path starts with a slash so
		// we aren't matching directory name substrings
		if (filePath.startsWith("/") == false)
			filePath = String.format("/%s", filePath);
		// get leaf filename of argument path
		String filename = FilenameUtils.getName(filePath);
		// look through all files to find matches
		for (File datasetFile : datasetFiles) {
			if (datasetFile.getAbsolutePath().endsWith(filePath))
				exactMatches.add(datasetFile);
			else if (datasetFile.getName().equals(filename))
				leafMatches.add(datasetFile);
		}
		// if there one exact match, return that
		if (exactMatches.size() == 1)
			return exactMatches.iterator().next();
		// if there is more than one exact match,
		// then we don't know which one to pick
		else if (exactMatches.size() > 1)
			throw new IllegalStateException(String.format(
				"Dataset [%s] contains %d distinct files " +
				"with identical relative path [%s].",
				datasetID, exactMatches.size(), filePath));
		// if there are no exact matches but one leaf match, return that
		else if (leafMatches.size() == 1)
			return leafMatches.iterator().next();
		// if there is more than one remaining match,
		// then we don't know which one to pick
		else if (leafMatches.size() > 1)
			throw new IllegalStateException(String.format(
				"Dataset [%s] contains %d distinct files " +
				"with identical filename [%s].",
				datasetID, leafMatches.size(), filename));
		// if there are no matches at all, then it's just not there
		else return null;
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private static String[] getLinkCommand(File source, File destination) {
		if (source == null || destination == null)
			return null;
		String srcPath = source.getAbsolutePath();
		String dstPath = destination.getAbsolutePath();
		return System.getProperty("os.name").startsWith("Windows") ? 
			new String[]{"CMD", "/C", "mklink",
				source.isDirectory() ? "/J" : "/H", dstPath, srcPath} :
			new String[]{"ln", "-s", srcPath, dstPath};
	}
}
