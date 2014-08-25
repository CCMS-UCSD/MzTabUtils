package edu.ucsd.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

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

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

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
	
	public static final boolean copyFile(File source, File destination)
	throws IOException {
		if (source == null || destination == null)
			return false;
		BufferedReader reader = null;
		PrintWriter writer = null;
		try {
			reader = new BufferedReader(new FileReader(source));
			writer = new PrintWriter(
				new BufferedWriter(new FileWriter(destination, false)));
			String line = null;
			while ((line = reader.readLine()) != null)
				writer.println(line);
			return true;
		} catch (IOException error) {
			throw new IOException(String.format(
					"Error copying contents of file \"%s\" to file \"%s\"",
					source.getAbsolutePath(), destination.getAbsolutePath()),
				error);
		} finally {
			if (reader != null) try {
				reader.close();
			} catch (Throwable error) {}
			if (writer != null) try {
				writer.close();
			} catch (Throwable error) {}
		}
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
