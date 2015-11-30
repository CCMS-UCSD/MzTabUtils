package edu.ucsd.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CommonUtils
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	public static final Pattern FILE_URI_PROTOCOL_PATTERN =
		Pattern.compile("file:(?:[/\\\\]{1,2})?(.*)");
	//	Pattern.compile("[^/]+:(?:[/]{1,2})?(.*)");
	
	/*========================================================================
	 * Static utility methods
	 *========================================================================*/
	public static String cleanFileURL(String fileURLString) {
		if (fileURLString == null)
			return null;
		// account for buggy mzidentml-lib implementation
		Pattern pattern = Pattern.compile("^[^:/]+:/{2,3}([^:/]+://.*)$");
		Matcher matcher = pattern.matcher(fileURLString);
		if (matcher.matches())
			fileURLString = matcher.group(1);
		// if this is a file URI, clean it
		matcher = FILE_URI_PROTOCOL_PATTERN.matcher(fileURLString);
		if (matcher.matches())
			fileURLString = matcher.group(1);
		// account for buggy jmzTab file URLs
		if (fileURLString.startsWith("file:"))
			fileURLString = fileURLString.substring(5);
		return fileURLString;
	}
}
