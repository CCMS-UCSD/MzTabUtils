package edu.ucsd.util;

import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CommonUtils
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	public static final Pattern FILE_URI_PROTOCOL_PATTERN =
		Pattern.compile("file:(?:[/]{2})?(.*)");
//		Pattern.compile("[^/]+:(?:[/]{1,2})?(.*)");
	
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
	
	public static String pluralize(String name, int quantity) {
		return pluralize(name, (long)quantity);
	}
	
	public static String pluralize(String name, long quantity) {
		if (name == null)
			return null;
		else if (quantity == 1)
			return name;
		else if (name.charAt(name.length() - 1) == 's')
			return String.format("%ses", name);
		else return String.format("%ss", name);
	}
	
	public static String formatMilliseconds(long milliseconds) {
		long hours = TimeUnit.MILLISECONDS.toHours(milliseconds);
		long minutes = TimeUnit.MILLISECONDS.toMinutes(milliseconds) -
			TimeUnit.HOURS.toMinutes(hours);
		long seconds = TimeUnit.MILLISECONDS.toSeconds(milliseconds) -
			TimeUnit.HOURS.toSeconds(hours) -
			TimeUnit.MINUTES.toSeconds(minutes);
		long remainder = milliseconds - TimeUnit.HOURS.toMillis(hours) -
			TimeUnit.MINUTES.toMillis(minutes) -
			TimeUnit.SECONDS.toMillis(seconds);
		String suffix = remainder == 0 ? "" : String.format(".%03d", remainder);
		if (hours > 0)
			return String.format("%dh %dm %d%ss",
				hours, minutes, seconds, suffix);
		else if (minutes > 0)
			return String.format("%dm %d%ss", minutes, seconds, suffix);
		else if (seconds > 0 )
			return String.format("%d%ss", seconds, suffix);
		else return String.format("%d ms", milliseconds);
	}
	
	public static String formatBytes(long bytes) {
		return formatBytes(bytes, true);
	}
	
	public static String formatBytes(long bytes, boolean si) {
		int unit = si ? 1000 : 1024;
	    if (bytes < unit)
	    	return String.format("%d %s", bytes, pluralize("byte", bytes));
	    int exp = (int)(Math.log(bytes) / Math.log(unit));
	    String prefix = String.format("%s%s",
	    	(si ? "KMGTPE" : "KMGTPE").charAt(exp-1), (si ? "" : "i"));
	    return String.format("%.2f %sB", bytes / Math.pow(unit, exp), prefix);
	}
}
