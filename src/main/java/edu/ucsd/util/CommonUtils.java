package edu.ucsd.util;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

import edu.ucsd.mztab.model.MzTabConstants;

public class CommonUtils
{
	/*========================================================================
	 * Static utility methods
	 *========================================================================*/
	public static String cleanFileURL(String fileURLString) {
		if (fileURLString == null)
			return null;
		// if this is a file URI, clean it
		Matcher matcher =
			MzTabConstants.FILE_URI_PROTOCOL_PATTERN.matcher(fileURLString);
		if (matcher.matches())
			fileURLString = matcher.group(1);
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
	
	public static Boolean parseBooleanColumn(String value) {
		if (value == null)
			return null;
		else value = value.trim();
		// boolean columns can only be interpreted
		// using standard boolean string values
		if (value.equals("1") ||
			value.equalsIgnoreCase("true") ||
			value.equalsIgnoreCase("yes") ||
			value.equalsIgnoreCase("on"))
			return true;
		else if (value.equals("0") ||
			value.equalsIgnoreCase("false") ||
			value.equalsIgnoreCase("no") ||
			value.equalsIgnoreCase("off"))
			return false;
		// any other value, even though present in the column,
		// cannot be interpreted and thus we call it null
		else return null;
	}
	
	public static boolean headerCorrespondsToColumn(
		String header, String column, Map<Integer, String> scoreColumns
	) {
		if (header == null || column == null)
			return false;
		else if (header.equalsIgnoreCase(column))
			return true;
		else if (header.equalsIgnoreCase(
			String.format("opt_global_%s", column)))
			return true;
		// check mapped score columns, if present
		else if (scoreColumns != null) {
			for (Integer index : scoreColumns.keySet())
				if (header.equalsIgnoreCase(
					String.format("search_engine_score[%d]", index)) &&
					column.equalsIgnoreCase(scoreColumns.get(index)))
					return true;
		}
		return false;
	}
}
