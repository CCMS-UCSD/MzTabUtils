package edu.ucsd.util;

import java.io.File;
import java.util.Collection;
import java.util.Comparator;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringEscapeUtils;

/**
 * Comparator to sort file paths lexicographically in reverse path element
 * order.
 */
public class FilePathComparator
implements Comparator<String>
{
	/*====================================================================
	 * Public interface methods
	 *====================================================================*/
	public int compare(String path1, String path2) {
		// first split paths into arrays of path elements
		String[] elements1 = FilenameUtils.separatorsToSystem(path1)
			.split(StringEscapeUtils.escapeJava(File.separator));
		String[] elements2 = FilenameUtils.separatorsToSystem(path2)
			.split(StringEscapeUtils.escapeJava(File.separator));
		// then traverse paths in reverse order,
		// differentiating only when a path element differs
		int steps = 0;
		for (int i=elements1.length-1; i>=0; i--) {
			String element1 = elements1[i];
			// try grabbing the element from the second
			// path array that is this many steps back
			int index2 = elements2.length - 1 - steps;
			// if the second path doesn't have this many elements, then the
			// first path is longer and therefore should compare higher
			if (index2 < 0)
				return steps + 1;
			else {
				String element2 = elements2[index2];
				int comparison = element1.compareTo(element2);
				if (comparison > 0)
					return steps + 1;
				else if (comparison < 0)
					return (steps + 1) * -1;
			}
			steps++;
		}
		// if all elements of the first path have been exhausted, and the
		// second path still has more, then the second path is longer and
		// therefore should compare higher
		if (elements2.length > steps)
			return (steps + 1) * -1;
		// otherwise both paths are completely identical
		else return 0;
	}
	
	public String findBestFileMatch(
		String filename, Collection<String> filenames
	) {
		if (filename == null || filenames == null || filenames.isEmpty())
			return null;
		int bestComparison = 0;
		String bestMatch = null;
		for (String comparedFilename : filenames) {
			int comparison = new FilePathComparator().compare(
				filename, comparedFilename);
			// the paths are a perfect match if the comparator returns 0
			if (comparison == 0)
				return comparedFilename;
			// the paths don't match at all if the
			// comparator returns either 1 or -1
			else if (comparison == 1 || comparison == -1)
				continue;
			// otherwise the magnitude is proportional to the number of
			// path elements that matched; meaning higher is better
			else if (bestMatch == null || comparison > bestComparison) {
				bestMatch = comparedFilename;
				bestComparison = comparison;
			}
		}
		return bestMatch;
	}
}
