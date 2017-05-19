package edu.ucsd.mztab.util;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;

// TODO: this functionality should be factored out into an
// application that should have knowledge of ProteoSAFe/MassIVE
// files - NOT a generic mzTab utility package like this!
public class ProteoSAFeUtils
{
	/*========================================================================
	 * Constants
	 *========================================================================*/
	public static final String FILE_DESCRIPTOR_PATTERN = "^.{1}\\..*$";
	public static final String DATASET_ID_PATTERN = "^MSV[0-9]{9}$";
	public static final String DATASET_FILES_ROOT = "/data/ccms-data/uploads";
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public static File findFileInDataset(
		String filePath, String datasetID, Collection<File> datasetFiles
	) {
		if (filePath == null || datasetID == null)
			return null;
		// otherwise, look through all the dataset's
		// files to find find the best match
		if (datasetFiles == null) {
			// get dataset directory
			File datasetDirectory = new File(DATASET_FILES_ROOT, datasetID);
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
		// if there is more than one exact match, try to
		// prune by considering known processed collections
		else if (exactMatches.size() > 1) {
			Collection<File> prunedMatches =
				pruneDatasetMatches(exactMatches, datasetID);
			// if there is still more than one exact match,
			// then we don't know which one to pick
			if (prunedMatches.size() != 1)
				throw new IllegalStateException(String.format(
					"Dataset [%s] contains %d distinct files " +
					"with identical relative path [%s].",
					datasetID, exactMatches.size(), filePath));
			else return prunedMatches.iterator().next();
		// if there are no exact matches but one leaf match, return that
		} else if (leafMatches.size() == 1)
			return leafMatches.iterator().next();
		// if there is more than one remaining match, try to
		// prune by considering known processed collections
		else if (leafMatches.size() > 1) {
			Collection<File> prunedMatches =
				pruneDatasetMatches(leafMatches, datasetID);
			// if there is still more than one remaining match,
			// then we don't know which one to pick
			if (prunedMatches.size() != 1)
				throw new IllegalStateException(String.format(
					"Dataset [%s] contains %d distinct files " +
					"with identical filename [%s].",
					datasetID, leafMatches.size(), filename));
			else return prunedMatches.iterator().next();
		// if there are no matches at all, then it's just not there
		} else return null;
	}
	
	public static String getVerifiedSourceDescriptor(String path) {
		if (path == null)
			return null;
		else path = FilenameUtils.separatorsToUnix(path);
		// try to resolve the argument path to an actual
		// file on disk under the ProteoSAFe files root
		File file = null;
		if (path.startsWith(DATASET_FILES_ROOT))
			file = new File(path);
		else file = new File(DATASET_FILES_ROOT, path);
		// if the file was found, build and return its descriptor
		if (file.exists()) {
			String relativePath = FilenameUtils.separatorsToUnix(
				file.getAbsolutePath()).substring(DATASET_FILES_ROOT.length());
			// trim off leading slash, if present
			if (relativePath.startsWith("/"))
				relativePath = relativePath.substring(1);
			return String.format("f.%s", relativePath);
		}
		// if the file does not exist, then the
		// path is not a valid descriptor path
		else return null;
	}
	
	public static String getVerifiedDatasetDescriptor(
		String path, String datasetID
	) {
		if (path == null || datasetID == null)
			return null;
		// first make sure the path is a valid descriptor in the first place
		String descriptor = getVerifiedSourceDescriptor(path);
		if (descriptor == null)
			return null;
		// if so, strip off the descriptor prefix
		else if (descriptor.matches(FILE_DESCRIPTOR_PATTERN))
			descriptor = descriptor.substring(2);
		// if this file is present in the argument dataset, then the first
		// directory in the descriptor path should be the dataset ID
		if (descriptor.split(Pattern.quote("/"))[0].equals(datasetID))
			return descriptor;
		// if not, then even though the file exists, it's not in this dataset
		else return null;
	}
	
	public static String getVerifiedDatasetDescriptor(String path) {
		if (path == null)
			return null;
		// first make sure the path is a valid descriptor in the first place
		String descriptor = getVerifiedSourceDescriptor(path);
		if (descriptor == null)
			return null;
		// if so, strip off the descriptor prefix
		else if (descriptor.matches(FILE_DESCRIPTOR_PATTERN))
			descriptor = descriptor.substring(2);
		// if this file is present in some dataset, then the first
		// directory in the descriptor path should be a valid dataset ID
		if (descriptor.split(Pattern.quote("/"))[0].matches(DATASET_ID_PATTERN))
			return descriptor;
		// if not, then even though the file exists, it's not in any dataset
		else return null;
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	private static Collection<File> pruneMatches(
		Collection<File> files, String preferredPathExpression
	) {
		if (files == null || files.isEmpty() || preferredPathExpression == null)
			return files;
		// remove all files from the argument collection that
		// do not contain the argument directory in their path
		Collection<File> pruned = new LinkedHashSet<File>(files);
		for (File file : files)
			if (FilenameUtils.separatorsToUnix(file.getAbsolutePath())
				.matches(preferredPathExpression) == false)
				pruned.remove(file);
		return pruned;
	}
	
	private static Collection<File> pruneDatasetMatches(
		Collection<File> files, String datasetID
	) {
		if (files == null || files.isEmpty() || datasetID == null)
			return files;
		// first look for the file in top-level "ccms_result"
		String pruneExpression =
			String.format("^.*%s/ccms_result/.*$", datasetID);
		Collection<File> pruned = pruneMatches(files, pruneExpression);
		if (pruned != null && pruned.size() == 1)
			return pruned;
		// then look in "ccms_result" for updates
		pruneExpression =
			String.format("^.*%s/updates/[^/]+/ccms_result/.*$", datasetID);
		pruned = pruneMatches(files, pruneExpression);
		if (pruned != null && pruned.size() == 1)
			return pruned;
		// then look in "ccms_result" for reanalysis attachments
		pruneExpression =
			String.format("^.*%s/reanalyses/[^/]+/ccms_result/.*$", datasetID);
		pruned = pruneMatches(files, pruneExpression);
		if (pruned != null && pruned.size() == 1)
			return pruned;
		// then look in top-level "ccms_peak"
		pruneExpression = String.format("^.*%s/ccms_peak/.*$", datasetID);
		pruned = pruneMatches(files, pruneExpression);
		if (pruned != null && pruned.size() == 1)
			return pruned;
		// then look in top-level "peak"
		pruneExpression = String.format("^.*%s/peak/.*$", datasetID);
		pruned = pruneMatches(files, pruneExpression);
		if (pruned != null && pruned.size() == 1)
			return pruned;
		// then look in "ccms_peak" for updates
		pruneExpression =
			String.format("^.*%s/updates/[^/]+/ccms_peak/.*$", datasetID);
		pruned = pruneMatches(files, pruneExpression);
		if (pruned != null && pruned.size() == 1)
			return pruned;
		// then look in "peak" for updates
		pruneExpression =
			String.format("^.*%s/updates/[^/]+/peak/.*$", datasetID);
		pruned = pruneMatches(files, pruneExpression);
		if (pruned != null && pruned.size() == 1)
			return pruned;
		// then look in "peak" for reanalysis attachments
		pruneExpression =
			String.format("^.*%s/reanalyses/[^/]+/peak/.*$", datasetID);
		pruned = pruneMatches(files, pruneExpression);
		if (pruned != null && pruned.size() == 1)
			return pruned;
		// if none of these collections yielded a unique match, then
		// the filename collision is legitimately irreconcilable
		return files;
	}
}
