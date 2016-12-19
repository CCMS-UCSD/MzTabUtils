package edu.ucsd.mztab.model;

import java.io.File;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.xpath.XPathAPI;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import edu.ucsd.mztab.exceptions.UnverifiableNativeIDException;
import edu.ucsd.mztab.util.FileIOUtils;

public class MzIdentMLNativeIDMap
{
	/*========================================================================
	 * Properties
	 *========================================================================*/
	private File                  uploadedResultDirectory;
	private Map<String, Document> parsedMzidFileCache;
	private Map<Document, Map<String, Map<String, Collection<String>>>>
		mzidSpectrumIDCache;
	
	/*========================================================================
	 * Constructor
	 *========================================================================*/
	public MzIdentMLNativeIDMap(File uploadedResultDirectory) {
		// validate uploaded result directory (can be null, although if so, an
		// UnverifiableNativeIDException will be thrown by the isScan method)
		if (uploadedResultDirectory != null) {
			if (uploadedResultDirectory.isDirectory() == false)
				throw new IllegalArgumentException(String.format(
					"Uploaded result files directory [%s] must be a " +
					"directory.", uploadedResultDirectory.getAbsolutePath()));
			else if (uploadedResultDirectory.canRead() == false)
				throw new IllegalArgumentException(String.format(
					"Uploaded result files directory [%s] must be " +
					"readable.", uploadedResultDirectory.getAbsolutePath()));
		}
		this.uploadedResultDirectory = uploadedResultDirectory;
		// initialize parsed mzid caches
		parsedMzidFileCache = new LinkedHashMap<String, Document>();
		mzidSpectrumIDCache = new LinkedHashMap<
			Document, Map<String, Map<String, Collection<String>>>>();
	}
	
	/*========================================================================
	 * Public interface methods
	 *========================================================================*/
	public boolean isScan(
		MzTabFile mzTabFile, String sequence, int id
	) throws UnverifiableNativeIDException {
		if (mzTabFile == null)
			throw new NullPointerException("MzTab file is null.");
		else if (sequence == null)
			throw new NullPointerException("\"sequence\" string is null.");
		else if (uploadedResultDirectory == null ||
			uploadedResultDirectory.canRead() == false)
			throw new UnverifiableNativeIDException(String.format(
				"Invalid NativeID-formatted spectrum identifier [%d]: no " +
				"submitted result file could be found to verify whether " +
				"this identifier represents an index or scan number.", id));
		// get mangled result filename
		String mangledResultFilename = mzTabFile.getMangledResultFilename();
		// get original mzid file that this mzTab was converted from, if any
		File mzidFile = getUploadedMzIdentMLFile(
			mangledResultFilename, uploadedResultDirectory);
		// if this mzTab file was not converted from an mzid file, then there's
		// no source to look up; its nativeIDs are just inherently bad
		if (mzidFile == null || mzidFile.canRead() == false)
			throw new UnverifiableNativeIDException(String.format(
				"Invalid NativeID-formatted spectrum identifier [%d]: no " +
				"submitted mzIdentML file could be found to verify whether " +
				"this identifier represents an index or scan number.", id));
		// check cache for parsed mzid document, to avoid redundant parsing
		Document mzidDocument = parsedMzidFileCache.get(mzidFile.getName());
		// if no cached copy was found, parse the file
		if (mzidDocument == null) try {
			mzidDocument = FileIOUtils.parseXML(mzidFile);
			parsedMzidFileCache.put(mzidFile.getName(), mzidDocument);
		} catch (Throwable error) {
			throw new UnverifiableNativeIDException(String.format(
				"Invalid NativeID-formatted spectrum identifier [%d]: " +
				"submitted mzIdentML file [%s] could not be parsed to verify " +
				"whether this identifier represents an index or scan number.",
				id, mzidFile.getName()), error);
		}
		// check cache for processed mzid document,
		// to avoid redundant XML processing
		Map<String, Map<String, Collection<String>>> mzidMap =
			mzidSpectrumIDCache.get(mzidDocument);
		if (mzidMap == null) {
			mzidMap =
				new LinkedHashMap<String, Map<String, Collection<String>>>();
			mzidSpectrumIDCache.put(mzidDocument, mzidMap);
		}
		// look for the nativeID in the processed map
		String nativeID = getNativeIDFromMap(sequence, id, mzidMap);
		// if not found in the map, look it up in the document
		if (nativeID == null) {
			boolean found = false;
			NodeList peptides = getPeptideNodes(mzidDocument, sequence);
			if (peptides == null)
				throw new UnverifiableNativeIDException(String.format(
					"Invalid NativeID-formatted spectrum identifier [%d]: " +
					"could not find an entry for peptide sequence [%s] in " +
					"submitted mzIdentML file [%s] to verify whether this " +
					"identifier represents an index or scan number.",
					id, sequence, mzidFile.getName()));
			else for (int i=0; i<peptides.getLength(); i++) {
				Node peptide = peptides.item(i);
				NodeList spectrumIDs = null;
				String peptideRef = null;
				try {
					peptideRef = peptide.getAttributes()
						.getNamedItem("id").getNodeValue();
				} catch (Throwable error) {}
				spectrumIDs = getSpectrumIDNodes(mzidDocument, peptideRef);
				if (spectrumIDs == null || peptideRef == null)
					continue;
				for (int j=0; j<spectrumIDs.getLength(); j++) {
					Node spectrumID = spectrumIDs.item(j);
					try {
						nativeID = spectrumID.getAttributes()
							.getNamedItem("spectrumID").getNodeValue();
					} catch (Throwable error) {}
					if (nativeID == null)
						continue;
					else {
						// add found nativeID to cache
						addNativeIDToMap(
							sequence, peptideRef, nativeID, mzidMap);
//
System.out.print("?");
//
						// if this nativeID matches, stop looking
						if (nativeID.equals(String.format("scan=%d", id)) ||
							nativeID.equals(
								String.format("index=%d", id - 1))) {
							found = true;
							break;
						}
					}
				}
				if (found)
					break;
			}
		}
//
else System.out.print("!");
//
		// once all steps have been taken to find a matching nativeID,
		// process whatever was found appropriately
		if (nativeID == null)
			throw new UnverifiableNativeIDException(String.format(
				"Invalid NativeID-formatted spectrum identifier [%d]: " +
				"no evidence could be found in submitted mzIdentML file [%s] " +
				"to verify whether this identifier represents an index or " +
				"scan number.", id, mzidFile.getName()));
		else if (nativeID.equals(String.format("scan=%d", id)))
			return true;
		// need to decrement the ID printed in the mzTab file by
		// 1 when comparing against mzid index IDs, since
		// apparently jmzTab increments it during the conversion
		else if (nativeID.equals(String.format("index=%d", id - 1)))
			return false;
		else throw new UnverifiableNativeIDException(String.format(
			"Invalid NativeID-formatted spectrum identifier [%d]: " +
			"found spectrum ID [%s] in submitted mzIdentML file [%s], " +
			"but the NativeID format is not recognized.",
			id, nativeID, mzidFile.getName()));
	}
	
	/*========================================================================
	 * Convenience methods
	 *========================================================================*/
	@SuppressWarnings("unused")
	private Map<String, Map<String, Collection<String>>> mapMzidDocument(
		Document mzidDocument
	) {
		if (mzidDocument == null)
			return null;
		// build map of sequence -> peptide_ref
		Map<String, Collection<String>> peptideMap =
			new LinkedHashMap<String, Collection<String>>();
		// build map of peptide_ref -> nativeID
		Map<String, Collection<String>> spectrumMap =
			new LinkedHashMap<String, Collection<String>>();
		// first look at peptide_refs and map them to their sequences
		NodeList peptides = null;
		try {
			peptides = XPathAPI.selectNodeList(mzidDocument, "//Peptide");
		} catch (Throwable error) {}
		if (peptides == null || peptides.getLength() < 1)
			return null;
		for (int i=0; i<peptides.getLength(); i++) {
			Node peptide = peptides.item(i);
			String peptideRef = null;
			try {
				peptideRef =
					peptide.getAttributes().getNamedItem("id").getNodeValue();
			} catch (Throwable error) {}
			if (peptideRef == null)
				continue;
			NodeList peptideSequences = null;
			try {
				peptideSequences =
					XPathAPI.selectNodeList(peptide, "PeptideSequence");
			} catch (Throwable error) {}
			if (peptideSequences == null || peptideSequences.getLength() < 1)
				continue;
			for (int j=0; j<peptideSequences.getLength(); j++) {
				Node peptideSequence = peptideSequences.item(j);
				String sequence = null;
				try {
					sequence = peptideSequence.getTextContent().trim();
				} catch (Throwable error) {}
				if (sequence == null)
					continue;
				Collection<String> peptideRefs = peptideMap.get(sequence);
				if (peptideRefs == null) {
					peptideRefs = new LinkedHashSet<String>();
					peptideMap.put(sequence, peptideRefs);
				}
				peptideRefs.add(peptideRef);
			}
		}
		// then look at spectrum IDs and map their nativeIDs to peptide_refs
		NodeList spectrumIDs = null;
		try {
			spectrumIDs = XPathAPI.selectNodeList(
				mzidDocument, "//SpectrumIdentificationResult");
		} catch (Throwable error) {}
		if (spectrumIDs == null || spectrumIDs.getLength() < 1)
			return null;
		for (int i=0; i<spectrumIDs.getLength(); i++) {
			Node spectrumID = spectrumIDs.item(i);
			String nativeID = null;
			try {
				nativeID = spectrumID.getAttributes()
					.getNamedItem("spectrumID").getNodeValue();
			} catch (Throwable error) {}
			if (nativeID == null)
				continue;
			NodeList spectrumIDItems = null;
			try {
				spectrumIDItems = XPathAPI.selectNodeList(
					spectrumID, "SpectrumIdentificationItem");
			} catch (Throwable error) {}
			if (spectrumIDItems == null || spectrumIDItems.getLength() < 1)
				continue;
			for (int j=0; j<spectrumIDItems.getLength(); j++) {
				Node spectrumIDItem = spectrumIDItems.item(j);
				String peptideRef = null;
				try {
					peptideRef = spectrumIDItem.getAttributes()
						.getNamedItem("peptide_ref").getNodeValue();
				} catch (Throwable error) {}
				if (peptideRef == null)
					continue;
				Collection<String> nativeIDs = spectrumMap.get(peptideRef);
				if (nativeIDs == null) {
					nativeIDs = new LinkedHashSet<String>();
					spectrumMap.put(peptideRef, nativeIDs);
				}
				nativeIDs.add(nativeID);
			}
		}
		// finally, combine the maps
		Map<String, Map<String, Collection<String>>> outerMap =
			new LinkedHashMap<String, Map<String, Collection<String>>>();
		for (String sequence : peptideMap.keySet()) {
			// make sure an inner map exists for this sequence
			Map<String, Collection<String>> innerMap = outerMap.get(sequence);
			if (innerMap == null) {
				innerMap = new LinkedHashMap<String, Collection<String>>();
				outerMap.put(sequence, innerMap);
			}
			// gather all peptide_refs for this sequence and
			// move their nativeIDs into the inner map
			Collection<String> peptideRefs = peptideMap.get(sequence);
			for (String peptideRef : peptideRefs) {
				Collection<String> nativeIDs = spectrumMap.get(peptideRef);
				if (nativeIDs == null)
					continue;
				// make sure an inner collection of nativeIDs
				// exists for this peptide_ref
				Collection<String> innerNativeIDs = innerMap.get(peptideRef);
				if (innerNativeIDs == null) {
					innerNativeIDs = new LinkedHashSet<String>();
					innerMap.put(peptideRef, innerNativeIDs);
				}
				innerNativeIDs.addAll(nativeIDs);
			}
		}
		return outerMap;
	}
	
	private NodeList getPeptideNodes(
		Document mzidDocument, String sequence
	) {
		if (mzidDocument == null || sequence == null)
			return null;
		try {
			return XPathAPI.selectNodeList(mzidDocument, String.format(
				"//Peptide[PeptideSequence[text()='%s']]", sequence));
		} catch (Throwable error) {
			return null;
		}
	}
	
	private NodeList getSpectrumIDNodes(
		Document mzidDocument, String peptideRef
	) {
		if (mzidDocument == null || peptideRef == null)
			return null;
		try {
			return XPathAPI.selectNodeList(mzidDocument,
				String.format("//SpectrumIdentificationResult[" +
				"SpectrumIdentificationItem[@peptide_ref='%s']]",
				peptideRef));
		} catch (Throwable error) {
			return null;
		}
	}
	
	private String getNativeIDFromMap(
		String sequence, int id,
		Map<String, Map<String, Collection<String>>> mzidMap
	) {
		if (sequence == null || mzidMap == null || mzidMap.isEmpty())
			return null;
		Map<String, Collection<String>> peptideRefs = mzidMap.get(sequence);
		if (peptideRefs == null || peptideRefs.isEmpty())
			return null;
		for (String peptideRef : peptideRefs.keySet()) {
			Collection<String> nativeIDs = peptideRefs.get(peptideRef);
			if (nativeIDs == null || nativeIDs.isEmpty())
				continue;
			for (String nativeID : nativeIDs)
				if (nativeID.equals(String.format("scan=%d", id)) ||
					nativeID.equals(String.format("index=%d", id - 1)))
					return nativeID;
		}
		return null;
	}
	
	private void addNativeIDToMap(
		String sequence, String peptideRef, String nativeID,
		Map<String, Map<String, Collection<String>>> mzidMap
	) {
		if (sequence == null || peptideRef == null || nativeID == null ||
			mzidMap == null)
			return;
		Map<String, Collection<String>> peptideRefs = mzidMap.get(sequence);
		if (peptideRefs == null) {
			peptideRefs = new LinkedHashMap<String, Collection<String>>();
			mzidMap.put(sequence, peptideRefs);
		}
		Collection<String> nativeIDs = peptideRefs.get(peptideRef);
		if (nativeIDs == null) {
			nativeIDs = new LinkedHashSet<String>();
			peptideRefs.put(peptideRef, nativeIDs);
		}
		nativeIDs.add(nativeID);
	}
	
	private File getUploadedMzIdentMLFile(
		String mzTabFilename, File uploadedResultDirectory
	) {
		if (mzTabFilename == null || uploadedResultDirectory == null ||
			uploadedResultDirectory.canRead() == false)
			return null;
		File mzidFile = new File(uploadedResultDirectory, String.format(
			"%s.%s", FilenameUtils.getBaseName(mzTabFilename), "mzid"));
		if (mzidFile.canRead() == false)
			return null;
		else return mzidFile;
	}
}
