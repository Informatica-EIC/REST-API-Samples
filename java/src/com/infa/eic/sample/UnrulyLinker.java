/**
 * 
 */
package com.infa.eic.sample;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import com.infa.products.ldm.core.rest.v2.client.invoker.ApiException;
import com.infa.products.ldm.core.rest.v2.client.invoker.ApiResponse;
import com.infa.products.ldm.core.rest.v2.client.models.LinkedObjectRequest;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectIdRequest;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectResponse;
import com.infa.products.ldm.core.rest.v2.client.utils.ObjectAdapter;

/**
 * This program uses the EIC REST API to add lineage links between tables of two
 * resources having same names.
 * 
 * @author gpathak
 *
 */
public class UnrulyLinker {

	String source; // Name of the resource which is the source
	String target;// Name of the resource which is the target

	/**
	 * @param source
	 * @param target
	 * @param types
	 */
	public UnrulyLinker(String source, String target) {
		this.source = source;
		this.target = target;
	}

	public void run() throws Exception {
		// Get table names from source
		HashMap<String, String> sourceTableMap = APIUtils.getAssetsByType(source, APIUtils.TABLE_CLASSTYPE);
		// Get table names from target
		HashMap<String, String> targetTableMap = APIUtils.getAssetsByType(target, APIUtils.TABLE_CLASSTYPE);

		// Find matching tables names in source and target
		for (String sourceTableID : sourceTableMap.keySet()) {
			// List<ExtractedResult> results= FuzzySearch.extractAll(colName,
			// termMap.values(), THRESHOLD);

			// To lowercase is a hack to match strings.
			String targetTableObjectID = getKeyByValue(targetTableMap, sourceTableMap.get(sourceTableID).toLowerCase());

			if (targetTableObjectID != null) {
				System.out.println(sourceTableID + ":" + targetTableObjectID);
				// For matching names add lineage link
				addDatasetLink(sourceTableID, targetTableObjectID);

				// Use this to remove lineage links
				// removeDatasetLink(sourceTableID, targetTableObjectID);
			}

		}
	}

	/**
	 * Add a TABLE lineage link between the given two objects
	 * 
	 * @param sourceDatasetObjectID
	 * @param targetDatasetObjectID
	 * @throws Exception
	 */
	public void addDatasetLink(String sourceDatasetObjectID, String targetDatasetObjectID) throws Exception {
		ApiResponse<ObjectResponse> apiResponse = APIUtils.READER
				.catalogDataObjectsIdGetWithHttpInfo(targetDatasetObjectID);
		ObjectIdRequest request = ObjectAdapter.INSTANCE.copyIntoObjectIdRequest(apiResponse.getData());

		LinkedObjectRequest link = new LinkedObjectRequest();
		link.setAssociation(APIUtils.DATASET_FLOW);
		link.setId(sourceDatasetObjectID);

		request.addSrcLinksItem(link);

		String ifMatch;
		try {
			ifMatch = APIUtils.READER.catalogDataObjectsIdGetWithHttpInfo(targetDatasetObjectID).getHeaders()
					.get("ETag").get(0);

			ObjectResponse newor = APIUtils.WRITER.catalogDataObjectsIdPut(targetDatasetObjectID, request, ifMatch);
			System.out.println(
					"Link Added between:" + sourceDatasetObjectID + " AND " + targetDatasetObjectID + " " + newor);
		} catch (ApiException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Remove table lineage links between two objects
	 * 
	 * @param sourceDatasetObjectID
	 * @param targetDatasetObjectID
	 * @throws Exception
	 */
	public void removeDatasetLink(String sourceDatasetObjectID, String targetDatasetObjectID) throws Exception {
		ApiResponse<ObjectResponse> apiResponse = APIUtils.READER
				.catalogDataObjectsIdGetWithHttpInfo(targetDatasetObjectID);
		ObjectIdRequest request = ObjectAdapter.INSTANCE.copyIntoObjectIdRequest(apiResponse.getData());

		int index = 0;
		int remIndex = 0;
		for (LinkedObjectRequest link : request.getSrcLinks()) {
			if (link.getId().equals(sourceDatasetObjectID)) {
				remIndex = index;
			}
			index++;
		}
		request.getSrcLinks().remove(remIndex);
		String ifMatch;
		try {
			ifMatch = APIUtils.READER.catalogDataObjectsIdGetWithHttpInfo(targetDatasetObjectID).getHeaders()
					.get("ETag").get(0);

			ObjectResponse newor = APIUtils.WRITER.catalogDataObjectsIdPut(targetDatasetObjectID, request, ifMatch);
			System.out.println("Link Removed between:" + sourceDatasetObjectID + " AND " + targetDatasetObjectID);
		} catch (ApiException e) {
			e.printStackTrace();
		}

	}

	public static <T, E> T getKeyByValue(Map<T, E> map, E value) {
		for (Entry<T, E> entry : map.entrySet()) {
			if (Objects.equals(value, entry.getValue())) {
				return entry.getKey();
			}
		}
		return null;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		UnrulyLinker linker = new UnrulyLinker("ORACLE_API_SOURCE", "Hive_Demo");
		// Connect to the EIC REST Instance
		try {
			APIUtils.setupOnce();
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			linker.run();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
