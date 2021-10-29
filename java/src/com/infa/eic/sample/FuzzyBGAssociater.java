/**
 * 
 */
package com.infa.eic.sample;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import com.infa.products.ldm.core.rest.v2.client.invoker.ApiResponse;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectIdRequest;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectRefRequest;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectResponse;
import com.infa.products.ldm.core.rest.v2.client.utils.ObjectAdapter;

import me.xdrop.fuzzywuzzy.FuzzySearch;
import me.xdrop.fuzzywuzzy.model.ExtractedResult;

/**
 * Sample REST API Program that associates data assets with business glossary
 * terms based on fuzzy name matches.
 * 
 * @author gpathak
 *
 */
public class FuzzyBGAssociater {

	/**
	 * Thresholds go from 1-100 where 100 stands for exact match.
	 */
	private static int THRESHOLD = 80;

	private static String BG_RESOURCE_NAME = "BG_DEFAULT_RESOURCE";
	private static String RESOURCE = "OrderEntry";

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		FuzzyBGAssociater fbg = new FuzzyBGAssociater();

		// Connect to the EIC REST Instance
		try {
			APIUtils.setupOnce();
		} catch (Exception e) {
			throw e;
		}
		try {

			// Get all business terms
			HashMap<String, String> termMap = APIUtils.getAssetsByType(BG_RESOURCE_NAME, APIUtils.BGTERM);
			// Get all Columns from the specified resource
			HashMap<String, String> columnMap = APIUtils.getAssetsByType(RESOURCE, APIUtils.COL_CLASSTYPE);
			System.out.println(termMap.size() + ":" + columnMap.size());

			int i = 1;
			for (String columnID : columnMap.keySet()) {
				// Remove all _ for a better fuzzy match
				String colName = columnMap.get(columnID).replaceAll("_", " ");

				// List<ExtractedResult> results= FuzzySearch.extractAll(colName,
				// termMap.values(), THRESHOLD);
				List<ExtractedResult> results = FuzzySearch.extractSorted(colName, termMap.values(), THRESHOLD);

				if (!results.isEmpty()) {
					// System.out.println(columnName+":"+term);
					System.out.println(i++ + ":" + colName + ":" + results.get(0).getString());
					String termID = getKeyByValue(termMap, results.get(0).getString());
					// Perform BG Term Associations
					fbg.associateBGTerm(columnID, termID);
					// Use the method below to remove BG terms from a given column
					// fbg.resetTerms(columnID);
				}
			}

			// fbg.associateBGTerm("Hive_Atlas://Hive
			// Metastore/default/exmaple_2/displacement",
			// "BG_DEFAULT_RESOURCE://BG/Term/413134508838413");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Utility method to get hash key from value. Works best with unique values.
	 * 
	 * @param map
	 * @param value
	 * @return
	 */
	public static <T, E> T getKeyByValue(Map<T, E> map, E value) {
		for (Entry<T, E> entry : map.entrySet()) {
			if (Objects.equals(value, entry.getValue())) {
				return entry.getKey();
			}
		}
		return null;
	}

	/**
	 * Associates a bg term to a given object.
	 * 
	 * @param objectID
	 * @param bgTermID
	 * @throws Exception
	 */
	public void associateBGTerm(String objectID, String bgTermID) throws Exception {
		System.out.println(objectID + ":" + bgTermID);
		ApiResponse<ObjectResponse> apiResponse = APIUtils.READER.catalogDataObjectsIdGetWithHttpInfo(objectID);
		ObjectIdRequest request = ObjectAdapter.INSTANCE.copyIntoObjectIdRequest(apiResponse.getData());
		ArrayList<ObjectRefRequest> bgterms = request.getBusinessTerms();
		if (bgterms == null || bgterms.isEmpty()) {
			ObjectRefRequest bg = new ObjectRefRequest();
			bg.setId(bgTermID);
			request.addBusinessTermsItem(bg);
			String ifMatch = APIUtils.READER.catalogDataObjectsIdGetWithHttpInfo(objectID).getHeaders().get("ETag")
					.get(0);
			System.out.println(ifMatch);
			APIUtils.WRITER.catalogDataObjectsIdPut(objectID, request, ifMatch);
		} else {
			System.out.println("Existing bg term");
		}
	}

	/**
	 * Removes BG term association from a given object
	 * 
	 * @param objectID
	 * @throws Exception
	 */
	public void resetTerms(String objectID) throws Exception {
		ApiResponse<ObjectResponse> apiResponse = APIUtils.READER.catalogDataObjectsIdGetWithHttpInfo(objectID);
		ObjectIdRequest request = ObjectAdapter.INSTANCE.copyIntoObjectIdRequest(apiResponse.getData());

		ArrayList<ObjectRefRequest> bgterms = request.getBusinessTerms();
		if (bgterms != null && !bgterms.isEmpty()) {
			request.setBusinessTerms(new ArrayList<ObjectRefRequest>());
			// ObjectRefRequest bg=new ObjectRefRequest();
			// bg.setId(bgTermID);
			// request.addBusinessTermsItem(bg);
			String ifMatch = APIUtils.READER.catalogDataObjectsIdGetWithHttpInfo(objectID).getHeaders().get("ETag")
					.get(0);
			// System.out.println(ifMatch);
			System.out.println(request.toString());

			APIUtils.WRITER.catalogDataObjectsIdPut(objectID, request, ifMatch);
		} else {
			System.out.println("BG Term Not Found");
		}
	}

}
