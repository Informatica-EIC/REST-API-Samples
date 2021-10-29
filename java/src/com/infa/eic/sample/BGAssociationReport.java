package com.infa.eic.sample;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.infa.products.ldm.core.rest.v2.client.invoker.ApiResponse;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectIdRequest;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectRefRequest;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectResponse;
import com.infa.products.ldm.core.rest.v2.client.utils.ObjectAdapter;
import com.opencsv.CSVWriter;

/**
 * This program uses the EIC REST API to generate a coverage report of BG terms
 * against specified resources. Using this program, data stewards can quickly
 * get a report on # of columns not associated with BG terms yet.
 * 
 * @author gpathak
 *
 */
public class BGAssociationReport {

	private static String DIVISION = "com.infa.appmodels.ldm.LDM_a5922c30_42eb_40ac_bb1e_75362b67ea9c";

	List<String> resources;
	List<String> attributes;
	List<String> types;

	/**
	 * Instantiate class with resource name list, attribute list and class type
	 * list.
	 * 
	 * @param resources
	 * @param attributes
	 * @param types
	 */
	public BGAssociationReport(List<String> resources, List<String> attributes, List<String> types) {
		this.resources = resources;
		this.attributes = attributes;
		this.types = types;
	}

	/**
	 * Get BG/Custom Attribute Associations for a given object
	 * 
	 * @param objectID
	 * @return
	 * @throws Exception
	 */
	public List<String> getAssociations(String objectID) throws Exception {
		ApiResponse<ObjectResponse> apiResponse = APIUtils.READER.catalogDataObjectsIdGetWithHttpInfo(objectID);
		ObjectIdRequest request = ObjectAdapter.INSTANCE.copyIntoObjectIdRequest(apiResponse.getData());
		// Get Associated BG Terms
		ArrayList<ObjectRefRequest> bgterms = request.getBusinessTerms();
		List<String> retList = new ArrayList<String>();
		retList.add(objectID);
		retList.add(APIUtils.getValue(apiResponse.getData(), "core.classType"));
		retList.add(apiResponse.getData().getHref());
		String cval;
		for (String customAttribute : attributes) {
			cval = APIUtils.getValue(apiResponse.getData(), customAttribute);
			if (cval != null) {
				retList.add(cval);
			} else {
				retList.add("");
			}
		}

		// If no BG term is associated add FALSE to the report
		if (bgterms == null || bgterms.isEmpty()) {
			retList.add("FALSE");
			retList.add("");

		} else {
			retList.add("TRUE");
			retList.add(request.getBusinessTerms().get(0).getId());

		}
		return retList;
	}

	public List<List<String>> run() throws Exception {
		List<List<String>> retList = new ArrayList<List<String>>();
		for (String resource : resources) {
			for (String type : types) {
				for (String objectID : APIUtils.getAssetsByType(resource, type).keySet()) {
					retList.add(getAssociations(objectID));
				}
			}
		}
		return retList;
	}

	public static void main(String args[]) {

		// Connect to the EIC REST Instance
		try {
			APIUtils.setupOnce();
		} catch (Exception e) {
			e.printStackTrace();
		}
		// List of resources
		List<String> resources = new ArrayList<String>();
		resources.add("Hive_Atlas");
		resources.add("ORACLE_CRM");
		resources.add("OrderEntry");

		// List of attributes (provide ids)
		List<String> attributes = new ArrayList<String>();
		attributes.add("core.resourceName");
		attributes.add(DIVISION);

		// List of asset types
		List<String> types = new ArrayList<String>();
		types.add("com.infa.ldm.relational.Column");

		// Path to the output report
		String csv = "output.csv";
		CSVWriter writer = null;
		try {
			writer = new CSVWriter(new FileWriter(csv));
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		String header = "ID,TYPE,LINK,";
		for (String attribute : attributes) {
			header += attribute + ",";
		}
		header += "BG_ASSOCIATED, BG TERM ID";

		writer.writeNext(header.split(","));

		BGAssociationReport rep = new BGAssociationReport(resources, attributes, types);
		try {
			for (List<String> l : rep.run()) {
				writer.writeNext(l.toArray(new String[l.size()]));
				System.out.println();
				for (String s : l) {
					System.out.print(s + ",");
				}
			}
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
