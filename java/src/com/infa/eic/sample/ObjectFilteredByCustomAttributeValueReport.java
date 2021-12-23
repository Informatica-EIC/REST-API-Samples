package com.infa.eic.sample;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

import com.infa.products.ldm.core.rest.v2.client.invoker.ApiException;
import com.infa.products.ldm.core.rest.v2.client.models.AttributeResponse;
import com.infa.products.ldm.core.rest.v2.client.models.AttributesResponse;
import com.infa.products.ldm.core.rest.v2.client.models.FactResponse;
import com.infa.products.ldm.core.rest.v2.client.models.LinkedObjectResponse;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectResponse;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectsResponse;
import com.opencsv.CSVWriter;

/**
 * 
 */

/**
 * This program uses the EIC REST API to copy values from one custom attribute
 * to the other.
 * 
 * @author lntrapad
 *
 */
public class ObjectFilteredByCustomAttributeValueReport {

	private static String srcCustomAttrName = "PartyType";
	private static String srcCustomAttrValue = "Legal Entity";

	public ObjectFilteredByCustomAttributeValueReport() {
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ObjectFilteredByCustomAttributeValueReport b = new ObjectFilteredByCustomAttributeValueReport();

		// Connect to the EIC REST Instance
		try {
			APIUtils.setupOnce();
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			String attrName = "";
			String attrValue = "";
			if (args.length == 2) {
				attrName = args[0];
				attrValue = args[1];
			} else {
				attrName = srcCustomAttrName;
				attrValue = srcCustomAttrValue;
			}

			String srcCustomAttrID = b.getCustomAttributeID(attrName);
			System.out.println("Source Attribute ID for " + srcCustomAttrName + " is " + srcCustomAttrID);

			HashMap<String, HashMap<String, String>> objMap = b.getObjectFilteredByCustomAttrValue(srcCustomAttrID,
					attrValue);

			String csv = "output1.csv";
			CSVWriter writer = null;

			writer = new CSVWriter(new FileWriter(csv));

			String header = "Object ID,Object Type,Object Name,Resource Name,Parent Object Name";
			writer.writeNext(header.split(","));

			for (String colId : objMap.keySet()) {
				HashMap<String, String> col = objMap.get(colId);
				writer.writeNext(new String[] { colId, col.get("objectType"), col.get("objectName"),
						col.get("resourceName"), col.get("parentObjectName") });
			}

			writer.close();

		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();

		}

	}

	public HashMap<String, HashMap<String, String>> getObjectFilteredByCustomAttrValue(String srcCustomAttributeID,
			String srcCustomAttributeValue) throws Exception {
		int total = 1000;
		int offset = 0;
		final int pageSize = 300;

		HashMap<String, HashMap<String, String>> retMap = new HashMap<String, HashMap<String, String>>();

		String query = srcCustomAttributeID + ":\"" + srcCustomAttributeValue + "\"";
		System.out.println("query syntax=" + query);

		while (offset < total) {
			// EDC (client.jar) <=10.2.1
			// ObjectsResponse response=APIUtils.READER.catalogDataObjectsGet(query, null,
			// BigDecimal.valueOf(offset), BigDecimal.valueOf(pageSize), false);
			// EDC (client.jar) 10.2.2 (+ 10.2.2 sp1)
			// ObjectsResponse response=APIUtils.READER.catalogDataObjectsGet(query, null,
			// offset, pageSize, null, null);
			// EDC (client.jar) 10.4+
			ObjectsResponse response = APIUtils.CATALOG_API.catalogDataObjectsGet(query, null, null, offset, pageSize,
					null, null, null, null, true, true, null, null);

			total = response.getMetadata().getTotalCount().intValue();
			offset += pageSize;

			for (ObjectResponse or : response.getItems()) {
				System.out.println(or.getId());
				retMap.put(or.getId(), this.getObjectFullDetails(or.getId()));

			}
			if (offset >= total)
				System.out.println(total + "/" + total);
			else
				System.out.println(offset + "/" + total);
		}

		return retMap;
	}

	public String getCustomAttributeID(String customAttributeName) throws Exception {
		int total = 1000;
		int offset = 0;
		final int pageSize = 300;

		String customAttributeId = new String();
		boolean dup = false;

		while (offset < total) {
			try {
				// EDC (client.jar) <=10.2.1
				// AttributesResponse
				// response=APIUtils.MODEL_READER.catalogModelsAttributesGet(null, null,
				// BigDecimal.valueOf(offset), BigDecimal.valueOf(pageSize));
				// EDC (client.jar) 10.2.2 (+ 10.2.2 sp1)
				// AttributesResponse response =
				// APIUtils.MODEL_READER.catalogModelsAttributesGet(null, null, offset,
				// pageSize);
				// EDC (client.jar) 10.4+
				AttributesResponse response = APIUtils.MODEL_READER.catalogModelsAttributesGet(null, null, null, null,
						offset, pageSize);
				total = response.getMetadata().getTotalCount().intValue();
				offset += pageSize;

				for (AttributeResponse ar : response.getItems()) {
					if (ar.getName().equals(customAttributeName)) {
						if (customAttributeId != null && !customAttributeId.equals(""))
							dup = true;
						customAttributeId = ar.getId();
					}
				}
			} catch (ApiException e) {
				e.printStackTrace();
			}
		}

		if (customAttributeId.equals("")) {
			throw new Exception("Custom Attribute ID not found");
		} else if (dup) {
			throw new Exception("Duplicate Attribute ID found");
		} else {
			return customAttributeId;
		}
	}

	private HashMap<String, String> getObjectFullDetails(String Id) throws Exception {

		ObjectResponse or = APIUtils.READER.catalogDataObjectsIdGet(Id);

		HashMap<String, String> hashtemp = new HashMap<String, String>();
		hashtemp.put("ID", or.getId());

		for (LinkedObjectResponse lor : or.getSrcLinks()) {
			if (lor.getAssociation().equals("com.infa.ldm.relational.TableColumn")) {
				hashtemp.put("parentObjectName", lor.getName());
				// HashMap<String,String> tMap = getObjectFullDetails(lor.getId());
				// hashtemp.put("schemaName",tMap.get("schemaName"));
			}
			if (lor.getAssociation().equals("com.infa.ldm.relational.SchemaTable")) {
				hashtemp.put("parentObjectName", lor.getName());
			}
		}

		for (FactResponse f : or.getFacts()) {
			if (f.getAttributeId().equals("core.resourceName"))
				hashtemp.put("resourceName", f.getValue());
			if (f.getAttributeId().equals("core.name"))
				hashtemp.put("objectName", f.getValue());
			if (f.getAttributeId().equals("core.classType"))
				hashtemp.put("objectType", f.getValue());
		}

		return hashtemp;
	}

}
