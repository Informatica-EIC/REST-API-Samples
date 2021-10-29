package com.infa.eic.sample;

import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.infa.products.ldm.core.rest.v2.client.models.FactResponse;
import com.infa.products.ldm.core.rest.v2.client.models.Link;
import com.infa.products.ldm.core.rest.v2.client.models.LinkedObjectResponse;
import com.infa.products.ldm.core.rest.v2.client.models.Links;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectResponse;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectsResponse;
import com.opencsv.CSVWriter;

public class UltimateColumnLineageReport {
	private static String DIVISION = "com.infa.appmodels.ldm.LDM_a5922c30_42eb_40ac_bb1e_75362b67ea9c";

	List<String> attributes;

	public UltimateColumnLineageReport(List<String> attributes) {
		this.attributes = attributes;
	}

	public UltimateColumnLineageReport() {
		this.attributes = null;
	}

	private HashMap<String, String> getObjectFullDetails(String Id) throws Exception {

		ObjectResponse or = APIUtils.READER.catalogDataObjectsIdGet(Id);

		HashMap<String, String> hashtemp = new HashMap<String, String>();
		hashtemp.put("ID", or.getId());
		// listId.add(or.getId());

		for (LinkedObjectResponse lor : or.getSrcLinks()) {
			if (lor.getAssociation().equals("com.infa.ldm.relational.TableColumn")) {
				hashtemp.put("tableName", lor.getName());
				HashMap<String, String> tMap = getObjectFullDetails(lor.getId());
				hashtemp.put("schemaName", tMap.get("schemaName"));

			}
			if (lor.getAssociation().equals("com.infa.ldm.relational.SchemaTable")) {
				hashtemp.put("schemaName", lor.getName());
			}
		}
		for (FactResponse f : or.getFacts()) {
			if (f.getAttributeId().equals("core.resourceName"))
				hashtemp.put("resourceName", f.getValue());
			if (f.getAttributeId().equals("core.name"))
				hashtemp.put("columnName", f.getValue());
		}

		return hashtemp;
		// listObj.add(hashtemp);

	}

	private HashMap<String, HashMap<String, String>> getAllColumns() throws Exception {
		int total = 3000;
		int offset = 0;
		final int pageSize = 300;

		String query = "* AND core.allclassTypes:\"" + APIUtils.COL_CLASSTYPE + "\"";
		HashMap<String, HashMap<String, String>> retMap = new HashMap<String, HashMap<String, String>>();
		HashMap<String, HashMap<String, String>> listObj = new HashMap<String, HashMap<String, String>>();
		String lastParent;

		// get all table/Schema
		String query2 = "* AND core.allclassTypes:\"" + APIUtils.TABLE_CLASSTYPE + "\"";
		HashMap<String, String> tMap = new HashMap<String, String>();
		while (offset < total) {
			// EDC (client.jar) <=10.2.1
			// ObjectsResponse tableResponse=APIUtils.READER.catalogDataObjectsGet(query2,
			// null, BigDecimal.valueOf(offset), BigDecimal.valueOf(pageSize), false);
			// EDC (client.jar) 10.2.2 (+ 10.2.2 sp1)
			// ObjectsResponse tableResponse = APIUtils.READER.catalogDataObjectsGet(query2,
			// null, offset, pageSize, null,
			// null);
			// EDC (client.jar) 10.4+
			ObjectsResponse tableResponse = APIUtils.CATALOG_API.catalogDataObjectsGet(query2, null, null, offset,
					pageSize, null, null, null, null, true, true, null, null);
			offset += pageSize;

			for (ObjectResponse tor : tableResponse.getItems()) {
				for (LinkedObjectResponse lor : tor.getSrcLinks()) {
					if (lor.getAssociation().equals("com.infa.ldm.relational.SchemaTable")) {
						tMap.put(tor.getId(), lor.getName());
					}
				}
			}
		}

		// get all column
		total = 3000;
		offset = 0;
		while (offset < total) {
			// EDC (client.jar) <=10.2.1
			// ObjectsResponse response=APIUtils.READER.catalogDataObjectsGet(query, null,
			// BigDecimal.valueOf(offset), BigDecimal.valueOf(pageSize), false);
			// EDC (client.jar) 10.2.2 (+ 10.2.2 sp1)
			// ObjectsResponse response = APIUtils.READER.catalogDataObjectsGet(query, null,
			// offset, pageSize, null, null);
			// EDC (client.jar) 10.4+
			ObjectsResponse response = APIUtils.CATALOG_API.catalogDataObjectsGet(query, null, null, offset, pageSize,
					null, null, null, null, true, true, null, null);

			total = response.getMetadata().getTotalCount().intValue();

			if (offset == 0)
				System.out.println("Total object # :" + total);
			offset += pageSize;

			ArrayList<String> listId = new ArrayList<String>();
			for (ObjectResponse or : response.getItems()) {
				HashMap<String, String> hashtemp = new HashMap<String, String>();
				hashtemp.put("ID", or.getId());
				listId.add(or.getId());

				for (LinkedObjectResponse lor : or.getSrcLinks()) {
					if (lor.getAssociation().equals("com.infa.ldm.relational.TableColumn")) {
						hashtemp.put("tableId", lor.getId());
						hashtemp.put("tableName", lor.getName());
						// HashMap<String,String> tMap = getObjectFullDetails(lor.getId());
						hashtemp.put("schemaName", tMap.get(lor.getId()));
					}
				}
				for (FactResponse f : or.getFacts()) {
					if (f.getAttributeId().equals("core.resourceName"))
						hashtemp.put("resourceName", f.getValue());
					if (f.getAttributeId().equals("core.name"))
						hashtemp.put("columnName", f.getValue());
				}

				listObj.put(or.getId(), hashtemp);
			}

			ArrayList<String> b = new ArrayList<String>();
			b.add("core.DataFlow");

			// APIUtils.READER.catalogDataRelationshipsGetWithHttpInfo(seed, association,
			// depth, direction, removeDuplicateAggregateLinks, includeTerms,
			// includeAttribute)
			Links lr = APIUtils.READER.catalogDataRelationshipsGet(listId, b, BigDecimal.valueOf(0), "IN", true, null,
					null, null);

			HashMap<String, String> tempMap = new HashMap<String, String>();
			for (Link l : lr.getItems()) {
				if (!l.getAssociationId().equals("core.SynonymDataElementDataFlow")) {
					tempMap.put(l.getInId(), l.getOutId());
				}
			}

			for (int i = 0; i < listId.size(); i++) {

				String parent = tempMap.get(listId.get(i));
				lastParent = "";

				while (parent != null) {
					lastParent = parent;
					parent = tempMap.get(parent);
				}

				if (!lastParent.equals("")) {
					listObj.get(listId.get(i)).put("parentColumn", lastParent);
					// retMap.put(listObj.get(i));
					// System.out.println(listId.get(i) + " : " + lastParent);
				}

				// System.out.println(listId.get(i));
			}

			System.out.println("Progress : " + offset);

		}
		return listObj;
	}

	private String getTableName(String columnID) {
		return columnID.substring(0, columnID.lastIndexOf("/"));
	}

	public void run() throws Exception {

		HashMap<String, HashMap<String, String>> cd = getAllColumns();

		String csv = "output1.csv";
		CSVWriter writer = null;
		try {
			writer = new CSVWriter(new FileWriter(csv));
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		String header = "Resource Name, Schema Name, Table Name, Column Name,Parent Resource Name,Parent Schema Name,Parent Table Name,Parent Column Name";
		//

		writer.writeNext(header.split(","));

		for (String colId : cd.keySet()) {
			// HashMap<String, String> col=getObjectFullDetails(cd.get(i).get("ID"));
			HashMap<String, String> col = cd.get(colId);
			if (cd.get(colId).get("parentColumn") != null && !cd.get(colId).get("parentColumn").equals("")) {
				HashMap<String, String> parentCol = cd.get((cd.get(colId).get("parentColumn")));
				writer.writeNext(new String[] { col.get("resourceName"), col.get("schemaName"), col.get("tableName"),
						col.get("columnName"), parentCol.get("resourceName"), parentCol.get("schemaName"),
						parentCol.get("tableName"), parentCol.get("columnName") });
			}
		}

		try {
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void main(String args[]) {

		List<String> attributes = new ArrayList<String>();
		attributes.add("core.resourceName");
		// attributes.add(DIVISION);

		try {
			APIUtils.setupOnce();
		} catch (Exception e1) {
			e1.printStackTrace();
		}

		UltimateColumnLineageReport cdar = new UltimateColumnLineageReport();
		try {
			cdar.run();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}