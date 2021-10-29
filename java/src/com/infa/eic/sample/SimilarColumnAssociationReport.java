package com.infa.eic.sample;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import com.infa.products.ldm.core.rest.v2.client.models.LinkedObjectResponse;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectResponse;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectsResponse;
import com.opencsv.CSVWriter;

public class SimilarColumnAssociationReport {
	private static String DIVISION = "com.infa.appmodels.ldm.LDM_a5922c30_42eb_40ac_bb1e_75362b67ea9c";

	List<String> attributes;

	public SimilarColumnAssociationReport(List<String> attributes) {
		this.attributes = attributes;
	}

	public SimilarColumnAssociationReport() {
		this.attributes = null;
	}

	@SuppressWarnings("deprecation")
	private HashMap<String, HashSet<String>> getAllColumns() throws Exception {
		int total = 3000;
		int offset = 0;
		final int pageSize = 300;

		String query = "* AND core.allclassTypes:\"" + APIUtils.COL_CLASSTYPE + "\"";
		HashMap<String, HashSet<String>> retMap = new HashMap<String, HashSet<String>>();

		while (offset < total) {
			// EDC (client.jar) <=10.2.1
			// ObjectsResponse response=APIUtils.READER.catalogDataObjectsGet(query, null,
			// BigDecimal.valueOf(offset), BigDecimal.valueOf(pageSize), false);
			// EDC (client.jar) 10.2.2 (+ 10.2.2 sp1)
			ObjectsResponse response = APIUtils.READER.catalogDataObjectsGet(query, null, offset, pageSize, null, null);
			total = response.getMetadata().getTotalCount().intValue();
			offset += pageSize;

			for (ObjectResponse or : response.getItems()) {
				System.out.println("Similar Columns for:" + or.getId());
				System.out.println("+++++++++++++++++++++++++++++++++");

				for (LinkedObjectResponse lr : or.getSrcLinks()) {
					if (lr.getAssociation().equals("com.infa.ldm.similarity.SimilarColumn")) {

						System.out.println(lr.getId());
						// System.out.println(getTableName(or.getId())+":"+lr.getId());
						HashSet<String> cols = retMap.get(or.getId());
						if (cols == null) {
							cols = new HashSet<String>();
							retMap.put(or.getId(), cols);
						}
						cols.add(lr.getId());
					}
				}
				System.out.println("+++++++++++++++++++++++++++++++++");

			}
		}
		return retMap;
	}

	private HashMap<String, HashSet<String>> getSimilarTables(HashMap<String, HashSet<String>> similarColumns)
			throws Exception {

		HashMap<String, HashSet<String>> retMap = new HashMap<String, HashSet<String>>();

		HashMap<String, HashSet<String>> tableColumnMap = APIUtils.getTableColumnMap();

		HashSet<String> tables = new HashSet<String>();

		for (String mainColumn : similarColumns.keySet()) {
			tables.add(getTableName(mainColumn));
		}

		for (String mainTable : tables) {

			HashSet<String> cols = tableColumnMap.get(mainTable);

			if (cols == null) {
				System.err.println("ERROR! for table: " + mainTable);
				continue;
			}

			HashMap<String, HashSet<String>> checkMap = new HashMap<String, HashSet<String>>();

			for (String col : cols) {
				HashSet<String> checkSet = null;
				if (!similarColumns.containsKey(col)) {
					checkMap.put("NOSIM", null);
					continue;
				} else {
					checkSet = new HashSet<String>();
					checkMap.put(col, checkSet);
				}
				for (String similarCol : similarColumns.get(col)) {
					if (similarCol == null) {
						continue;
					}
					checkSet.add(getTableName(similarCol));
				}
			}

			if (checkMap.containsKey("NOSIM")) {
				System.out.println("No Similar Tables for:" + mainTable);
				continue;
			}

			HashSet<String> similarTables = checkMap.get(cols.toArray()[0]);
			for (String col : cols) {
				similarTables.retainAll(checkMap.get(col));
			}

			retMap.put(mainTable, similarTables);
		}

		return retMap;
	}

	private String getTableName(String columnID) {
		return columnID.substring(0, columnID.lastIndexOf("/"));
	}

	public void run() throws Exception {
		HashMap<String, HashSet<String>> cd = getAllColumns();

		String csv = "output1.csv";
		CSVWriter writer = null;
		try {
			writer = new CSVWriter(new FileWriter(csv));
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		String header = "MAIN,CONNECTED";
		// for(String attribute: attributes) {
		// header+=attribute+",";
		// }
		// header+="BG_ASSOCIATED, BG TERM ID";
		//

		writer.writeNext(header.split(","));

		for (String mainColumn : cd.keySet()) {
			HashSet<String> cols = cd.get(mainColumn);
			for (String col : cols) {
				writer.writeNext(new String[] { mainColumn, col });
			}
		}

		try {
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		CSVWriter writer1 = null;
		try {
			writer1 = new CSVWriter(new FileWriter("SimilarTables.csv"));
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		String header1 = "MAIN,CONNECTED";
		// for(String attribute: attributes) {
		// header+=attribute+",";
		// }
		// header+="BG_ASSOCIATED, BG TERM ID";
		//

		writer.writeNext(header1.split(","));

		HashMap<String, HashSet<String>> similarTables = getSimilarTables(cd);

		for (String mainTable : similarTables.keySet()) {
			HashSet<String> tabs = similarTables.get(mainTable);
			if (tabs == null) {
				System.err.println("ERROR! for table:" + mainTable);
				continue;
			}
			System.out.println("**************************");
			for (String tab : tabs) {
				writer1.writeNext(new String[] { mainTable, tab });
				System.out.println("Similar " + mainTable + " : " + tab);
			}
		}

		try {
			writer1.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void main(String args[]) {

		List<String> attributes = new ArrayList<String>();
		attributes.add("core.resourceName");
		attributes.add(DIVISION);

		try {
			APIUtils.setupOnce();
		} catch (Exception e1) {
			e1.printStackTrace();
		}

		SimilarColumnAssociationReport cdar = new SimilarColumnAssociationReport();
		try {
			cdar.run();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
