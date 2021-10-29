package com.infa.eic.sample;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.infa.products.ldm.core.rest.v2.client.models.LinkedObjectResponse;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectResponse;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectsResponse;
import com.opencsv.CSVWriter;

public class ColumnDomainAssociationReport {
	private static String DIVISION = "com.infa.appmodels.ldm.LDM_a5922c30_42eb_40ac_bb1e_75362b67ea9c";

	List<String> attributes;

	public ColumnDomainAssociationReport(List<String> attributes) {
		this.attributes = attributes;
	}

	public ColumnDomainAssociationReport() {
		this.attributes = null;
	}

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
			// ObjectsResponse response=APIUtils.READER.catalogDataObjectsGet(query, null,
			// offset, pageSize, null, null);
			// EDC (client.jar) 10.4+
			ObjectsResponse response = APIUtils.CATALOG_API.catalogDataObjectsGet(query, null, null, offset, pageSize,
					null, null, null, null, true, true, null, null);

			total = response.getMetadata().getTotalCount().intValue();
			offset += pageSize;

			for (ObjectResponse or : response.getItems()) {
				for (LinkedObjectResponse lr : or.getSrcLinks()) {
					if (lr.getClassType().equals(APIUtils.DOMAIN_CLASSTYPE)
							&& lr.getAssociation().equals("com.infa.ldm.profiling.DataDomainColumnInferred")) {
						System.out.println(getTableName(or.getId()) + ":" + lr.getId());
						HashSet<String> cols = retMap.get(lr.getId());
						if (cols == null) {
							cols = new HashSet<String>();
							retMap.put(lr.getId(), cols);
						}
						cols.add(getTableName(or.getId()));
					}
				}
			}
		}
		return retMap;
	}

	private String getTableName(String columnID) {
		return columnID.substring(0, columnID.lastIndexOf("/"));
	}

	public void run() throws Exception {
		Map<String, HashSet<String>> cd = getAllColumns();

		String csv = "output1.csv";
		CSVWriter writer = null;
		try {
			writer = new CSVWriter(new FileWriter(csv));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		String header = "MAIN,CONNECTED,TABLE";
		// for(String attribute: attributes) {
		// header+=attribute+",";
		// }
		// header+="BG_ASSOCIATED, BG TERM ID";
		//

		writer.writeNext(header.split(","));

		for (String mainDomain : cd.keySet()) {
			for (String connectedDomain : cd.keySet()) {
				if (mainDomain.equals(connectedDomain)) {
					continue;
				}

				HashSet<String> cols = cd.get(mainDomain);
				for (String col : cols) {
					writer.writeNext(new String[] { mainDomain, "", col });
					if (cd.get(connectedDomain).contains(col)) {
						// writer.writeNext(new String[]{mainDomain,connectedDomain,col});
						writer.writeNext(new String[] { mainDomain, connectedDomain, "" });
					}
				}

			}
		}

		try {
			writer.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String args[]) {

		List<String> attributes = new ArrayList<String>();
		attributes.add("core.resourceName");
		attributes.add(DIVISION);

		APIUtils.setupOnce();

		ColumnDomainAssociationReport cdar = new ColumnDomainAssociationReport();
		try {
			cdar.run();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
