/**
 * 
 */
package com.infa.eic.sample;

import java.io.Console;
// import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;

import javax.swing.JOptionPane;
import javax.swing.JPasswordField;

import com.infa.products.ldm.core.rest.v2.client.api.CatalogApi;
import com.infa.products.ldm.core.rest.v2.client.api.ModelInfoApi;
import com.infa.products.ldm.core.rest.v2.client.api.ObjectInfoApi;
import com.infa.products.ldm.core.rest.v2.client.api.ObjectModificationApi;
import com.infa.products.ldm.core.rest.v2.client.models.FactResponse;
import com.infa.products.ldm.core.rest.v2.client.models.LinkedObjectResponse;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectResponse;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectsResponse;

/**
 * @author gpathak
 *
 */
public final class APIUtils {

	public static final String TABLE_CLASSTYPE = "com.infa.ldm.relational.Table";
	public static final String COL_CLASSTYPE = "com.infa.ldm.relational.Column";
	public static final String DOMAIN_CLASSTYPE = "com.infa.ldm.profiling.DataDomain";
	public static final String CORE_NAME = "core.name";
	public static final String CORE_RESOURCE_NAME = "core.resourceName";
	public static final String BGTERM = "com.infa.ldm.bg.BGTerm";

	public static final String DATASET_FLOW = "core.DataSetDataFlow";

	/**
	 * Access URL of the EIC Instance
	 */
	private static String URL = "http://<eic-host>:<port>/access/2";

	/**
	 * Credentials.
	 */
	private static String USER = ""; // Enter User name
	private static String PASS = ""; // Enter password

	public final static ObjectInfoApi READER = new ObjectInfoApi();
	public final static ObjectModificationApi WRITER = new ObjectModificationApi();

	public final static CatalogApi CATALOG_API = new CatalogApi();

	public final static ModelInfoApi MODEL_READER = new ModelInfoApi();

	public final static void setupOnce() {
		READER.getApiClient().setUsername(USER);
		READER.getApiClient().setPassword(PASS);
		READER.getApiClient().setBasePath(URL);

		WRITER.getApiClient().setUsername(USER);
		WRITER.getApiClient().setPassword(PASS);
		WRITER.getApiClient().setBasePath(URL);
		MODEL_READER.getApiClient().setUsername(USER);
		MODEL_READER.getApiClient().setPassword(PASS);
		MODEL_READER.getApiClient().setBasePath(URL);
		CATALOG_API.getApiClient().setUsername(USER);
		CATALOG_API.getApiClient().setPassword(PASS);
		CATALOG_API.getApiClient().setBasePath(URL);

	}

	/**
	 * setup the rest api - passing the url, id and password (so it does not need to
	 * be hard-coded)
	 * 
	 * @author dwrigley
	 * 
	 * @param url the rest api url
	 * @param uid the user id (prefixed with security domain & \, if not Native)
	 * @param pwd the password for the user
	 */
	public final static void setupOnce(String url, String uid, String pwd) {
		READER.getApiClient().setUsername(uid);
		READER.getApiClient().setPassword(pwd);
		READER.getApiClient().setBasePath(url);

		WRITER.getApiClient().setUsername(uid);
		WRITER.getApiClient().setPassword(pwd);
		WRITER.getApiClient().setBasePath(url);

		MODEL_READER.getApiClient().setUsername(uid);
		MODEL_READER.getApiClient().setPassword(pwd);
		MODEL_READER.getApiClient().setBasePath(url);
	}

	public static final String getValue(ObjectResponse obj, String name) {
		for (FactResponse fact : obj.getFacts()) {
			if (name.equals(fact.getAttributeId())) {
				return fact.getValue();
			}
		}
		return null;
	}

	/**
	 * Returns a hashmap of <assetID, assetName> where data assets belong to the
	 * provided type and resource
	 * 
	 * @param resourceName EIC Resource Name
	 * @param type         Class ID of the asset type
	 * @return hashmap of <assetID,assetName>
	 * @throws Exception
	 */
	public static final HashMap<String, String> getAssetsByType(String resourceName, String type) throws Exception {
		int total = 1000;
		int offset = 0;
		// Get objects in increments of 300
		final int pageSize = 300;

		// Standard Lucene style object query to get assets of a given type from a given
		// resource.
		String query = CORE_RESOURCE_NAME + ":\"" + resourceName + "\" AND core.allclassTypes:\"" + type + "\"";

		HashMap<String, String> retMap = new HashMap<String, String>();

		while (offset < total) {
			// Query the Object READER
			// EDC (client.jar) <=10.2.1
			// ObjectsResponse response=READER.catalogDataObjectsGet(query, null,
			// BigDecimal.valueOf(offset), BigDecimal.valueOf(pageSize), false);
			// EDC (client.jar) 10.2.2 (+ 10.2.2 sp1)
			// ObjectsResponse response = READER.catalogDataObjectsGet(query, null, offset,
			// pageSize, null, null);
			// EDC (client.jar) 10.2.2hf1+
			// ObjectsResponse response=CATALOG_API.catalogDataObjectsGet(query, null,
			// offset, pageSize, null, null, null, null, null, null, null);
			// EDC (client.jar) 10.4+
			ObjectsResponse response = APIUtils.CATALOG_API.catalogDataObjectsGet(query, null, null, offset, pageSize,
					null, null, null, null, true, true, null, null);

			total = response.getMetadata().getTotalCount().intValue();
			offset += pageSize;

			// Iterate over returned objects and add them to the return hashmap
			for (ObjectResponse or : response.getItems()) {
				String curVal = getValue(or, CORE_NAME);
				if (curVal != null) {
					// Hashkey is the object ID.
					retMap.put(or.getId(), curVal);
				}
			}
		}
		return retMap;
	}

	public static final HashMap<String, HashSet<String>> getTableColumnMap() throws Exception {
		int total = 1000;
		int offset = 0;
		final int pageSize = 300;

		String query = "* AND core.allclassTypes:\"" + TABLE_CLASSTYPE + "\"";
		HashMap<String, HashSet<String>> retMap = new HashMap<String, HashSet<String>>();

		while (offset < total) {
			// EDC (client.jar) <=10.2.1
			// ObjectsResponse response=READER.catalogDataObjectsGet(query, null,
			// BigDecimal.valueOf(offset), BigDecimal.valueOf(pageSize), false);
			// EDC (client.jar) 10.2.2 (+ 10.2.2 sp1)
			// ObjectsResponse response = READER.catalogDataObjectsGet(query, null, offset,
			// pageSize, null, null);
			// EDC (client.jar) 10.4+
			ObjectsResponse response = APIUtils.CATALOG_API.catalogDataObjectsGet(query, null, null, offset, pageSize,
					null, null, null, null, true, true, null, null);

			total = response.getMetadata().getTotalCount().intValue();
			offset += pageSize;

			for (ObjectResponse or : response.getItems()) {
				HashSet<String> colSet = new HashSet<String>();
				retMap.put(or.getId(), colSet);
				for (LinkedObjectResponse lr : or.getDstLinks()) {
					if (lr.getAssociation().equals("com.infa.ldm.relational.TableColumn")) {
						colSet.add(lr.getId());
					}
				}

			}
		}
		return retMap;
	}

	/**
	 * prompt the user for a password, using the console (default) for development
	 * environments like eclipse, their is no standard console. so in that case we
	 * open a swing ui panel with an input field to accept a password
	 * 
	 * @return the password entered
	 * 
	 * @author dwrigley
	 */
	public static String getPassword() {
		String password;
		Console c = System.console();
		if (c == null) { // IN ECLIPSE IDE (prompt for password using swing ui
			final JPasswordField pf = new JPasswordField();
			String message = "User password:";
			password = JOptionPane.showConfirmDialog(null, pf, message, JOptionPane.OK_CANCEL_OPTION,
					JOptionPane.QUESTION_MESSAGE) == JOptionPane.OK_OPTION ? new String(pf.getPassword())
							: "enter your pwd here....";
		} else { // Outside Eclipse IDE (e.g. windows/linux console)
			password = new String(c.readPassword("User password: "));
		}
		return password;
	}

}
