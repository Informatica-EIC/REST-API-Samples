/**
 * 
 */
package com.infa.eic.sample;

import java.math.BigDecimal;
import java.util.HashMap;

import com.infa.products.ldm.core.rest.v2.client.api.ModelInfoApi;
import com.infa.products.ldm.core.rest.v2.client.api.ObjectInfoApi;
import com.infa.products.ldm.core.rest.v2.client.api.ObjectModificationApi;
import com.infa.products.ldm.core.rest.v2.client.models.FactResponse;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectResponse;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectsResponse;

/**
 * @author gpathak
 *
 */
public final class APIUtils {
	
	public static final String TABLE_CLASSTYPE="com.infa.ldm.relational.Table";
	public static final String COL_CLASSTYPE="com.infa.ldm.relational.Column";
	public static final String DOMAIN_CLASSTYPE="com.infa.ldm.profiling.DataDomain";
	public static final String CORE_NAME="core.name";
	public static final String CORE_RESOURCE_NAME="core.resourceName";
	public static final String BGTERM="com.infa.ldm.bg.BGTerm";
	
	//private static String BASE="http://eicbeta38004.informatica.com:9085";
	public static final String DATASET_FLOW="core.DataSetDataFlow";
	
//	/**
//	 * Access URL of the EIC Instance
//	 */
//	private static String URL="http://34.213.249.201:9085/access/2";
//	
//	/**
//	 * Credentials.
//	 */
//	private static String USER="Administrator";
//	private static String PASS="Infa@2016";

	/**
	 * Access URL of the EIC Instance
	 */
	private static String URL="http://eicbeta38004.informatica.com:9085/access/2";
	
	/**
	 * Credentials.
	 */
	private static String USER="gaurav";
	private static String PASS="welcome1";
	
	
	
	public final static ObjectInfoApi READER = new ObjectInfoApi(); 
	public final static ObjectModificationApi WRITER = new ObjectModificationApi();

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
	}
	
	public static final String getValue(ObjectResponse obj, String name) {
		for(FactResponse fact:obj.getFacts()) {
			if(name.equals(fact.getAttributeId())) {
				return fact.getValue();
			}
		}
		return null;
	}
	
	/**
	 * Returns a hashmap of <assetID, assetName> where data assets belong to the provided type
	 * and resource
	 * @param resourceName EIC Resource Name
	 * @param type Class ID of the asset type
	 * @return hashmap of <assetID,assetName>
	 * @throws Exception
	 */
	public static final HashMap<String, String> getAssetsByType(String resourceName,String type) throws Exception {
		int total=1000;
		int offset=0;
		//Get objects in increments of 300
		final int pageSize=300;
		
		//Standard Lucene style object query to get assets of a given type from a given resource.
		String query=CORE_RESOURCE_NAME+":\""+resourceName+"\" AND core.allclassTypes:\""+type+"\"";
		
		HashMap<String, String> retMap=new HashMap<String,String>();
		
		while (offset<total) {
			//Query the Object READER
			ObjectsResponse response=READER.catalogDataObjectsGet(query, null, BigDecimal.valueOf(offset), BigDecimal.valueOf(pageSize), false);
			
			total=response.getMetadata().getTotalCount().intValue();
			offset+=pageSize;
			
			//Iterate over returned objects and add them to the return hashmap
			for(ObjectResponse or: response.getItems()) {
				String curVal=getValue(or,CORE_NAME);
				if(curVal!=null) {
					//Hashkey is the object ID.
					retMap.put(or.getId(),curVal);
				}
			}
		}
		return retMap;
	}

}
