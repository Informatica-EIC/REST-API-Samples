package com.infa.eic.sample;
import java.math.BigDecimal;
import java.util.ArrayList;

import com.infa.products.ldm.core.rest.v2.client.invoker.ApiException;
import com.infa.products.ldm.core.rest.v2.client.models.AttributeResponse;
import com.infa.products.ldm.core.rest.v2.client.models.AttributesResponse;
import com.infa.products.ldm.core.rest.v2.client.models.FactRequest;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectIdRequest;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectRefResponse;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectResponse;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectsResponse;
import com.infa.products.ldm.core.rest.v2.client.models.RefAttributeResponse;
import com.infa.products.ldm.core.rest.v2.client.utils.ObjectAdapter;

/**
 * 
 */

/**
 * This program uses the EIC REST API to add values to custom attributes in data assets.
 * @author gpathak
 *
 */
public class BulkClassifier {
	
	private static String DIVISION="com.infa.appmodels.ldm.LDM_a5922c30_42eb_40ac_bb1e_75362b67ea9c";
	private static String HR="Human Resources";
	private static String MARKETING="Marketing";
	private static String PROCUREMENT="Procurement";

	
	public BulkClassifier() {
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		BulkClassifier b=new BulkClassifier();
		
		//Connect to the EIC REST Instance 
		try {
			APIUtils.setupOnce();
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		try {
			b.bulkClassify(DIVISION, HR, "emp*" +" AND core.allclassTypes:\""+ APIUtils.TABLE_CLASSTYPE+"\"");
			
//			b.bulkClassify(DIVISION, HR, "dept*" +" AND core.allclassTypes:\""+ TABLE_CLASSTYPE+"\"");
//			b.bulkClassify(DIVISION, HR, "dept*" +" AND core.allclassTypes:\""+ COL_CLASSTYPE+"\"");
//			b.bulkClassify(DIVISION, HR, "sal*" +" AND core.allclassTypes:\""+ COL_CLASSTYPE+"\"");
//			b.bulkClassify(DIVISION, HR, "emp*" +" AND core.allclassTypes:\""+ COL_CLASSTYPE+"\"");
//			
//			b.bulkClassify(DIVISION, MARKETING, "cust*" +" AND core.allclassTypes:\""+ TABLE_CLASSTYPE+"\"");
//			b.bulkClassify(DIVISION, MARKETING, "cust*" +" AND core.allclassTypes:\""+ COL_CLASSTYPE+"\"");
//			
//			b.bulkClassify(DIVISION, PROCUREMENT, "ord*" +" AND core.allclassTypes:\""+ COL_CLASSTYPE+"\"");
//			b.bulkClassify(DIVISION, PROCUREMENT, "ord*" +" AND core.allclassTypes:\""+ TABLE_CLASSTYPE+"\"");
//			b.bulkClassify(DIVISION, PROCUREMENT, "prod*" +" AND core.allclassTypes:\""+ COL_CLASSTYPE+"\"");
//			b.bulkClassify(DIVISION, PROCUREMENT, "prod*" +" AND core.allclassTypes:\""+ TABLE_CLASSTYPE+"\"");
//			b.bulkClassify(DIVISION, PROCUREMENT, "*ware*" +" AND core.allclassTypes:\""+ COL_CLASSTYPE+"\"");
//			b.bulkClassify(DIVISION, PROCUREMENT, "*ware*" +" AND core.allclassTypes:\""+ TABLE_CLASSTYPE+"\"");
			
			//b.bulkClassify(DIVISION, HR, "OrderEntry" +" AND core.allclassTypes:\""+ COL_CLASSTYPE+"\"");
			
//			ArrayList<String> referenceClassType=new ArrayList<String>();
//			referenceClassType.add("com.infa.ldm.isp.User");
//			b.bulkClassifyReferenceAttribute("", referenceClassType, "", "");
			
		} catch(Exception e) {
			e.printStackTrace();
			
		}
		

	}
	
	
	
	public void bulkClassify(String customAttributeID, String value, String query) throws Exception  {
		int total=1000;
		int offset=0;
		final int pageSize=20;
		
		while (offset<total) {
			ObjectsResponse response=APIUtils.READER.catalogDataObjectsGet(query, null, BigDecimal.valueOf(offset), BigDecimal.valueOf(pageSize), false);
			
			total=response.getMetadata().getTotalCount().intValue();
			offset+=pageSize;
			
			for(ObjectResponse or: response.getItems()) {
				
				ObjectIdRequest request=ObjectAdapter.INSTANCE.copyIntoObjectIdRequest(or);
				String curVal=APIUtils.getValue(or,customAttributeID);
				if(curVal!=null) {
					request.getFacts().remove(new FactRequest().attributeId(customAttributeID).value(curVal));
				}
				
				request.addFactsItem(new FactRequest().attributeId(customAttributeID).value(value));
				
				String ifMatch;
				try {
					ifMatch = APIUtils.READER.catalogDataObjectsIdGetWithHttpInfo(or.getId()).getHeaders().get("ETag").get(0);
				
					ObjectResponse newor=APIUtils.WRITER.catalogDataObjectsIdPut(or.getId(), request, ifMatch);
					System.out.println(or.getId()+":"+value);
				} catch (ApiException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		}
	}
		
		public void bulkClassifyReferenceAttribute(String customAttributeID, ArrayList<String> referenceClassType, String value, String query) throws Exception  {
			int total=1000;
			int offset=0;
			final int pageSize=20;
			ArrayList<String> refAttributeID=new ArrayList<String>();
			refAttributeID.add("com.infa.appmodels.ldm.LDM_6de56c74_4560_40c7_9efe_bbdb3eba9b51");
			while(offset<total) {
				AttributesResponse responses=APIUtils.MODEL_READER.catalogModelsAttributesGet(refAttributeID, referenceClassType, BigDecimal.valueOf(offset), BigDecimal.valueOf(pageSize));
				total=responses.getMetadata().getTotalCount().intValue();
				offset+=pageSize;
				
				for(AttributeResponse response: responses.getItems()) {
					for(ObjectRefResponse or: response.getClasses()) {
						System.out.println(or.getName());
					}
				}
			}
			
			
//			while (offset<total) {
//				ObjectsResponse response=APIUtils.READER.catalogDataObjectsGet(query, null, BigDecimal.valueOf(offset), BigDecimal.valueOf(pageSize), false);
//				
//				total=response.getMetadata().getTotalCount().intValue();
//				offset+=pageSize;
//				
//				for(ObjectResponse or: response.getItems()) {
//					
//					ObjectIdRequest request=ObjectAdapter.INSTANCE.copyIntoObjectIdRequest(or);
//					String curVal=APIUtils.getValue(or,customAttributeID);
//					if(curVal!=null) {
//						request.getFacts().remove(new FactRequest().attributeId(customAttributeID).value(curVal));
//						request.getFacts().remove(new FactRequest().attributeId(customAttributeID).value(curVal));
//						
//					}
//					
//					request.addFactsItem(new FactRequest().attributeId(customAttributeID).value(value));
//					
//					String ifMatch;
//					try {
//						ifMatch = APIUtils.READER.catalogDataObjectsIdGetWithHttpInfo(or.getId()).getHeaders().get("ETag").get(0);
//					
//						ObjectResponse newor=APIUtils.WRITER.catalogDataObjectsIdPut(or.getId(), request, ifMatch);
//						System.out.println(or.getId()+":"+value);
//					} catch (ApiException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//					
//				}
//			}
		
		
		
	}
	
	
	

}
