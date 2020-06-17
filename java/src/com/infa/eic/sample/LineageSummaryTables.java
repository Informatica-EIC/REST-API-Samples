package com.infa.eic.sample;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
//import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.text.StringEscapeUtils;
 
import com.infa.products.ldm.core.rest.v2.client.invoker.ApiException;
import com.infa.products.ldm.core.rest.v2.client.models.LinkPropertyResponse;
import com.infa.products.ldm.core.rest.v2.client.models.LinkedObjectResponse;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectResponse;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectsResponse;

/**
 * what linked - table level - summary links  (cloned from column version - probably needs some cleanup)
 * 
 * queries EDC to:
 * 	- get a list of all datasets 
 *  - looks for dstLinks that use the "core.DataSetDataFlow" link type
 *  	- this is the link type used for summary links at the dataset level
 *  	- writes to file the from/to objects + some linking information
 *  
 *  useful for any EDC environment - especially POC's & custom demo's
 *  where you find yourself asking what the $%^& actually linked (for lineage)
 *  
 * @author dwrigley
 *
 */
public class LineageSummaryTables {
	
	/**
	 * constructor
	 */
	public LineageSummaryTables() {
		specialClasses.add("com.infa.ldm.file.delimited.DelimitedField");
		specialClasses.add("com.infa.ldm.file.json.JSONField");
		specialClasses.add("com.infa.ldm.file.xml.XMLField");
	}

	public static void main(String[] args) {
		long start = System.currentTimeMillis();
		System.out.println(System.getProperty("java.home"));
//		System.out.println("LineageSummary - Columns - starting:" +start);

		// read command line args
//		String user="Informatica_LDAP_AWS_Users\\dwrigley";
		String user="Administrator";
		String pwd="Administrator";
		String url="http://napslxapp01:9085/access/2";
		String outFolder=".";
		String resourceName="<all>";
		String fileNamePrefix="";
		String edcQuery="";
		boolean includeRefObjects = true;
		
		// default property file - catalog_utils.properties (in current folder)
		String propertyFile = "catalog_utils.properties";
		
		if (args.length==0) {
			// assume default property file	(initial value of propertyFile)
		} else {
			propertyFile = args[0];
		}
			

		
		// read the settings needed to control the process from the property file passed as arg[0]
		try {
			System.out.println("reading properties from: " + propertyFile);
			File file = new File(propertyFile);
			FileInputStream fileInput = new FileInputStream(file);
			Properties prop = new Properties();
			prop.load(fileInput);
			fileInput.close();
			
			url = prop.getProperty("rest_service");
			user = prop.getProperty("user");
			pwd = prop.getProperty("password");
			if (pwd.equals("<prompt>") || pwd.isEmpty() ) {
				System.out.println("password set to <prompt> - waiting for user input... user=" + user);
				pwd = APIUtils.getPassword();
//				System.out.println("pwd entered (debug): " + pwd);
			}
		
			outFolder=prop.getProperty("lineageSummary.outfolder");
			if (!Files.exists(Paths.get(outFolder), new LinkOption[]{ LinkOption.NOFOLLOW_LINKS})) {
				System.out.println("path does not exist..." + outFolder + " exiting");
				System.exit(0);
			}

			includeRefObjects=Boolean.parseBoolean(prop.getProperty("lineageSummary.includeRefObjects", "false"));
			
			fileNamePrefix = prop.getProperty("lineageSummary.filePrefix");
			edcQuery = prop.getProperty("lineageSummary.table.query", "core.allclassTypes:core.DataSet");
			if (edcQuery.isEmpty() ) {
				System.out.println("setting in property file: lineageSummary.table.query - has no content! - exiting" );
				System.exit(0);
			}
//			System.out.println(edcQuery);
						
	     } catch(Exception e) {
	     	System.out.println("error reading properties file: " + propertyFile);
	     	e.printStackTrace();
	     }

				
		// append the rest url - if only the 
		if (! url.endsWith("/access/2")) {
			url = url + "/access/2";
		}
		
		System.out.println("LineageSummary - Table edition:" + url + " user=" + user + " pwd=" + pwd.replaceAll(".", "*") + " " + resourceName);
		System.out.println("\tincluding reference objects:" + includeRefObjects);

		// test
//		String myStr = "POC_EnterpriseDW_Ora_ResScripts://APPL_STAR/Procedures/F_ENC_BILL_PAYOR_FACT/<116,4>MERGE UPDATE/1 POLICY_NO";
//		System.out.println(myStr);
//		System.out.println(escapeIfNecessary(myStr));

			
		// initialize the rest api
		APIUtils.setupOnce(url, user, pwd);
		
		LineageSummaryTables lineageSummary = new LineageSummaryTables();
//		String test="datalake://Hive Metastore/datalake/acme_hive_customer/cust_country_iso";
//		System.out.println(lineageSummary.getIdPart(test, 0).replaceAll(":", ""));
		

		lineageSummary.run(resourceName, outFolder, fileNamePrefix, edcQuery, includeRefObjects);
		
		long end = System.currentTimeMillis();
		long totalMillis = end-start;
		String timeTaken = 	String.format("%d min, %d sec", 
			    TimeUnit.MILLISECONDS.toMinutes(totalMillis),
			    TimeUnit.MILLISECONDS.toSeconds(totalMillis) - 
			    TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(totalMillis))
			);

		System.out.println("LineageSummary - Columns.  time: " + timeTaken);

	}
	
	
	List<String> specialClasses = new ArrayList<String>();
	
	/** 
	 * run the column level linking analysis
	 * 
	 * Note:  since the query result is not sorted
	 * 			we are using collections to store all output in memory
	 * 			then sorting before writing to file
	 * 			this should be tested with larger catalogs with millions of objects for memory sizing
	 * 
	 * @param resource - resource name (Not used:  query assumes all resources currently
	 * @param folder - folder to write the resulting files
	 * @param prefix - prefix with environment/customer/...
	 */
	public void run(String resource, String folder, String prefix, String query, boolean includeRefObjects) {
		System.out.println(this.getClass().getName() + ".run starting....");
		System.out.println("java home=" + System.getProperty("java.home"));
		String sepa = ",";
//		System.out.println("test here...");
		
		// set the total objects returned to > offset.  after the actual query - total will have the # objects found
		int total=1000;
		int offset=0;
		//Get objects in increments of pageSize
		final int pageSize=1000;
//		int count = 0;
		
		// monitoring vars
		int expectedPages=0;
		int currentPage=0;
		int linkCount=0;
		long qStart=0;
		long qTime=0;
//		long pTime=0;
		
		String linkingResource="";
		String outLine="";
		String operation="";
		
		// tableLinks - 
		List<String> columnReport = new ArrayList<String>();
		// columnData - count of facts/links(general src/dst)/lineage src/dst)
		List<String> columnStats = new ArrayList<String>();
		
		ArrayList<String> sort_attrs = new ArrayList<String>();
		sort_attrs.add("id asc");
		ArrayList<String> include_links = new ArrayList<String>();
		include_links.add("core.DataSetDataFlow");
		include_links.add("com.infa.ldm.etl.DetailedDataSetDataFlow");
//		include_links.add("com.infa.ldm.etl.DetailedDataFlow");
		
		System.out.println("query=" + query);
		
		
		// object counts
		int objectCount=0; 	// counter for all objects processed
		int numFacts;
		int numSrcLinks;
		int numDstLinks;
		int numSrcSummaryLineage;
		int numDstSummaryLineage;
		int numSrcDetailLineage;
		int numDstDetailLineage;
//		int duplicates=0;
		
		// count of links per page (statistics reporting & perf monitoring
		int pageDstLinks=0;		// # dstLinks for the page
		int pageSrcLinks=0;		// # srcLinks for the page
		int pageFacts=0;		// # facts for all objects in the page
		
		// for each page/chunk - continue until the offset (starting point for the chunk of items)
		// is less than the total items returned
		// this is also why total is set to something larger than offset above - otherwise it would never start
		while (offset<total) {
			
			ObjectsResponse response;
			try {
				// if first time - it will execute the search
				
				// for subsequent calls - it is reading the next 'page' of the resultset
				qStart = System.currentTimeMillis();
//				response = APIUtils.READER.catalogDataObjectsGet(query, null, BigDecimal.valueOf(offset), BigDecimal.valueOf(pageSize), false);
//				response = APIUtils.READER.catalogDataObjectsGet(query, null, offset, pageSize, null, null);
				// updates for 10.4 + - include reference objects
				response = APIUtils.CATALOG_API.catalogDataObjectsGet(query, null, null, offset, pageSize, null, null, null, include_links, true, true, null, includeRefObjects);
				
				qTime = System.currentTimeMillis() - qStart;
				// get the total objects count  (1 time only?  if offset==0?)
				total=response.getMetadata().getTotalCount().intValue();
				
				// for next call... increment the offset (move to end of method?)
				offset+=pageSize;

				// reset page counters
				pageDstLinks=0;
				pageSrcLinks=0;
				pageFacts=0;

				// if this is the first time - calculate the # of pages to process (total/pageSize)
				if (currentPage==0) {
					// one time... figure out the total page count - for showing n/total
					expectedPages = new BigDecimal(total).divide(new BigDecimal(pageSize)).intValue();
					// +1 if not an exact match (there is a remainder after dividing)
					if (total % pageSize > 0) {
						expectedPages++;
					}

					System.out.println("Catalog search completed: objects found: " + total + " pages to process:" + expectedPages + " pageSize=" + pageSize + " queryTime=" + (qTime/1000) + "sec" );					
				}
				currentPage++;  // don't move this - currentPage=0 is really page 1 (print that to progress screen)
				System.out.print("\tpage " + currentPage + "/" + expectedPages);
						
				//Iterate over returned objects (for the page)
				for(ObjectResponse or: response.getItems()) {
					objectCount++;
					
//					String objectId = or.getId();
					
					// get initial counts (for statistics collection)
					numFacts = or.getFacts().size();		
					numDstLinks = or.getDstLinks().size();
					numSrcLinks = or.getSrcLinks().size();
					// reset counts per object (in case there are no links at all)
					numDstSummaryLineage = 0;  
					numDstDetailLineage = 0;   
					numSrcSummaryLineage = 0;	
					numSrcDetailLineage = 0;
					
					pageFacts+=numFacts;
					
					// object processing starts here
					String objName=APIUtils.getValue(or,APIUtils.CORE_NAME);
					String fmClassType = APIUtils.getValue(or, "core.classType");
					String fromSchemaName = this.getSchemaFromId(or.getId(), fmClassType);
					String fromTableName = this.getTableNameFromId(or.getId(), fmClassType);
					String fromColumnNane = this.getColumnFromId(or.getId(), fmClassType);
					if (! fromColumnNane.equalsIgnoreCase(objName) && ! specialClasses.contains(fmClassType)) {
//						System.out.println("!!! warning - name is not the name: " + objName + " better name=" + fromColumnNane + " id=" + or.getId() + " type=" + fmClassType);
						// log it and set the name back (since we know it here)
						fromColumnNane = objName;
					}
					
					
					
					// look at each dstLink - specifically for core.DirectionalDataFlow
					for(LinkedObjectResponse lr : or.getDstLinks()) {
						numDstLinks++;
						pageDstLinks++;
						// check the association type
						if(lr.getAssociation().equals("core.DataSetDataFlow")) {
							linkCount++;
							numDstSummaryLineage++;
							
							String toSchemaName = this.getSchemaFromId(lr.getId(), lr.getClassType());
							String toTableName = this.getTableNameFromId(lr.getId(), lr.getClassType());
							String toColumnName = this.getColumnFromId(lr.getId(), lr.getClassType());
//							if (! toColumnName.equalsIgnoreCase(lr.getName()) && !lr.getClassType().startsWith("com.infa.ldm.relational") ) {
							if (! toColumnName.equalsIgnoreCase(lr.getName()) && ! specialClasses.contains(lr.getClassType()) ) {
								//System.out.println("!!! warning - name is not the name: " + lr.getName() + " better name=" + toColumnName + " id=" + lr.getId() + " type=" + lr.getClassType());
								// log it and move on
								toColumnName = lr.getName();
							}

							
							//							List<LinkPropertyResponse> linkProps = lr.getLinkProperties();
							// iterate over the link properties to get some context
							linkingResource= APIUtils.getValue(or, "core.resourceName");
							operation="";
							// iterate over all link properties - get info on what scanner created it
							for (LinkPropertyResponse lpr : lr.getLinkProperties()) {
								if (lpr.getAttributeId().equals("core.resourceName")) {
									linkingResource = lpr.getValue();
								} else if (lpr.getAttributeId().equals("com.infa.ldm.etl.pc.Operation")) {
									// if the scanner is PC (or others?)
									// store the operation (summary of any tx logic)
									operation = lpr.getValue();
								}
							}
							
							// for the lineage scanner - the xid property is the only one that has the resource name
							// it will also include the csv file that it originally came from
							if (lr.getProviderId().equals("LineageScanner") | lr.getProviderId().equals("BDMScanner")) {
								linkingResource = lr.getXid();
							}
							

							// format our resultset (1 row per object)
							// structure (fromResource,fromResType,fromid,fromclass,
							//				lineageResourceType,lineage_resourceName,
							//				toResource,toId,toClass,Operation
							// do we care about schema names????
							outLine = APIUtils.getValue(or, "core.resourceName") + sepa  
//										+ APIUtils.getValue(or, "core.resourceType") + sepa
										+ StringEscapeUtils.escapeCsv(or.getId()) + sepa 
										+ StringEscapeUtils.escapeCsv(fmClassType) + sepa  
//										+ StringEscapeUtils.escapeCsv(fromSchemaName) + sepa
//										+ getIdPart(or.getId(), -3) + sepa
										+ StringEscapeUtils.escapeCsv(fromTableName) + sepa
//										+ getParentNameFromId(or.getId()) + sepa
										+ StringEscapeUtils.escapeCsv(fromColumnNane) + sepa
//										+ objName + sepa
										+ StringEscapeUtils.escapeCsv(lr.getProviderId()) + sepa    // lineage scannerType
//										+ lr.g
										+ StringEscapeUtils.escapeCsv(linkingResource) + sepa								// lineageScannerName
										+ StringEscapeUtils.escapeCsv(getIdPart(lr.getId(), 0).replaceAll(":", "")) + sepa     
										+ StringEscapeUtils.escapeCsv(lr.getId()) + sepa  
										+ StringEscapeUtils.escapeCsv(lr.getClassType()) + sepa  
//										+ StringEscapeUtils.escapeCsv(toSchemaName)	+ sepa	// schema name
//										+ getIdPart(lr.getId(), -3)	+ sepa	// schema name
										+ StringEscapeUtils.escapeCsv(toTableName) + sepa   // table name
//										+ getParentNameFromId(lr.getId()) + sepa   // table name
										+ StringEscapeUtils.escapeCsv(toColumnName)	+ sepa	// column name
										+ "\"" + StringEscapeUtils.escapeCsv(operation) + "\"";
							if (!columnReport.contains(outLine)) {
								columnReport.add(outLine);
							} else {
								// count duplicates?
//								duplicates++;
							}

							
						}  // end of core.DirectionalDataFlow
						
						// stats gathering - count the # of DetailedDataFlow links
						if(lr.getAssociation().equals("com.infa.ldm.etl.DetailedDataSetDataFlow")) {
							numDstDetailLineage++;
						}
						
					} // for each dstLink
					
					// iterate ove all srcLinks - get counts (for reporting)
					// same as for dstLinks - but we don't care about the scanner type or outputting col details
					for(LinkedObjectResponse lr : or.getSrcLinks()) {
						numSrcLinks++;
						pageSrcLinks++;
						if(lr.getAssociation().equals("core.DataSetDataFlow")) {
							numSrcSummaryLineage++;
							
							// experimental code starts here **********************************************************
							String toSchemaName = this.getSchemaFromId(lr.getId(), lr.getClassType());
							String toTableName = this.getTableNameFromId(lr.getId(), lr.getClassType());
							String toColumnName = this.getColumnFromId(lr.getId(), lr.getClassType());

							linkingResource= APIUtils.getValue(or, "core.resourceName");

//							System.out.println("tableau source links....");
							if (lr.getProviderId().equals("LineageScanner") | lr.getProviderId().equals("BDMScanner")) {
								linkingResource = lr.getXid();
							}
							
							operation="";
							// iterate over all link properties - get info on what scanner created it
							for (LinkPropertyResponse lpr : lr.getLinkProperties()) {
								if (lpr.getAttributeId().equals("core.resourceName")) {
									linkingResource = lpr.getValue();
//									System.out.println("linking resource???" + linkingResource);
								} else if (lpr.getAttributeId().equals("com.infa.ldm.etl.pc.Operation")) {
									// if the scanner is PC (or others?)
									// store the operation (summary of any tx logic)
									operation = lpr.getValue();
								}
							}



							// add the link from this src to the target object (the one being read)
							outLine = getIdPart(lr.getId(), 0).replaceAll(":", "") + sepa   
									+ StringEscapeUtils.escapeCsv(lr.getId()) + sepa 
									+ StringEscapeUtils.escapeCsv(lr.getClassType()) + sepa  
//									+ StringEscapeUtils.escapeCsv(toSchemaName)	+ sepa	// schema name
									+ StringEscapeUtils.escapeCsv(toTableName) + sepa   // table name
									+ StringEscapeUtils.escapeCsv(toColumnName)	+ sepa	// column name
									+ StringEscapeUtils.escapeCsv(lr.getProviderId()) + sepa   // lineageScannerType
									+ StringEscapeUtils.escapeCsv(linkingResource) + sepa
									+ StringEscapeUtils.escapeCsv(APIUtils.getValue(or, "core.resourceName")) + sepa
									+ StringEscapeUtils.escapeCsv(or.getId()) + sepa  
									+ StringEscapeUtils.escapeCsv(fmClassType) + sepa  
//									+ StringEscapeUtils.escapeCsv(fromSchemaName) + sepa
									+ StringEscapeUtils.escapeCsv(fromTableName) + sepa
									+ StringEscapeUtils.escapeCsv(fromColumnNane) + sepa
									+ "\"" + StringEscapeUtils.escapeCsv(operation) + "\"";
							if (!columnReport.contains(outLine)) {
//								System.out.println("tableau source links.... new here....");
//								System.out.println(outLine);
								columnReport.add(outLine);
							} else {
							// count duplicates?
//														duplicates++;
							}
							// experimental ends here ******************************************************************

							
						}
						if(lr.getAssociation().equals("com.infa.ldm.etl.DetailedDataSetDataFlow")) {
							numSrcDetailLineage++;
						}
					}

					// add to the object summary for this object
					columnStats.add(APIUtils.getValue(or, "core.resourceName") 
							+ sepa + StringEscapeUtils.escapeCsv(or.getId())
							+ sepa + StringEscapeUtils.escapeCsv(APIUtils.getValue(or, "core.classType"))
//							+ sepa + StringEscapeUtils.escapeCsv(getIdPart(or.getId(), -3))
							+ sepa + StringEscapeUtils.escapeCsv(getIdPart(or.getId(), -2))
							+ sepa + StringEscapeUtils.escapeCsv(objName)
							+ sepa + numFacts 
							+ sepa + numSrcLinks
							+ sepa + numDstLinks
							+ sepa + numSrcSummaryLineage
							+ sepa + numDstSummaryLineage
							+ sepa + numSrcDetailLineage
							+ sepa + numDstDetailLineage
							);
					
				}  // iterator - items in the returned 'page'
				
			} catch (ApiException e) {
				e.printStackTrace();
			}
			
			
			// end of processing for the page
			// print some stats (to show progress) & help understand why some pages take longer than others
			// 		- it is all to do with the # src/dst links (more testing/understanding needed here)
			float percentage = (currentPage * 100/ expectedPages);
			System.out.println(" obj:" + (offset-pageSize+1) + "-" + offset + "/" + total +
			" progress:" + percentage + "%" + " linkCount=" + linkCount 
			+ " qTime=" + (qTime/1000) + "sec ptime=" + (System.currentTimeMillis()-qStart-qTime) + "ms" 
			+ " facts: " + pageFacts + " srcLinks:" + pageSrcLinks + " dstLinks:" + pageDstLinks
			);

		} // end of all objects queried

		System.out.println("end of processing: objects processed: " + objectCount);
//		System.out.println(tableLinks);
		
		// dump to file...
		// rest api does not have sorted resultsets - so we sort here before writing to file
		Collections.sort(columnReport);
		columnReport.add(0, "fromResource,FromId,FromClass,fromSchema,fromTable,lineageScannerType,lineageScannerName,ToResource,ToId,ToClass,toSchema,toTable,Operation");
		String outFile = folder + "/" + prefix + "_Table_SummaryLineage.csv";
		System.out.println("writing file: " + outFile + " - " + columnReport.size() + " records" );
		this.dumpToFile(outFile, columnReport);
		
		Collections.sort(columnStats);
		outFile = folder + "/" + prefix + "_Table_LinkCounts.csv";
		columnStats.add(0, "resource,id,classType,schema,table,facts,srcLinks,dstLinks,srcSummaryLineage,dstSummaryLineage,srcDetailLineage,dstDetailedLineage");
		System.out.println("writing file: " + outFile + " - " + columnReport.size() + " records" );
		this.dumpToFile(outFile, columnStats);

	}
	
	static String escapeIfNecessary(String instr) {
//		String newStr="";
		if (instr.contains(",")) {
			return "\"" + instr + "\"";
		} else {
			return instr;
		}
	}

	/**
	 * write the contents of a List (of Strings) to file
	 * @param fileName - to write to
	 * @param theStructure - list of Strings to write
	 */
	protected void dumpToFile(String fileName, List<String> theStructure) {
		try {
			Path toPath = Paths.get(fileName);
			Charset charset = Charset.forName("UTF-8");
			Files.write(toPath, theStructure, charset);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * get the schema name from an id
	 * @param id
	 * @param classType
	 * @return the schema name
	 * 
	 * depending on the class type - a schema means different things
	 * for dbms - it is the 3rd last entry (db/schema/table/column)
	 * for csv,xml,avro.json - the schema is the path to the file (full path - after the filesystem & before the file name
	 * 
	 * examples:
	 * s3://amazons3/files/demo/person_with_gender_email_ip46.json/rootArray/root/last_name
	 * 		schemaname = files/demo
	 * 
	 * for 
	 */
	public String getSchemaFromId(String objectId, String classType) {
		String schemaName = "";
		if (specialClasses.contains(classType)) {
			String nameparts[]= objectId.split("/");
			int filepos = getFilePosFromId(nameparts);
			
			for (int i=3; i<filepos; i++) {
				if (schemaName.length()==0) {
					schemaName = nameparts[i];
				} else {
					schemaName+="/" + nameparts[i];
				}
			}
			
		} else {
			// return the 3rd last item (resource://db/schema/table/column)
			schemaName = getIdPart(objectId, -3);
			
		}
		
		return schemaName;
	}

	public String getTableNameFromId(String objectId, String classType) {
		String fileName = "";
		
			// return the 3rd last item (resource://db/schema/table/column)
		if (specialClasses.contains(classType)) {
			
			String nameparts[]= objectId.split("/");
			int filepos = getFilePosFromId(nameparts);
			fileName = nameparts[filepos];
			
		} else {
			// databases and other objects
			fileName = getParentNameFromId(objectId);
		}
		
		return fileName;
	}


	public String getColumnFromId(String objectId, String classType) {
		String columnName = "";
		String nameparts[]= objectId.split("/");

		
		if (specialClasses.contains(classType)) {
			
			int filepos = getFilePosFromId(nameparts);
			
			for (int i=filepos+1; i<nameparts.length; i++) {
				if (columnName.length()==0) {
					columnName = nameparts[i];
				} else {
					columnName+="/" + nameparts[i];
				}
			}
			
		} else {
			// databases and other objects
			columnName = nameparts[nameparts.length-1];
			
		}
		
		return columnName;
	}


	private int getFilePosFromId(String[] nameparts) {
		int filepos = nameparts.length-1;
		// go backwards through the array elements - looking for a filename (<string>.<string>)
		for (int i=nameparts.length-2; i>2; i--) {
//			System.out.println("checking..." + i + " " + nameparts[i]);
			if (nameparts[i].contains(".")) {
				filepos = i;
				break;
				// now get the names from position 3-filepos
			}
		}
//		System.out.println("filename..." + filepos);
		return filepos;
	}

	
	// utility functions
	
	/**
	 * getParentName(String id)
	 * 
	 * id's are formmated as:-
	 * [resourceName]://[db]/[schema]/[table]/[column]
	 * or 
	 * [resourceName]://[fileSystem]/[folder]+/[file]/[field]
	 * 
	 * 
	 * @param id object id (for a column or file)
	 * @return the parent object (2nd last element split by / 
	 */
	protected String getParentNameFromId(String id) {
		String array[]= id.split("/");
//		System.out.println("checking string: " + id + " arraysize=" + array.length + " " + array);
		return array[array.length-2];

	}

	/**
 	 * get a part of an id - seperated by /
 	 * 
	 * @param id - the id to extract the part
	 * @param partToGet - if negative - get from the end.  -1 = the last part, -2 2nd last part
	 * @return string with the part requested
	 */
	protected String getIdPart(String id, int partToGet) {
		String array[]= id.split("/");
//		System.out.println("checking string: " + id + " arraysize=" + array.length + " partNum=" + partToGet);
		if (partToGet<0 ) {
			return array[array.length+partToGet];
		} else {
			return array[partToGet];
		}

	}

}
