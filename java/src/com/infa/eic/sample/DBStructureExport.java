package com.infa.eic.sample;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.infa.products.ldm.core.rest.v2.client.models.EmbeddedFact;
import com.infa.products.ldm.core.rest.v2.client.models.EmbeddedObject;
import com.infa.products.ldm.core.rest.v2.client.models.Link;
import com.infa.products.ldm.core.rest.v2.client.models.LinkedObjectResponse;
import com.infa.products.ldm.core.rest.v2.client.models.Links;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectResponse;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectsResponse;

/**
 * this program creates a file which represents the data structure of an EIC
 * resource details included are:- database,schema,table,column,datatype,length
 * 
 * this information can be used to compare resource structure loads over time
 * (until version history has been implemented in edc)
 * 
 * @author dwrigley
 *
 */
public class DBStructureExport {
	public static final String version = "1.3";

	String sepa = "|";
	Boolean includeAxonTermLink = false;
	int axonLinks = 0;

	// for v1 calls store id/pwd
	// private String userId;
	// private String userPwd;
	// private String restUrl;
	private List<String> externalDbIds = new ArrayList<String>();
	boolean excludeExtDbItems = true;

	/**
	 * set the flag to include axon terms in the result
	 * 
	 * @param showAxonTerm true to include axon term link for the db structure
	 *                     (default false)
	 */
	public void setIncludeAxonTermLinks(Boolean showAxonTerm) {
		this.includeAxonTermLink = showAxonTerm;
	}

	/**
	 * set the flag that determines if external DB objects should be excluded
	 * default will be true
	 * 
	 * @param filterExcludeExtDbObjects
	 */
	public void setExcludeExtDbObjects(boolean filterExcludeExtDbObjects) {
		this.excludeExtDbItems = filterExcludeExtDbObjects;
	}

	/**
	 * Instantiate class with resource name list, attribute list and class type
	 * list.
	 * 
	 * @param restUrl  the server:port/url used for the rest api
	 * @param user     user id for EIC (either username or securitydomain\\username
	 *                 (with one slash char)
	 * @param password
	 */
	public DBStructureExport(String restUrl, String user, String pwd) {
		System.out.println(
				"\t" + this.getClass().getSimpleName() + " " + version + " called: " + restUrl + " user=" + user);
		APIUtils.setupOnce(restUrl, user, pwd);
		// store the id/pwd/url - for v1 calls (using search)
		// this.userId = user;
		// this.userPwd = pwd;
		// this.restUrl = restUrl;
	}

	/**
	 * query EIC to get the resource structure for db objects
	 * 
	 * @param resourceName
	 * @param versionLabel
	 * @return
	 * @throws Exception
	 */
	public List<String> getResourceStructure(String resourceName, int pageSize) throws Exception {
		String type = "com.infa.ldm.relational.Column";
		String dbName;
		String schemaName;
		String tableName;
		long start = System.currentTimeMillis();
		// String[]

		// execute the search to find all instances of external databases from the
		// resource
		this.getExternalDbIdsForResource(resourceName);

		List<String> dbStructLines = new ArrayList<String>();

		System.out.println("\textracting structure for resource=" + resourceName + " type=" + type);

		int total = 1000;
		int offset = 0;
		int skipped = 0;

		// Standard Lucene style object query to get assets of a given type from a given
		// resource.
		String query = APIUtils.CORE_RESOURCE_NAME + ":\"" + resourceName + "\" AND (" + "core.allclassTypes:\"" + type
				+ "\"" + " OR " + "core.allclassTypes:\"com.infa.ldm.relational.ViewColumn\" )";

		// for each page
		while (offset < total) {
			// EDC (client.jar) <=10.2.1
			// ObjectsResponse response=APIUtils.READER.catalogDataObjectsGet(query, null,
			// BigDecimal.valueOf(offset), BigDecimal.valueOf(pageSize), false);
			// EDC (client.jar) 10.2.2 (+ 10.2.2 sp1)
			// ObjectsResponse response=APIUtils.READER.catalogDataObjectsGet(query, null,
			// offset, pageSize, null, null);
			// 10.2.2hf1+
			// ObjectsResponse response = APIUtils.CATALOG_API.catalogDataObjectsGet(query,
			// null, null, offset, pageSize,
			// null, null, null, null, null, null, null);
			// EDC (client.jar) 10.4+
			ObjectsResponse response = APIUtils.CATALOG_API.catalogDataObjectsGet(query, null, null, offset, pageSize,
					null, null, null, null, true, true, null, null);

			total = response.getMetadata().getTotalCount().intValue();
			offset += pageSize;
			System.out.println("\tobjects found: " + total + " offset: " + offset + " pagesize=" + pageSize);

			// Iterate over returned objects and add them to the return hashmap
			for (ObjectResponse or : response.getItems()) {

				// check - if external objects are to be excluded and the id
				if (excludeExtDbItems && this.isIdFromExternalDB(or.getId())) {
					skipped++;
					continue;
				}
				String colName = APIUtils.getValue(or, APIUtils.CORE_NAME);
				String array[] = or.getId().split("/");
				dbName = array[2];
				schemaName = array[3];
				tableName = array[4];
				// add a line to the db structure list (to be sorted & written to file later)
				String axonId = "";
				String axonName = "";

				StringBuffer dbTableLine = new StringBuffer();
				StringBuffer dbStructureLine = new StringBuffer();
				dbStructureLine.append(dbName).append(sepa).append(schemaName).append(sepa).append(tableName)
						.append(sepa).append(colName).append(sepa)
						.append(APIUtils.getValue(or, "com.infa.ldm.relational.Datatype")).append(sepa)
						.append(APIUtils.getValue(or, "com.infa.ldm.relational.DatatypeLength")).append(sepa)
						.append(APIUtils.getValue(or, "com.infa.ldm.relational.DatatypeScale"));
				// experiemental - table level...
				dbTableLine.append(dbName).append(sepa).append(schemaName).append(sepa).append(tableName).append(sepa)
						.append("").append(sepa).append("").append(sepa).append("").append(sepa).append("");
				// );

				// Note: also put the table name (no column or scale etc...

				if (this.includeAxonTermLink) {
					// check if there is a linked axon object - if so - we need to make another call
					// to get the name...
					for (LinkedObjectResponse lr : or.getSrcLinks()) {
						if (lr.getAssociation().equals("com.infa.ldm.axon.associatedGlossaries")) {
							axonLinks++;
							// System.out.println(or.getId()+": axon object :"+lr.getId());
							axonId = lr.getId();
							axonName = lr.getName();
						}
					}
					// adds blank columns where no axon terms are linked
					dbStructureLine.append(sepa).append(axonName);
					dbStructureLine.append(sepa).append(axonId);

					dbTableLine.append(sepa).append("");
					dbTableLine.append(sepa).append("");

				}

				// write the column structure info to the array (for sorting and finally writing
				// to file)
				dbStructLines.add(dbStructureLine.toString());

			} // iterator - items in the returned 'page'
		} // end of all objects queried

		// dbStructLines has all of the content
		long sortStart = System.currentTimeMillis();
		Collections.sort(dbStructLines);
		long sortEnd = System.currentTimeMillis();
		System.out.println("\tcolumns processed " + dbStructLines.size() + " skipped:" + skipped + " sort time:"
				+ (sortEnd - sortStart) + "milliseconds");
		if (includeAxonTermLink) {
			System.out.println("\tAxon terms linked: " + axonLinks);
		}
		// System.out.println("lines are sorted now..." + (sortEnd-sortStart) +
		// "milliseconds");
		long end = System.currentTimeMillis();
		long totalMillis = end - start;
		String timeTaken = String.format("%d min, %d sec", TimeUnit.MILLISECONDS.toMinutes(totalMillis),
				TimeUnit.MILLISECONDS.toSeconds(totalMillis)
						- TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(totalMillis)));

		System.out.println("\tdb structure extract finished in " + timeTaken);

		return dbStructLines;
	} // end getResourceStructure

	/**
	 * alternate method for extracting db resource structure contents instead of
	 * using GET for all objects (columns, view columns) use relationships from
	 * db->schema - then process each schema in alpha order getting tables/views &
	 * columns for each schema
	 * 
	 * in theory this should be faster because only specific
	 * relationships/attributes are returned instead of all facts/relationships
	 * using the get method
	 * 
	 * update: now also looks for Synonym|PublicSynonym objects in a schema for
	 * parity with the search method (for all columns)
	 * 
	 * @param resourceName
	 * @param pageSize
	 * @return List<String> with the database structure (sorted) - the caller can
	 *         then write to file as needed
	 * @throws Exception
	 */
	public List<String> getResourceStructureUsingRel(String resourceName, int pageSize) throws Exception {
		// use get for the database(s) for the resource
		// from there we can get a list of related schema objects
		// sort the list of schema id's - since the output needs to be sorted

		// then use relationships to get tables/columns, views/columns and any required
		// attributes for the column objects
		// Note: for databases with over 10k columns - this technique is slower
		// testing processing for each table
		// processing multiple large schemas should then respond more quickly than 1
		// call for all objects
		long start = System.currentTimeMillis();
		List<String> dbStructLines = new ArrayList<String>();
		String dbName;
		String dbId;
		String schId;
		String schemaName;
		String tableName;
		int count = 0;
		String dbQuery = "core.resourceName:" + resourceName + " and core.classType:com.infa.ldm.relational.Database";

		// Note: there is a bug here. the search adds a wildcard - so finding resource
		// AdventureWorks will also return AdventureWorks2014
		// quick work-around is to compare the found object id - with the resource name

		int total = 1000;
		int offset = 0;
		count = 0;
		int axonLinks = 0;

		// for each page
		while (offset < total) {
			// EDC (client.jar) <=10.2.1
			// ObjectsResponse response=APIUtils.READER.catalogDataObjectsGet(dbQuery, null,
			// BigDecimal.valueOf(offset), BigDecimal.valueOf(pageSize), false);
			// EDC (client.jar) 10.2.2 (+ 10.2.2 sp1)
			// ObjectsResponse response=APIUtils.READER.catalogDataObjectsGet(dbQuery, null,
			// offset, pageSize, null, null);
			// 10.2.2hf1+
			// ObjectsResponse response =
			// APIUtils.CATALOG_API.catalogDataObjectsGet(dbQuery, null, null, offset,
			// pageSize,
			// null, null, null, null, null, null, null);
			// EDC (client.jar) 10.4+
			ObjectsResponse response = APIUtils.CATALOG_API.catalogDataObjectsGet(dbQuery, null, null, offset, pageSize,
					null, null, null, null, true, true, null, null);

			total = response.getMetadata().getTotalCount().intValue();
			offset += pageSize;

			int dbCols = 0;
			count = 0;

			// Iterate over the database objects found
			for (ObjectResponse or : response.getItems()) {
				dbId = or.getId();
				dbName = APIUtils.getValue(or, APIUtils.CORE_NAME);
				// get the resource name for the database - compare it to resourceName filter
				// condition
				if (!dbId.startsWith(resourceName + "://")) {
					System.out.println("\tskipping database: " + dbName + " id=" + dbId + " different resource");
					// go the the next object - skipping all subsequent processing
					continue;
				}
				System.out.println("\tprocessing database: " + dbName + " id=" + dbId);

				// look at each dstLink - specifically for core.DirectionalDataFlow
				for (LinkedObjectResponse lr : or.getDstLinks()) {
					// check the association type
					if (lr.getAssociation().equals("com.infa.ldm.relational.DatabaseSchema")) {
						dbCols = 0;
						schemaName = lr.getName();
						schId = lr.getId();

						System.out.println("\t\tprocessing schema: " + schemaName + " id=" + schId);

						// list all tables for the schema (performance test)
						// make the relationship call - schema->table|view->column|viewcolumn

						ArrayList<String> seedIds = new ArrayList<String>();
						seedIds.add(schId);
						ArrayList<String> linksToFollow = new ArrayList<String>();
						// linksToFollow.add("core.ParentChild");
						linksToFollow.add("com.infa.ldm.relational.SchemaTable");
						linksToFollow.add("com.infa.ldm.relational.SchemaView");
						linksToFollow.add("com.infa.ldm.relational.TableColumn");
						linksToFollow.add("com.infa.ldm.relational.ViewViewColumn");
						// also follow SchemaSynonym and SchemaPublicSynonym
						linksToFollow.add("com.infa.ldm.relational.SchemaPublicSynonym");
						linksToFollow.add("com.infa.ldm.relational.PublicSynonymColumn");
						linksToFollow.add("com.infa.ldm.relational.SchemaSynonym");
						linksToFollow.add("com.infa.ldm.relational.SynonymColumn");

						ArrayList<String> attrsToReturn = new ArrayList<String>();
						attrsToReturn.add("core.name");
						attrsToReturn.add("core.classType");
						attrsToReturn.add("com.infa.ldm.relational.Datatype");
						attrsToReturn.add("com.infa.ldm.relational.DatatypeLength");
						attrsToReturn.add("com.infa.ldm.relational.DatatypeScale");
						// if axon
						if (includeAxonTermLink) {
							attrsToReturn.add("com.infa.ldm.axon.associatedGlossaries");
						}

						// APIUtils.READER.catalogDataRelationshipsGetWithHttpInfo(seed, association,
						// depth, direction, removeDuplicateAggregateLinks, includeTerms,
						// includeAttribute)
						Links relResp = APIUtils.READER.catalogDataRelationshipsGet(seedIds, linksToFollow,
								BigDecimal.valueOf(2), "OUT", true, null, attrsToReturn, null);
						// System.out.println("lineage returned..." + relResp);
						for (Link l : relResp.getItems()) {
							if (l.getAssociationId().equals("com.infa.ldm.relational.TableColumn")
									|| l.getAssociationId().equals("com.infa.ldm.relational.ViewViewColumn")
									|| l.getAssociationId().equals("com.infa.ldm.relational.SynonymColumn")
									|| l.getAssociationId().equals("com.infa.ldm.relational.PublicSynonymColumn")) {
								// we have a column level object...
								String array[] = l.getInId().split("/");
								tableName = array[4];

								StringBuffer dbStructureLine = new StringBuffer();
								dbStructureLine.append(dbName).append(sepa).append(schemaName).append(sepa)
										.append(tableName).append(sepa)
										.append(getEmbeddedValue(l.getInEmbedded(), "core.name")).append(sepa)
										.append(getEmbeddedValue(l.getInEmbedded(), "com.infa.ldm.relational.Datatype"))
										.append(sepa)
										.append(getEmbeddedValue(l.getInEmbedded(),
												"com.infa.ldm.relational.DatatypeLength"))
										.append(sepa).append(getEmbeddedValue(l.getInEmbedded(),
												"com.infa.ldm.relational.DatatypeScale"));

								if (includeAxonTermLink) {
									String axonTermName = getEmbeddedValue(l.getInEmbedded(),
											"com.infa.ldm.axon.associatedGlossaries");
									String axonTermLink = getEmbeddedProjectedFrom(l.getInEmbedded(),
											"com.infa.ldm.axon.associatedGlossaries");
									if (axonTermName != null) {
										axonLinks++;
										dbStructureLine.append(sepa).append(axonTermName);
										dbStructureLine.append(sepa).append(axonTermLink);
									} else {
										dbStructureLine.append(sepa).append("");
										dbStructureLine.append(sepa).append("");

									}
								}

								dbStructLines.add(dbStructureLine.toString());

								count++;
								dbCols++;
							}
						}

						System.out.println("\t\t\tcolumns for schema=" + dbCols);
					}
				} // for each dstLink (database schema)
			}

		}
		System.out.println("\tcolumns processed: " + count);
		if (includeAxonTermLink) {
			System.out.println("\tAxon terms linked: " + axonLinks);
		}

		Collections.sort(dbStructLines);

		long end = System.currentTimeMillis();
		long totalMillis = end - start;
		String timeTaken = String.format("%d min, %d sec", TimeUnit.MILLISECONDS.toMinutes(totalMillis),
				TimeUnit.MILLISECONDS.toSeconds(totalMillis)
						- TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(totalMillis)));

		System.out.println("\tdb structure extract (relationships) finished in " + timeTaken);
		return dbStructLines;
	}

	public String getEmbeddedValue(EmbeddedObject obj, String name) {
		for (EmbeddedFact fact : obj.getFacts()) {
			if (name.equals(fact.getAttributeId())) {
				return fact.getValue();
			}
		}
		return null;
	}

	public String getEmbeddedProjectedFrom(EmbeddedObject obj, String name) {
		for (EmbeddedFact fact : obj.getFacts()) {
			if (name.equals(fact.getAttributeId())) {
				return fact.getProjectedFrom();
			}
		}
		return null;
	}

	/**
	 * write the whole db structure to file
	 * 
	 * @param fileName
	 * @param theStructure List<String> all lines of the file (sorted)
	 */
	protected void writeStructureToFile(String fileName, List<String> theStructure) {
		try {
			Path toPath = Paths.get(fileName);
			Charset charset = Charset.forName("UTF-8");
			Files.write(toPath, theStructure, charset);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * store a list of the id of every ExternalDatabase object for the resource this
	 * is used to filter any columns beloning to the ExternalDatabase Object - since
	 * they are not relevant to the db structure
	 * 
	 * @param resourceName
	 */
	private void getExternalDbIdsForResource(String resourceName) {
		String dbQuery = "core.resourceName:" + resourceName
				+ " and core.classType:com.infa.ldm.relational.ExternalDatabase";

		// Note: there is a bug here. the search adds a wildcard - so finding resource
		// AdventureWorks will also return AdventureWorks2014
		// quick work-around is to compare the found object id - with the resource name

		int total = 1000;
		int offset = 0;
		int pageSize = 100;
		try {
			// for each page
			while (offset < total) {
				// EDC (client.jar) <=10.2.1
				// ObjectsResponse response=APIUtils.READER.catalogDataObjectsGet(dbQuery, null,
				// BigDecimal.valueOf(offset), BigDecimal.valueOf(pageSize), false);
				// EDC (client.jar) 10.2.2 (+ 10.2.2 sp1)
				// ObjectsResponse response=APIUtils.READER.catalogDataObjectsGet(dbQuery, null,
				// offset, pageSize, null, null);
				// 10.2.2hf1+
				// ObjectsResponse response =
				// APIUtils.CATALOG_API.catalogDataObjectsGet(dbQuery, null, null, offset,
				// pageSize, null, null, null, null, null, null, null);
				// EDC (client.jar) 10.4+
				ObjectsResponse response = APIUtils.CATALOG_API.catalogDataObjectsGet(dbQuery, null, null, offset,
						pageSize, null, null, null, null, true, true, null, null);

				total = response.getMetadata().getTotalCount().intValue();
				offset += pageSize;

				// Iterate over the database objects found & add to the collection for
				// reference/lookup later
				for (ObjectResponse or : response.getItems()) {
					String dbId = or.getId();
					this.externalDbIds.add(dbId);
				}

			}

		} catch (Exception ex) {
			ex.printStackTrace();

		}

		System.out.println("\tfound extDbObjects: " + this.externalDbIds.size());

		return;
	}

	/**
	 * return true if the object belongs to an external database
	 * 
	 * @param anId the id to check
	 * @return true/false
	 */
	private boolean isIdFromExternalDB(String anId) {
		for (String extId : externalDbIds) {
			// System.out.println("checking " + anId + " with " + extId);
			if (anId.startsWith(extId)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * command-line calls start here (resource watcher will not use main()
	 * 
	 * @param args
	 */
	public static void main(String args[]) {
		long start = System.currentTimeMillis();
		System.out.println("db structure::start " + start);

		// read command line args
		String user;
		String pwd;
		String url;
		String version;
		String outFolder;
		String resourceName;
		Boolean includeAxonTerms = false;
		boolean filterOutExtDbItems = true;
		String dbExtractApiType = "objects"; // default to export using objects query vs relationships

		if (args.length == 0 && args.length < 5) {
			System.out.println(
					"DBStructureExtract: command-line arguments:-  [eic-url] [userid] [pwd] [resourcename] [outfolder] [versionlabel] [includeAxonTerms] [filterOutExternalDBObjects] [exportTechnique (objects|rels]");
			System.exit(0);
		}

		url = args[0];
		user = args[1];
		pwd = args[2];
		resourceName = args[3];
		outFolder = args[4];
		version = args[5];
		if (args.length >= 6) {
			includeAxonTerms = Boolean.parseBoolean(args[6]);
		}
		if (args.length > 7) {
			filterOutExtDbItems = Boolean.parseBoolean(args[7]);
		}

		if (args.length > 8) {
			if (args[8].equals("objects") || args[8].equals("rels")) {
				// valid
				dbExtractApiType = args[8];
			} else {
				System.out.println("extract type not equal to objects or rels - assuming objects");
				// no - need to set anything - since the default is already set
			}
			dbExtractApiType = args[8];
		}

		List<String> resources = new ArrayList<String>();
		resources.add(resourceName);

		if (!url.endsWith("/access/2")) {
			url = url + "/access/2";
		}

		System.out.println("DBStructureExtract:" + url + " user=" + user + " pwd=" + pwd.replaceAll(".", "*") + " "
				+ resourceName);

		// initialize the db stucture report
		DBStructureExport rep = new DBStructureExport(url, user, pwd);
		rep.setIncludeAxonTermLinks(includeAxonTerms);
		if (!filterOutExtDbItems) {
			rep.setExcludeExtDbObjects(filterOutExtDbItems);
		}
		List<String> dbStructureLines;
		try {
			// old way (slow but has no problems with volume)
			if (dbExtractApiType.equalsIgnoreCase("objects")) {
				System.out.println("\tcalling  getResourceStructure");
				dbStructureLines = rep.getResourceStructure(resourceName, 500);
			} else {
				System.out.println("\tcalling  getResourceStructureUsingRel");
				dbStructureLines = rep.getResourceStructureUsingRel(resourceName, 500);
			}

			String fileName = outFolder + "/" + resourceName + "_" + version + ".txt";
			System.out.println("writing file to: _" + fileName);
			rep.writeStructureToFile(fileName, dbStructureLines);

		} catch (Exception e) {
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();
		long totalMillis = end - start;
		String timeTaken = String.format("%d min, %d sec", TimeUnit.MILLISECONDS.toMinutes(totalMillis),
				TimeUnit.MILLISECONDS.toSeconds(totalMillis)
						- TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(totalMillis)));

		System.out.println("db structure::end " + end + " elapsed: " + timeTaken);

	}

}
