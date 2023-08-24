/**
 * 
 */
package com.infa.eic.sample;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import com.infa.products.ldm.core.rest.v2.client.invoker.ApiException;
import com.infa.products.ldm.core.rest.v2.client.invoker.ApiResponse;
import com.infa.products.ldm.core.rest.v2.client.models.EmbeddedFact;
import com.infa.products.ldm.core.rest.v2.client.models.FactRequest;
import com.infa.products.ldm.core.rest.v2.client.models.Link;
import com.infa.products.ldm.core.rest.v2.client.models.LinkedObjectRequest;
import com.infa.products.ldm.core.rest.v2.client.models.Links;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectIdRequest;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectResponse;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectsResponse;
import com.infa.products.ldm.core.rest.v2.client.utils.ObjectAdapter;

/**
 * @author dwrigley
 *
 */
public class ModelLinker {
	public static final String version = "1.3";

	String url = "";
	String user = "";
	String pwd = "";
	String entityQuery = "";
	String entityFQ = "";
	String tableQuery = "";
	String physNameAttr = "";
	String entityAttrLink = "";
	String tableToColLink = "";
	String logFile = "";
	String lineageFile = "";

	ArrayList<String> entityAttrLinks = new ArrayList<String>();
	ArrayList<String> physNameAttrs = new ArrayList<String>();
	PrintWriter logWriter;
	PrintWriter lineageWriter;

	boolean deleteLinks = false;
	boolean testOnly = true;
	List<String> replaceChars = new ArrayList<String>();

	private static int totalLinks = 0;
	private static int existingLinks = 0;
	private static int deletedLinks = 0;

	private static int datasetLineageLinks = 0;
	private static int elementLineageLinks = 0;

	private boolean doAttributePropagation = false;
	private String attributesToPropagate = "";
	Map<String, String> attrPropMap = new HashMap<String, String>();

	private String entityLinkType = "";
	private String attributeLinkType = "";

	private boolean useOwnerSchema = false;
	private String ownerSchemaAttr = "";

	/**
	 * no-arg constructor
	 */
	public ModelLinker() {
	};

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// default property file - catalog_utils.properities (in current folder)
		String propertyFile = "catalog_utils.properties";

		if (args.length == 0) {
			// assume default property file (initial value of propertyFile)
		} else {
			propertyFile = args[0];
		}

		ModelLinker mdl = new ModelLinker(propertyFile);
		mdl.run();
		mdl.finish();

	}

	/**
	 * constructor - initialize using the property file passed
	 * 
	 * @param propertyFile
	 */
	public ModelLinker(String propertyFile) {
		// System.out.println("Constructor:" + propertyFile);
		System.out.println(
				this.getClass().getSimpleName() + " " + version + " initializing properties from: " + propertyFile);

		// read the settings needed to control the process from the property file passed
		// as arg[0]
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
			if (pwd.equals("<prompt>") || pwd.isEmpty()) {
				System.out.println("password set to <prompt> - waiting for user input...");
				pwd = APIUtils.getPassword();
				// System.out.println("pwd entered (debug): " + pwd);
			}

			// model linker properties

			entityQuery = prop.getProperty("modelLinker.entityQuery");
			entityFQ = prop.getProperty("modelLinker.entityFQ");
			tableQuery = prop.getProperty("modelLinker.tableQuery");
			physNameAttr = prop.getProperty("modelLinker.physicalNameAttr");
			physNameAttrs = new ArrayList<String>(Arrays.asList(physNameAttr.split(",")));
			entityAttrLink = prop.getProperty("modelLinker.entityToAttrLink");
			entityAttrLinks = new ArrayList<String>(Arrays.asList(entityAttrLink.split(",")));
			tableToColLink = prop.getProperty("modelLinker.tableToColLink");
			deleteLinks = Boolean.parseBoolean(prop.getProperty("modelLinker.deleteLinks"));
			logFile = prop.getProperty("modelLinker.logfile", "modellog.log");
			lineageFile = prop.getProperty("modelLinker.lineageFile", "model_lineage.csv");

			useOwnerSchema = Boolean.parseBoolean(prop.getProperty("modelLinker.useOwnerSchema", "false"));
			ownerSchemaAttr = prop.getProperty("modelLinker.ownerSchemaAttr", "");

			entityLinkType = prop.getProperty("modelLinker.entityLinkType", "core.DataSetDataFlow");
			attributeLinkType = prop.getProperty("modelLinker.attributeLinkType", "core.DirectionalDataFlow");

			// termType = prop.getProperty("fuzzyLink.termType");

			// BG_RESOURCE_NAME=prop.getProperty("fuzzyLink.bgResourceName");
			// logFile=prop.getProperty("fuzzyLink.logfile");
			// linkResultFile=prop.getProperty("fuzzyLink.linkResults");
			// searchThreshold=Integer.parseInt(prop.getProperty("fuzzyLink.searchThreshold"));
			testOnly = Boolean.parseBoolean(prop.getProperty("modelLinker.testOnly"));
			// String repChars = prop.getProperty("fuzzyLink.replaceWithSpace", "");
			// if (! repChars.equalsIgnoreCase("")) {
			// only if there are characters to replace
			// replaceChars = new ArrayList<String>(Arrays.asList(repChars.split(",")));
			// }

			doAttributePropagation = Boolean
					.parseBoolean(prop.getProperty("modelLinker.attributePropagation", "false"));
			attributesToPropagate = prop.getProperty("modelLinker.attributesToPropagate", "");
			// split and create a hashmap
			if (attributesToPropagate.indexOf(",") > 0) {
				String[] attrtoprop = attributesToPropagate.split(",");
				for (String s : attrtoprop) {
					// System.out.println(s);
					// now split by : should give exactly 2 entries
					String[] leftRight = s.trim().split(":");
					if (leftRight.length == 2) {
						this.attrPropMap.put(leftRight[0], leftRight[1]);
					} else {
						System.out.println("attribute propagation does not have a left:right attribute entry");
					}
					// System.out.println(leftRight);
				}

			}

		} catch (Exception e) {
			System.out.println("error reading properties file: " + propertyFile);
			e.printStackTrace();
		}

		System.out.println("     EDC rest url: " + url);
		System.out.println("      entityQuery: " + entityQuery);
		System.out.println("      entity FQ: " + entityFQ);
		System.out.println("       tableQuery: " + tableQuery);
		System.out.println("     physNameAttr: " + physNameAttr);
		System.out.println("    physNameAttrs: " + physNameAttrs);
		System.out.println("   entityAttrLink: " + entityAttrLink);
		System.out.println(" use owner Schema: " + useOwnerSchema);
		System.out.println("     owner Schema: " + ownerSchemaAttr);
		System.out.println("  entityAttrLinks: " + entityAttrLinks);
		System.out.println("   tableToColLink: " + tableToColLink);
		System.out.println("   entityLinkType: " + entityLinkType);
		System.out.println("attributeLinkType: " + attributeLinkType);
		System.out.println("     delete links: " + deleteLinks);
		System.out.println("         log file: " + logFile);
		System.out.println("        test mode: " + testOnly);
		System.out.println(" attr Propagation: " + doAttributePropagation);
		System.out.println("    attrs to copy: " + attrPropMap);

		try {

			System.out.println("\tinitializing logFile:" + logFile);

			logWriter = new PrintWriter(logFile, "UTF-8");
			logWriter.println("Model Linker: version=" + version);
			logWriter.println("     EDC rest url: " + url);
			logWriter.println("      entityQuery: " + entityQuery);
			logWriter.println("       tableQuery: " + tableQuery);
			logWriter.println("     physNameAttr: " + physNameAttr);
			logWriter.println(" use owner Schema: " + useOwnerSchema);
			logWriter.println("     owner Schema: " + ownerSchemaAttr);
			logWriter.println("    physNameAttrs: " + physNameAttrs);
			logWriter.println("   entityAttrLink: " + entityAttrLink);
			logWriter.println("  entityAttrLinks: " + entityAttrLinks);
			logWriter.println("   tableToColLink: " + tableToColLink);
			logWriter.println("   entityLinkType: " + entityLinkType);
			logWriter.println("attributeLinkType: " + attributeLinkType);
			logWriter.println("     delete links: " + deleteLinks);
			logWriter.println("         log file: " + logFile);
			logWriter.println("        test mode: " + testOnly);
			logWriter.println(" attr Propagation: " + doAttributePropagation);
			logWriter.println("    attrs to copy: " + attrPropMap);

			logWriter.flush();

			System.out.println("\tinitializing lineageFile:" + lineageFile);
			lineageWriter = new PrintWriter(lineageFile, "UTF-8");
			lineageWriter.println("Association,From Connection,To Connection,From Object,To Object");
			lineageWriter.flush();

			// linkResultCSV = new CSVWriter(new FileWriter(linkResultFile),
			// CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER,
			// CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
			// linkResultCSV.writeNext(new String[]
			// {"fromObjectId","linkedTerm","TermName"});
			// linkResultCSV.flush();

		} catch (IOException e1) {
			e1.printStackTrace();
			e1.printStackTrace(lineageWriter);
		}

	}

	/**
	 * run the model linker processes
	 */
	private void run() {
		// System.out.println("modelLinker::run()");
		logWriter.println("\nModelLinker Starting");

		int errorsFound = 0;
		// String entityComment = null;
		// String entityDefinition = null;
		int objectsToUpdate = 0;
		int objectsUpdated = 0;

		// Connect to the EIC REST Instance
		try {
			APIUtils.setupOnce(url, user, pwd);

			int total = 1;
			int offset = 0;
			final int pageSize = 20;

			System.out.println("entity objects.. using query= " + entityQuery);
			logWriter.println("entity objects.. using query= " + entityQuery);

			String entityName = "";
			String entityId = "";
			String physicalName = "";
			Map<String, String> leftValues = new HashMap<String, String>();
			Map<String, String> rightValues = new HashMap<String, String>();

			while (offset < total) {
				// EDC (client.jar) <=10.2.1
				// // ObjectsResponse
				// response=APIUtils.READER.catalogDataObjectsGet(entityQuery, null,
				// BigDecimal.valueOf(offset), BigDecimal.valueOf(pageSize), false);
				// EDC (client.jar) 10.2.2 (+ 10.2.2 sp1) - deprecated but works with 10.2.2hf1+
				// // ObjectsResponse
				// response=APIUtils.READER.catalogDataObjectsGet(entityQuery, null, offset,
				// pageSize, null, null);
				// EDC (client.jar) 10.2.2hf1+ (works and returns same values)

				// need to add an FQ
				// List<String> fq_array = Arrays.asList(entityFQ);
				ArrayList<String> fq_array = new ArrayList<String>();
				fq_array.add(entityFQ);

				ObjectsResponse response = APIUtils.CATALOG_API.catalogDataObjectsGet(entityQuery, fq_array, null,
						offset,
						pageSize, null, null, null, null, true, true, null, null);

				total = response.getMetadata().getTotalCount().intValue();
				offset += pageSize;
				System.out.println("Entities found: " + total);
				logWriter.println("Entities found: " + total);

				// process each object returned in this chunk
				for (ObjectResponse or : response.getItems()) {
					// System.out.println("Entity: " + or.getId());
					leftValues.clear();
					rightValues.clear();

					entityId = or.getId();
					entityName = APIUtils.getValue(or, "core.name");
					physicalName = getPhysicalNameAttr(or, physNameAttrs);
					if (physicalName == null) {
						// use the object name
						// this happens when it is a physical only model (the entity/table name is the
						// name)
						System.out.println("\twarning: no physical name found, using object name = " + entityName);
						logWriter.println("\twarning: no physical name found, using object name = " + entityName);
						physicalName = entityName;
					}
					// physicalName = APIUtils.getValue(or, physNameAttr);
					System.out
							.println("Entity: " + or.getId() + " name=" + entityName + " physicalName=" + physicalName);
					logWriter
							.println("Entity: " + or.getId() + " name=" + entityName + " physicalName=" + physicalName);

					if (doAttributePropagation) {
						leftValues = getObjectAttributesFromResponse(this.attrPropMap.keySet().iterator(), or);
						System.out.println("\t\t\tfound existing attributes (from modelS): " + leftValues);
					}
					// find the corresponding table

					// assume only a small set of tables could be returned (name_lc_exact is no
					// longer used)
					// String findTables = tableQuery + " AND core.name_lc_exact:\"" + physicalName
					// + "\"";

					// Note: since v 10.4.x you cannot use core.name_lc_exact

					// 10.22hf1+
					String findTables = tableQuery + " +core.name:\"" + physicalName + "\"";
					if (useOwnerSchema) {
						String entitySchema = APIUtils.getValue(or, ownerSchemaAttr);
						System.out.println("Owner Schema is: " + entitySchema);
						logWriter.println("Owner Schema is: " + entitySchema);
						if (entitySchema != null) {
							// assume case insensitive autoSuggestMatchId
							findTables = tableQuery + " +core.autoSuggestMatchId:/" + entitySchema.toUpperCase() + "/"
									+ physicalName.toUpperCase();
						} else {
							System.out.println(
									"owner schema in model is null - cannot be used to find table " + physicalName);
							logWriter.println(
									"owner schema in model is null - cannot be used to find table " + physicalName);
						}
					}
					System.out.println("\tfinding table (exact name match): " + findTables);
					logWriter.println("\tfinding table (exact name match): " + findTables);
					// EDC (client.jar) <=10.2.1
					// ObjectsResponse tabsResp=APIUtils.READER.catalogDataObjectsGet(findTables,
					// null, BigDecimal.valueOf(0), BigDecimal.valueOf(10), false);
					// EDC (client.jar) 10.2.2 (+ 10.2.2 sp1) (note: deprecated in 10.2.2hf1+, but
					// still works)
					// ObjectsResponse tabsResp = APIUtils.READER.catalogDataObjectsGet(findTables,
					// null, 0, 10, null,
					// null);

					// EDC 10.4+
					ObjectsResponse tabsResp = APIUtils.CATALOG_API.catalogDataObjectsGet(findTables, null, null, 0, 10,
							null, null, null, null, true, true, null, null);

					int tabTotal = tabsResp.getMetadata().getTotalCount().intValue();
					System.out.println("\tTables found=" + tabTotal);
					logWriter.println("\tTables found=" + tabTotal);

					if (tabTotal > 1) {
						System.out.println(
								"\t> 1 table found - your query probably needs to filter by resourceName or resourceType.  first found will be used here");
						logWriter.println(
								"\t> 1 table found - your query probably needs to filter by resourceName or resourceType.  first found will be used here");
					}

					if (tabTotal >= 1) {
						ObjectResponse physTabResp = tabsResp.getItems().get(0);
						for (ObjectResponse resp : tabsResp.getItems()) {
							String objName = APIUtils.getValue(resp, "core.name");
							if (physicalName.equalsIgnoreCase(objName)) {
								System.out.println("match found...");
								physTabResp = resp;
							} else {
								System.out.println("skipping non-exact match... " + objName + " != " + physicalName
										+ " for id: " + resp.getId());
							}
						}
						// key - id:attr val = val
						Map<String, String> entAttrFacts = new HashMap<String, String>();
						// get the first table
						// ObjectResponse physTabResp = tabsResp.getItems().get(0);
						System.out.println("\tphysical table id: " + physTabResp.getId());

						System.out.println("\t\tlinking objects.... " + entityId + " -> " + physTabResp.getId());
						logWriter.println("\t\tlinking objects.... " + entityId + " -> " + physTabResp.getId());
						addDatasetLink(entityId, physTabResp.getId(), entityLinkType, deleteLinks);
						lineageWriter.println(entityLinkType + ",,," + entityId + "," + physTabResp.getId());
						datasetLineageLinks++;
						lineageWriter.flush();

						if (this.doAttributePropagation) {
							Map<String, String> attrsMap = new HashMap<String, String>();
							// check the display label and name

							rightValues = getObjectAttributesFromResponse(this.attrPropMap.values().iterator(),
									physTabResp);
							System.out.println("\t\t\tfound existing attributes (to dbms): " + rightValues);

							String axonLabelTerm = APIUtils.getValue(physTabResp,
									"com.infa.ldm.axon.associatedGlossaries");
							// System.out.println("axon term related=" + axonLabelTerm);
							if (axonLabelTerm != null) {
								System.out.println(
										"WARNING: Axon term already linked, name display name will not be propagated from model");
							} else {
								Iterator<String> lrattrs = this.attrPropMap.keySet().iterator();
								while (lrattrs.hasNext()) {
									String leftAttr = lrattrs.next();
									String rightAttr = attrPropMap.get(leftAttr);
									String leftVal = leftValues.get(leftAttr);
									String rightVal = rightValues.get(rightAttr);

									System.out.println("comparing: " + leftAttr + " -> " + rightAttr);
									System.out.println("comparing: " + leftVal + " -> " + rightVal);

									if (leftVal != null) {
										System.out.println("left value has contents - getting ready to update");
										if (rightVal != null) {
											if (!leftVal.equals(rightVal)) {
												System.out.println("replacing existing value");
												attrsMap.put(rightAttr, leftVal);
											} else {
												System.out.println("noting to do - values match");
											}
										} else {
											// adding new value
											attrsMap.put(rightAttr, leftVal);

										}
									}
								}

								if (attrsMap.size() > 0) {
									objectsToUpdate++;
									if (updateObjectFacts(physTabResp.getId(), attrsMap)) {
										objectsUpdated++;
									} else {
										errorsFound++;
									}
								}

							}

						} // end if do attribute propagation

						// get the columns - for the entity and the table - then match them...
						System.out.println("\tget attrs for entity: using link attribute: " + entityAttrLink);
						logWriter.println("\tget attrs for entity: using link attribute: " + entityAttrLink);

						ArrayList<String> attrs = new ArrayList<>();
						attrs.addAll(physNameAttrs);
						attrs.add("core.name");

						// also add the attributes that need to be checked for propagation
						attrs.addAll(attrPropMap.keySet());
						// System.out.println("attrs to read=" + attrs);

						Links entLinks = APIUtils.CATALOG_API.catalogDataRelationshipsGet(
								// Links entLinks = APIUtils.READER.catalogDataRelationshipsGet(
								new ArrayList<String>(Arrays.asList(entityId)), entityAttrLinks, BigDecimal.ZERO, "OUT",
								true, false, attrs, null);
						// System.out.println("links=" + entLinks);
						Map<String, String> entityAttrs = new HashMap<String, String>();
						for (Link entAttrLink : entLinks.getItems()) {
							String attrName = getValueFromEmbeddedFact("core.name",
									entAttrLink.getInEmbedded().getFacts());
							;
							String attrPhysName = "";
							for (String nameAttr : physNameAttrs) {
								attrPhysName = getValueFromEmbeddedFact(nameAttr,
										entAttrLink.getInEmbedded().getFacts());
								if (attrPhysName != null) {
									break;
								}
							}

							// collect the attributes to be propagated
							Iterator<String> lrattrs = this.attrPropMap.keySet().iterator();
							while (lrattrs.hasNext()) {
								String leftAttr = lrattrs.next();
								String leftVal = getValueFromEmbeddedFact(leftAttr,
										entAttrLink.getInEmbedded().getFacts());
								System.out.println("storing left attr fact: " + leftAttr + "=" + leftVal);
								entAttrFacts.put(entAttrLink.getInId() + ":" + leftAttr, leftVal);
							}

							// String attrPhysName = getValueFromEmbeddedFact(physNameAttr,
							// entAttrLink.getInEmbedded().getFacts());
							System.out.println("\t\tAttribute: " + entAttrLink.getInId() + " attr Name=" + attrName
									+ " physicalName=" + attrPhysName);
							logWriter.println("\t\tAttribute: " + entAttrLink.getInId() + " attr Name=" + attrName
									+ " physicalName=" + attrPhysName);

							entityAttrs.put(attrPhysName, entAttrLink.getInId());
						}
						System.out.println("\tattrMapKeys=" + entityAttrs.keySet());
						logWriter.println("\tattrMapKeys=" + entityAttrs.keySet());
						// all entit attrs are in the entityAttrs HashMap

						// now get the table columns
						// collect the list of attriutes to read
						Set<String> colAttrs = new HashSet<String>();
						colAttrs.addAll(attrPropMap.values());
						colAttrs.add("core.name"); // if not already there (set takes care of this)
						// colAttrs.add("com.infa.ldm.axon.associatedGlossaries"); // to check for axon
						// term linked already
						Links tabLinks = APIUtils.READER.catalogDataRelationshipsGet(
								new ArrayList<>(Arrays.asList(physTabResp.getId())),
								new ArrayList<>(Arrays.asList(tableToColLink)), BigDecimal.ZERO, "OUT", true, false,
								// new ArrayList<>(Arrays.asList("core.name"))
								new ArrayList<>(colAttrs), null);
						// System.out.println("table columns..." + tabLinks);
						for (Link tabColLink : tabLinks.getItems()) {
							String colName = getValueFromEmbeddedFact("core.name",
									tabColLink.getInEmbedded().getFacts());
							;
							System.out.println("\t\tColumn: " + tabColLink.getInId() + " attr Name=" + colName
									+ " match?" + entityAttrs.containsKey(colName));
							logWriter.println("\t\tColumn: " + tabColLink.getInId() + " attr Name=" + colName
									+ " match?" + entityAttrs.containsKey(colName));

							if (entityAttrs.containsKey(colName)) {
								String fromId = entityAttrs.get(colName);
								logWriter.println(
										"\t\t\tlinking objects... " + fromId + " -->> " + tabColLink.getInId());
								addDatasetLink(fromId, tabColLink.getInId(), attributeLinkType, deleteLinks);
								lineageWriter.println(
										attributeLinkType + ",,," + fromId + "," + tabColLink.getInId());
								elementLineageLinks++;

								lineageWriter.flush();

								if (this.doAttributePropagation) {
									// compare and copy the attributes
									// comparing to :
									System.out.println("comparing to ::" + entAttrFacts);
									Iterator<String> lrattrs = this.attrPropMap.keySet().iterator();
									Map<String, String> attrsMap = new HashMap<String, String>();
									while (lrattrs.hasNext()) {
										String leftAttr = lrattrs.next();
										String rightAttr = attrPropMap.get(leftAttr);
										String leftVal = entAttrFacts.get(fromId + ":" + leftAttr);
										String rightVal = getValueFromEmbeddedFact(rightAttr,
												tabColLink.getInEmbedded().getFacts());

										String axonLabelTerm = getValueFromEmbeddedFact(
												"com.infa.ldm.axon.associatedGlossaries",
												tabColLink.getInEmbedded().getFacts());
										if (axonLabelTerm != null && axonLabelTerm.length() > 0) {
											System.out.println(
													"WARNING: Axon term already linked, name display name will not be propagated from model");
										} else {
											System.out.println("column comp: is " + leftAttr + "::" + leftVal
													+ " == " + rightAttr + "::" + rightVal);
											if (leftVal != null && leftVal.length() > 0) {
												if (!leftVal.equals(rightVal)) {
													// copy it ...
													System.out.println("\tready to copy: is " + leftAttr + "::"
															+ leftVal + " == " + rightAttr + "::" + rightVal);
													attrsMap.put(rightAttr, leftVal);
												}
											}
										}
									}
									if (attrsMap.size() > 0) {
										System.out.println("\tcalling write attr for id= " + tabColLink.getInId() + " "
												+ attrsMap);
										this.updateObjectFacts(tabColLink.getInId(), attrsMap);
									}

								}

							} else {
								logWriter.println("\tError: cannot find matching column for " + colName);
								errorsFound++;
							}
							// entityAttrs.put(attrPhysName, entAttrLink.getInId());
						}

					} // table 1 table was found

					System.out.println("\tfinished with: " + entityId);
					logWriter.println("\tfinished with: " + entityId);
					logWriter.flush();
				} // for each object

			} // endwhile (catalog query)

		} catch (Exception e) {
			e.printStackTrace();
			e.printStackTrace(logWriter);
		}

		// try {
		System.out.println("finished... ");
		System.out.println("\tlineage links total=" + (datasetLineageLinks + elementLineageLinks)
				+ " " + entityLinkType + "=" + datasetLineageLinks + " " + attributeLinkType + "="
				+ elementLineageLinks);
		System.out.println("\t  objects to update=" + objectsToUpdate);
		System.out.println("\t    objects updated=" + objectsUpdated);
		System.out.println("\tlinks written via API=" + totalLinks + " links skipped(existing)=" + existingLinks
				+ " linksDeleted=" + deletedLinks);
		System.out.println("\terrors:  " + errorsFound);

		logWriter.println("finished... ");
		logWriter.println("\tlineage links total=" + (datasetLineageLinks + elementLineageLinks)
				+ " " + entityLinkType + "=" + datasetLineageLinks + " " + attributeLinkType + "="
				+ elementLineageLinks);
		logWriter.println("\t  objects to update=" + objectsToUpdate);
		logWriter.println("\t    objects updated=" + objectsUpdated);
		logWriter.println("\tlinks written via API=" + totalLinks + " links skipped(existing)=" + existingLinks
				+ " linksDeleted=" + deletedLinks);
		logWriter.println("\terrors:  " + errorsFound);

		// Path toPath = Paths.get(logFile);
		// Charset charset = Charset.forName("UTF-8");
		// Files.write(toPath, logList, charset);

		// write the linked objects too
		// toPath = Paths.get(linkResultFile);
		// Charset charset = Charset.forName("UTF-8");
		// Files.write(toPath, linkedObj, charset);

		// fuzzySearchCSV.close();
		// linkResultCSV.close();

		// } catch (IOException e) {
		// e.printStackTrace();
		// }

	}

	private Map<String, String> getObjectAttributesFromResponse(Iterator<String> attrNames, ObjectResponse or) {
		// Iterator<String> iter = this.attrPropMap.keySet().iterator();
		Map<String, String> returnedAttrs = new HashMap<String, String>();
		// for (String attrName: attrNames) {
		while (attrNames.hasNext()) {
			String attrName = attrNames.next();
			String attrVal = APIUtils.getValue(or, attrName);
			if (attrVal != null) {
				returnedAttrs.put(attrName, attrVal);
			}
		}
		return returnedAttrs;
	}

	private void finish() {
		logWriter.close();
	}

	private static String getPhysicalNameAttr(ObjectResponse or, List<String> attrNames) {
		String theName = "";
		for (String nameAttr : attrNames) {
			// System.out.println("checking attr=" + nameAttr);
			theName = APIUtils.getValue(or, nameAttr);
			// System.out.println("name=" + theName);
			if (theName != null) {
				break;
			}
		}
		// System.out.println("returning: " + theName);
		return theName;
	}

	private static String getValueFromEmbeddedFact(String physNameAttr, ArrayList<EmbeddedFact> theFacts) {
		String attrName = "";
		for (EmbeddedFact aFact : theFacts) {
			if (physNameAttr.equals(aFact.getAttributeId())) {
				attrName = aFact.getValue();
				// System.out.println("name=" + aFact.getValue());
			}
		}
		return attrName;
	}

	/**
	 * Utility method to get hash key from value. Works best with unique values.
	 * 
	 * @param map
	 * @param value
	 * @return
	 */
	public static <T, E> T getKeyByValue(Map<T, E> map, E value) {
		for (Entry<T, E> entry : map.entrySet()) {
			if (Objects.equals(value, entry.getValue())) {
				return entry.getKey();
			}
		}
		return null;
	}

	/**
	 * Add a TABLE lineage link between the given two objects
	 * 
	 * @param sourceDatasetObjectID
	 * @param targetDatasetObjectID
	 * @throws Exception
	 */
	public void addDatasetLink(String sourceDatasetObjectID, String targetDatasetObjectID, String linkType,
			boolean removeLink) throws Exception {
		// System.out.println("link objects: from: " + sourceDatasetObjectID + " to:" +
		// targetDatasetObjectID + " link:" + linkType + " removeLink=" + removeLink);

		if (!testOnly) {
			// get the to object - by id...
			ApiResponse<ObjectResponse> apiResponse = null;
			ApiResponse<ObjectResponse> fromObjectResp = null;
			ObjectIdRequest request = null;
			LinkedObjectRequest link = null;
			boolean isLinkedAlready = false;
			try {
				// System.out.println("\tvalidating target object: " + targetDatasetObjectID);
				apiResponse = APIUtils.READER.catalogDataObjectsIdGetWithHttpInfo(targetDatasetObjectID);
				request = ObjectAdapter.INSTANCE.copyIntoObjectIdRequest(apiResponse.getData());

				// check for existing links
				ArrayList<LinkedObjectRequest> srcObjects = request.getSrcLinks();
				// System.out.println("\tlinks from to target object already existing..." +
				// srcObjects.size());
				int index = 0;
				int remIndex = 0;
				// boolean isLinkedAlready=false;
				for (LinkedObjectRequest or : srcObjects) {
					// System.out.println("\tlinked object id=" + or.getId() + "linktype=" +
					// or.getAssociation());
					if (or.getId().equals(sourceDatasetObjectID) && or.getAssociation().equals(linkType)) {
						isLinkedAlready = true;
						if (removeLink) {
							remIndex = index;
						}
						existingLinks++;
					}
					index++;

				} // iterator - items in the returned 'page'
					// if we get here - the object is not already linked...
				if (removeLink) {
					int removed = 0;
					if (isLinkedAlready) {
						System.out.println("\tremoving link..." + remIndex);
						logWriter.println("\tremoving link..." + remIndex);
						request.getSrcLinks().remove(remIndex);
						String ifMatch;
						try {
							ifMatch = APIUtils.READER.catalogDataObjectsIdGetWithHttpInfo(targetDatasetObjectID)
									.getHeaders().get("ETag").get(0);

							ObjectResponse newor = APIUtils.WRITER.catalogDataObjectsIdPut(targetDatasetObjectID,
									request, ifMatch);

							System.out.println(newor);

							System.out.println("\tLink Removed between:" + sourceDatasetObjectID + " AND "
									+ targetDatasetObjectID);
							logWriter.println("\tLink Removed between:" + sourceDatasetObjectID + " AND "
									+ targetDatasetObjectID);
							removed++;
							deletedLinks++;
						} catch (ApiException e) {
							e.printStackTrace();
						}

					}
					System.out.println("\tinks removed... " + removed);
					return;
				}

			} catch (Exception ex) {
				System.out.println(
						"Error finding target object: " + targetDatasetObjectID + " message: " + ex.getMessage());
				logWriter.println(
						"Error finding target object: " + targetDatasetObjectID + " message: " + ex.getMessage());
				if (apiResponse != null) {
					System.out.println(apiResponse.getStatusCode());
				}
				// ex.printStackTrace();

			}

			try {
				// System.out.println("\t\t\tvalidating source object: " +
				// sourceDatasetObjectID);
				fromObjectResp = APIUtils.READER.catalogDataObjectsIdGetWithHttpInfo(sourceDatasetObjectID);

			} catch (Exception ex) {
				System.out.println(
						"Error finding source object: " + sourceDatasetObjectID + " message: " + ex.getMessage());
				logWriter.println(
						"Error finding source object: " + sourceDatasetObjectID + " message: " + ex.getMessage());
				if (apiResponse != null) {
					System.out.println(apiResponse.getStatusCode());
				}
				// ex.printStackTrace();

			}

			if (apiResponse == null || fromObjectResp == null) {
				// exit
				System.out.println("no links created - cannot find source or target object");
				logWriter.println("no links created - cannot find source or target object");
				return;
			}

			try {
				link = new LinkedObjectRequest();
				link.setAssociation(linkType);
				link.setId(sourceDatasetObjectID);

				request.addSrcLinksItem(link);

				// System.out.println("json=<<");
				// System.out.println(request.toString());
				// request.
				System.out.println("\t\t\talreadyLinked:" + isLinkedAlready);
				logWriter.println("\t\t\talreadyLinked:" + isLinkedAlready);
				// System.out.println("json=>>");
				// System.out.println(link.toString());

				if (!isLinkedAlready) {
					String ifMatch;
					try {
						ifMatch = APIUtils.READER.catalogDataObjectsIdGetWithHttpInfo(targetDatasetObjectID)
								.getHeaders().get("ETag").get(0);

						ObjectResponse newor = APIUtils.WRITER.catalogDataObjectsIdPut(targetDatasetObjectID, request,
								ifMatch);
						System.out.println(newor);
						System.out.println("Link Added between:" + sourceDatasetObjectID + " AND "
								+ targetDatasetObjectID + " ifMatch=" + ifMatch);
						logWriter.println("Link Added between:" + sourceDatasetObjectID + " AND "
								+ targetDatasetObjectID + " ifMatch=" + ifMatch);
						totalLinks++;
					} catch (ApiException e) {
						System.out.println("error reading from object to get ETag: id=" + targetDatasetObjectID);
						logWriter.println("error reading from object to get ETag: id=" + targetDatasetObjectID);
						e.printStackTrace();
					}
				}

			} catch (Exception ex) {
				System.out.println(
						"Error finding target object: " + targetDatasetObjectID + " message: " + ex.getMessage());
				logWriter.println(
						"Error finding target object: " + targetDatasetObjectID + " message: " + ex.getMessage());
				if (apiResponse != null) {
					System.out.println(apiResponse.getStatusCode());
				}
				// ex.printStackTrace();

			}

		} // test only
	}

	// get and update an object
	public boolean updateObjectFacts(String objectId, Map<String, String> attrContents) throws Exception {

		boolean updateSucceeded = false;

		System.out.println("\t\tupdating object attributes: " + objectId + " attrContents=" + attrContents);

		int total = 1;
		int offset = 0;
		final int pageSize = 1;

		while (offset < total) {
			// EDC (client.jar) <=10.2.1
			// ObjectsResponse response=APIUtils.READER.catalogDataObjectsGet(query, null,
			// BigDecimal.valueOf(offset), BigDecimal.valueOf(pageSize), false);
			// EDC (client.jar) 10.2.2 (+ 10.2.2 sp1)
			// ObjectsResponse response = APIUtils.READER.catalogDataObjectsGet(null,
			// new ArrayList<String>(Arrays.asList(objectId)), offset, pageSize, null,
			// null);

			// EDC 10.4+
			// catalogDataObjectsGet(q, fq, id, offset, pageSize, sort, relatedId, cursor,
			// associations,includeSrcLinks, includeDstLinks, shards, false);
			ObjectsResponse response = APIUtils.CATALOG_API.catalogDataObjectsGet(null, null,
					new ArrayList<String>(Arrays.asList(objectId)), 0, 10, null, null, null, null, true, true, null,
					null);

			total = response.getMetadata().getTotalCount().intValue();
			offset += pageSize;

			for (ObjectResponse or : response.getItems()) {

				// System.out.println("\t\tupdateing object attributes (" +
				// attrContents.keySet() + ") ..." + or.getId());

				// /**
				ObjectIdRequest request = ObjectAdapter.INSTANCE.copyIntoObjectIdRequest(or);

				Iterator<String> iter = attrContents.keySet().iterator();

				while (iter.hasNext()) {
					String attrName = iter.next();
					String attrVal = attrContents.get(attrName);
					String curVal = APIUtils.getValue(or, attrName);
					if (curVal != null) {
						System.out.println("removing existing value: " + curVal);
						request.getFacts().remove(new FactRequest().attributeId(attrName).value(curVal));
					}
					request.addFactsItem(new FactRequest().attributeId(attrName).value(attrVal));
				}

				String ifMatch;
				try {
					ifMatch = APIUtils.READER.catalogDataObjectsIdGetWithHttpInfo(or.getId()).getHeaders().get("ETag")
							.get(0);
					// ifMatch = "abcdfge";

					ObjectResponse newor = APIUtils.WRITER.catalogDataObjectsIdPut(or.getId(), request, ifMatch);
					// will throw exception if the upate does not work
					updateSucceeded = true;
					System.out.println("\t\t\tupdate completed successfully. " + newor.getId());
					logWriter.println("\t\t\tupdate completed successfully. " + newor.getId());

					// System.out.println(or.getId()+":"+newor);
				} catch (ApiException e) {
					System.out.println("ERROR: update failed: " + e.getMessage());
					logWriter.println("ERROR: update failed: " + e.getMessage());

					// e.printStackTrace();
				}

			} // end of item found
		} // end of while loop (query to get the object)

		return updateSucceeded;

	} // get and update object attrs

}
