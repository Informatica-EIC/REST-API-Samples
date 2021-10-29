package com.infa.eic.sample;

import java.io.BufferedReader;
import java.io.Console;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

import javax.swing.JOptionPane;
import javax.swing.JPasswordField;

import org.apache.commons.io.comparator.NameFileComparator;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class ResourceWatch {
	public static final String version = "1.33";

	protected static String DISCLAIMER = "\n************************************ Disclaimer *************************************\n"
			+ "By using this utility, you are agreeing to the following:-\n"
			+ "- this utility is not officially supported by Informatica\n"
			+ "  it was has been tested on many different dbms types and catalog versions (e.g. 10.2.1, 10.2.2)\n"
			+ "  but it may not work for all situations\n" + "- Issues can be created on githib:- \n"
			+ "  https://github.com/Informatica-EIC/REST-API-Samples  (java folder)\n"
			+ "*************************************************************************************\n" + "\n";

	Integer waitTime = 0;
	// Integer initialTime = 300;
	long startMillis;
	Connection con;
	Map<String, String> processedObjects = new HashMap<String, String>();
	String restURL;
	Logger logger;
	FileHandler fh;
	String glossary;
	String resourceTypes;
	String resourceFilter = "";
	String userName;
	String pwd;
	String dbsOutFolder;
	Integer pageSize = 100;
	boolean includeAxonTermLink = false;
	String dbExtractApiType = "objects";
	boolean excludeExternalDBObjects = true;

	public void setOutFolder(String theFolder) {
		this.dbsOutFolder = theFolder;
	}

	/**
	 * for querying the wait time (in main)
	 * 
	 * @return seconds that the system should wait before checking again 0 wait time
	 *         - means initialize - but do not begin the constant watch process
	 */
	protected int getWaitTime() {
		return waitTime.intValue();
	}

	List<String> resourcesTypesToMonitor = new ArrayList<String>();
	List<String> resourcesToWatch = new ArrayList<String>();
	List<String> resourcesToMonitor = new ArrayList<String>();
	Map<String, String> resourceJobMap = new HashMap<String, String>();
	DateFormat df = new SimpleDateFormat("yyyyMMdd'T'HHmmssX");

	/**
	 * map of job ids to job start times - for formatting db structure file names
	 */
	Map<String, String> jobStartTimes = new HashMap<String, String>();

	String propertyFileName;

	/**
	 * start the resource watch process keep running until killed
	 * 
	 * @param args property file that controls the process
	 */
	public static void main(String[] args) {
		if (args.length == 0) {
			System.out.println(
					"EIC Resource watcher: missing configuration properties file: usage:  ResourceWatch <folder>/config.properties");
		} else {
			System.out.println("EIC Resource watcher: " + args[0] + " currentTimeMillis=" + System.currentTimeMillis());

			String disclaimerParm = "";
			if (args.length >= 2) {
				// 2nd argument is an "agreeToDisclaimer" string
				disclaimerParm = args[1];
				// System.out.println("disclaimer parameter passed: " + disclaimerParm);
				if ("agreeToDisclaimer".equalsIgnoreCase(disclaimerParm)) {
					System.out.println(
							"the following disclaimer was agreed to by passing 'agreeToDisclaimer' as 2nd parameter");
					System.out.println(DISCLAIMER);
				}
			}

			if ("agreeToDisclaimer".equalsIgnoreCase(disclaimerParm) || showDisclaimer()) {

				// pass the property file - the constructor will read all input properties
				ResourceWatch watcher = new ResourceWatch(args[0]);
				if (watcher.resourcesTypesToMonitor.isEmpty()) {
					System.out.println("no resources found to watch, exiting");
					System.exit(0);
				}
				// call the watch process

				if (watcher.getWaitTime() > 0) {
					System.out.println("starting watch process for " + watcher.resourceJobMap.size() + " resources");
					watcher.watchForNewResourceJobsToComplete();
				} else {
					System.out.println("wait_time set to 0, exiting (no continuous monitoring)");
				}
			} else {
				System.out.println("Disclaimer was declined - exiting");
			}

		}
	}

	public ResourceWatch() {
	};

	/**
	 * constructor
	 * 
	 * @param propertyFile name/location of the propoerty file to use for
	 *                     controlling settings
	 */
	ResourceWatch(String propertyFile) {
		System.out.println(
				this.getClass().getSimpleName() + " " + version + " initializing properties from: " + propertyFile);

		// store the property file - for passing to the diff process (email properties)
		propertyFileName = propertyFile;

		System.out.println(this.getClass().getSimpleName() + " settings used");

		try {

			File file = new File(propertyFile);
			FileInputStream fileInput = new FileInputStream(file);
			Properties prop;
			prop = new Properties();
			prop.load(fileInput);
			fileInput.close();

			waitTime = Integer.parseInt(prop.getProperty("wait_time_seconds"));
			restURL = prop.getProperty("rest_service");
			resourceTypes = prop.getProperty("resourceTypesToWatch");
			resourceFilter = prop.getProperty("resourcesToWatch", "");
			userName = prop.getProperty("user");
			pwd = prop.getProperty("password");
			if (pwd.equals("<prompt>")) {
				System.out.println("password set to <prompt> for user " + userName + " - waiting for user input...");
				pwd = APIUtils.getPassword();
				System.out.println("pwd chars entered (debug):  " + pwd.length());
			}
			resourcesTypesToMonitor = new ArrayList<String>(Arrays.asList(resourceTypes.split(",")));
			// only add to resourcesToWatch - if an entry was made for resourceFilter
			if (resourceFilter.length() > 0) {
				resourcesToWatch = new ArrayList<String>(Arrays.asList(resourceFilter.split(",")));
			}

			dbExtractApiType = prop.getProperty("dbstruct.processtype", "objects");
			if (dbExtractApiType == null || dbExtractApiType.length() == 0) {
				System.out.println("no value set for dbstruct.processtype, using 'objects'");
				dbExtractApiType = "objects";
			}

			excludeExternalDBObjects = Boolean.parseBoolean(prop.getProperty("dbstruct.excludeExternalDBObjects"));
			if (!excludeExternalDBObjects) {
				System.out.println("Warning: external database columns will be exported");
			}

			pageSize = Integer.parseInt(prop.getProperty("pagesize", "300"));

			System.out.println("\tResource types filter: " + resourcesTypesToMonitor);
			System.out.println("\tResource names filter: " + resourcesToWatch);
			System.out.println("\t          catalog URL: " + restURL);
			System.out.println("\t                user : " + userName);
			System.out.println("\t     processing type : " + dbExtractApiType);

			includeAxonTermLink = Boolean.parseBoolean(prop.getProperty("includeAxonTermLink"));

			dbsOutFolder = prop.getProperty("dbstruct.outfolder");
			if (!Files.exists(Paths.get(dbsOutFolder), new LinkOption[] { LinkOption.NOFOLLOW_LINKS })) {
				System.out.println("path does not exist..." + dbsOutFolder + " exiting");
				// @todo - refactor separate the constructior from the initial phase of
				// connecting to eic
				System.exit(0);
			} else {

				// call initializeWatchProcess- gets a list of resources/jobs...
				this.initializeWatchProcess();

				System.out.println(""); // empty line
				System.out.println("initialize completed: jobs currently monitored" + resourceJobMap);

				System.out.println("eicResourceWatch=" + " wait_time=" + waitTime + " seconds" + " rest.service="
						+ restURL + " includeAxonTermLink=" + includeAxonTermLink);
				// logger.info("eicResourceWatch=" + "wait_time=" + waitTime + " seconds" + "
				// rest.service=" + restURL);
			}

		} catch (Exception e) {
			System.out.println("error reading properties file: " + propertyFile);
			e.printStackTrace();
		}

	}

	/**
	 * get a list of resources & jobs
	 * 
	 * gets a list of resources that should be monitored (ones in the
	 * resourceTypesToMonitor list) gets a list of the current job for each resource
	 * 
	 * the boolean passed to getJobList identifies the processing mode - false =
	 * startup mode means we not only get the current list of jobs for each
	 * monitored resource it will check to see if the structure has already been
	 * written to file for that resource and if not, will generate it if generating
	 * - it will also look for the most recent previous structure (based on file -
	 * that has a datestamp) and use that to pass to the diff process it will store
	 * a Map of key=resourceName val=jobid for the monitor mode to use - true =
	 * monitor mode the difference here is that we already know the previous jobs,
	 * so only look for new jobs (a new scan) to use
	 */
	private void initializeWatchProcess() {
		System.out.println("init watch service:-");
		boolean printToConsole = true;
		resourcesToMonitor = getResourceList(resourcesTypesToMonitor, printToConsole);

		System.out.println("\tget list of jobs (store with resources, as baseline or when new jobs are submitted");
		boolean monitorMode = false;

		// the rc here is the map that is the critical piece for on-going monitoring
		resourceJobMap = getJobList(monitorMode);

		return;
	}

	/**
	 * looks for new resources in eic to call the db structure compare/diff
	 * processes structure:
	 * 
	 * prints a . to the console - so you see that it is still running
	 * 
	 * @todo refactor to log to file (better for monitoring) after finishing will
	 *       sleep for <waitTime> seconds
	 */
	private void watchForNewResourceJobsToComplete() {
		startMillis = System.currentTimeMillis();
		// logger.info("Watching for eic resources to complete, every " + waitTime + "
		// seconds starting from: " + startMillis );
		System.out.println("Watching for eic resources to complete, every " + waitTime + " seconds");

		try {
			while (true) {
				// logger.info("looking for newly completed scans... since: " + startMillis);
				System.out.print(".");
				this.processWatchInterval();

				// startMillis = System.currentTimeMillis() - (waitTime *2)*1000;
				// System.out.println("keyset: " + processedObjects.keySet().toString());
				System.gc(); // clean up file locks Files.write(toPath, theStructure, charset); - does not
								// seem to release the lock
				// logger.info("sleeping for: " + waitTime + " seconds");
				Thread.sleep(waitTime * 1000);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	private List<String> resourcesInProgress = new ArrayList<String>();

	/**
	 * where the work really is - looks for changes.. if there are - calls the
	 * structure extract & compare
	 */
	private void processWatchInterval() {
		// get the current list of job ids for completed (or running???)

		// Note: this process does not detect new resources - so we also need to call
		// get
		// allResourcesToMonitor = getResourceList(resourcesTypesToMonitor);
		// then iterate through all results to see if there are any new - for each new,
		// need to call db structure extract
		// then continue to look for any changes for existing resources
		// System.out.println("checking to see if any new resources were created..." );
		List<String> allResourcesToMonitor = getResourceList(resourcesTypesToMonitor, false);
		for (String possibleNewResource : allResourcesToMonitor) {
			// System.out.println("testing: " + possibleNewResource + " in allresources: ");
			// System.out.println(allResourcesToMonitor.contains(possibleNewResource));

			if (!resourcesToMonitor.contains(possibleNewResource)) {
				// new resource...
				// System.out.println("need to add resource: " + possibleNewResource);
				resourcesToMonitor.add(possibleNewResource);

			}

		}

		// compare to the inital list - if different - then kick off the diff process
		// note the parameter passed to getJobList is a setting for beQuiet (true =
		// don't display connection messages)
		Map<String, String> currentJobMap = this.getJobList(true);
		// if a new resource and a job - then export
		// testing - add the resources from
		Set<String> allRes = new HashSet<String>();
		allRes.addAll(currentJobMap.keySet());
		allRes.addAll(resourceJobMap.keySet());
		// allRes - is a unique combination of both currently monitoryed resources + any
		// new resources with jobs
		// System.out.println("Allrez size=" + allRes.size() + " current:" +
		// resourceJobMap.keySet().size() + " new: " + currentJobMap.keySet().size());

		// any differences????
		// System.out.println("checking... " + currentJobMap.size());
		for (String resName : resourceJobMap.keySet()) {
			// check if the jobname is different....
			String prevJobId = resourceJobMap.get(resName);
			String currJobId = currentJobMap.get(resName);
			// System.out.println("previous job=" + prevJobId);
			// System.out.println("current job=" + currJobId);
			// System.out.println("interval check for " + resName + " old=" + prevJobId + "
			// new=" + currJobId + " matching???:" +
			// resourceJobMap.get(resName).equals(currentJobMap.get(resName)) );
			if (prevJobId != null && currJobId != null && !prevJobId.equals(currJobId)) {
				// !resourceJobMap.get(resName).equals(currentJobMap.get(resName))) {

				// if the current job is null - then the scan is still running...
				// could switch and only monitory for complete (iterate over current job map vs
				// resourceJobMap)
				// if (currentJobMap.get(resName)!=null) {
				if (allRes.contains(resName)) {
					// System.out.println("different jobs... time to kick off a new process..." +
					// resName + " currentJob=" + currentJobMap.get(resName));
					if (dbStructureExtract(resName, currentJobMap.get(resName))) {
						// update the jobid in the existing resource colleciton - so we don't call this
						// process again

						// + execute the diff report here too.....
						this.formatStructFileName(resName, resourceJobMap.get(resName));
						// File fromFile=new File(dbsOutFolder + "/" + resName + ":" +
						// resourceJobMap.get(resName) + ".txt");
						// File toFile =new File(dbsOutFolder + "/" + resName + ":" +
						// currentJobMap.get(resName) + ".txt");
						File fromFile = new File(formatStructFileName(resName, resourceJobMap.get(resName)));
						File toFile = new File(formatStructFileName(resName, currentJobMap.get(resName)));
						try {
							StructureDiff sd = new StructureDiff(fromFile, toFile, propertyFileName);
							sd.processDiffs();

						} catch (IOException e) {
							e.printStackTrace();
						}

						System.out.println(
								"\tupdating resource|job map key=" + resName + " value=" + currentJobMap.get(resName));
						resourceJobMap.put(resName, currentJobMap.get(resName));
						System.out.println("\tresuming watch process");

					}

				} else {
					// the resource dropped from 'completed' - so it is queued/running
					// only print the message 1 time - the resource could take a few cycles to
					// complete, but we don't want to print a message every time
					if (!this.resourcesInProgress.contains(resName)) {
						System.out.print("\nwaiting for " + resName + " to finish");
						resourcesInProgress.add(resName);
					}

				}
			} else {
				// old and new job names don't match
				if (prevJobId == null) {
					// no comp - but add the new job to the monitor list
					System.out.println("*********************************");
					resourceJobMap.put(resName, currJobId);
				}
			}

		}

		// logger.info("end of watch cycle");
	}

	/**
	 * common method for formatting the filename used for the db structure extract
	 * process format will be: resource:jobstartdate.txt
	 * 
	 * @param resourceName
	 * @param jobName
	 * @return filename including full path
	 * 
	 */
	private String formatStructFileName(String resourceName, String jobName) {
		String formattedName;
		String jobStart = this.jobStartTimes.get(jobName);
		if (jobStart == null) {
			jobStart = jobName;
		}
		formattedName = dbsOutFolder + "/" + resourceName + "__" + jobStart + ".txt";

		return formattedName;
	}

	/**
	 * calls the db Structure extract process
	 * 
	 * @param resourceName resource to get the structure for
	 * @param jobName      name of the job (the last scan)
	 * @return true if the file was created
	 */
	private boolean dbStructureExtract(String resourceName, String jobName) {
		boolean fileCreated = false;
		// get the job start time
		String jobStart = this.jobStartTimes.get(jobName);
		if (jobStart == null) {
			jobStart = jobName;
		}
		try {
			// format the path/name of the file that contains the db structure
			String fileName = this.formatStructFileName(resourceName, jobName);
			System.out.println("\n\t" + this.getClass().getSimpleName() + " calling db structure extract for: "
					+ resourceName + " job=" + jobName + " processingType=" + dbExtractApiType);
			// we need to add the /2 here - since this watcher uses v1 for resource stuff
			DBStructureExport dbs = new DBStructureExport(restURL + "/2", userName, pwd);
			dbs.setIncludeAxonTermLinks(includeAxonTermLink);
			if (!this.excludeExternalDBObjects) {
				// means that external objects will be included
				dbs.setExcludeExtDbObjects(excludeExternalDBObjects);
			}

			// call the structure export - depending on the technique that is configured
			List<String> dbStruct;
			if (dbExtractApiType == null || dbExtractApiType.equalsIgnoreCase("objects")) {
				System.out.println("\tcalling  getResourceStructure");
				dbStruct = dbs.getResourceStructure(resourceName, this.pageSize);
				// dbs.writeStructureToFile(fileName, dbStruct);
			} else {
				System.out.println("\tcalling  getResourceStructureUsingRel");
				dbStruct = dbs.getResourceStructureUsingRel(resourceName, this.pageSize);
				// dbs.writeStructureToFile(fileName, dbStruct);
			}
			// new case - if there are 0 records returned, don't write the results to file
			// (so no 0 byte files)
			if (dbStruct.size() > 0) {
				dbs.writeStructureToFile(fileName, dbStruct);
				fileCreated = true;
			} else {
				System.out.println("Error:  resource " + resourceName
						+ " has no datastructures extracted, no file will be created");
			}

			// return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		return fileCreated;
	}

	/**
	 * get a list of resources by filtering the resource type
	 * 
	 * @param resourceTypes (list of resource types)
	 * @return List of resources that match the filter condition
	 */
	protected ArrayList<String> getResourceList(List<String> resourceTypes, boolean printToConsole) {

		ArrayList<String> filteredResourceList = new ArrayList<String>();

		CredentialsProvider provider = new BasicCredentialsProvider();
		UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(userName, pwd);
		provider.setCredentials(AuthScope.ANY, credentials);

		HttpClient client = HttpClientBuilder.create().setDefaultCredentialsProvider(provider).build();

		String eicUrlV1CatalogResources = restURL + "/1/catalog/resources";
		if (printToConsole) {
			System.out.println("\tconnecting to... " + eicUrlV1CatalogResources + " user=" + this.userName);
		}

		HttpResponse response;
		try {
			response = client.execute(new HttpGet(eicUrlV1CatalogResources));
			int statusCode = response.getStatusLine().getStatusCode();
			if (printToConsole) {
				System.out.println("\tstatusCode=" + statusCode);
			}
			// System.out.println("\tresponse:" + response.toString());
			BufferedReader br = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));

			String output;
			StringBuffer json = new StringBuffer();
			// System.out.println("\tOutput from Server .... \n");
			while ((output = br.readLine()) != null) {
				// System.out.println("\tline: " + output);
				json.append(output);
			}

			Gson gson = new GsonBuilder().create();
			List<ResourceSimpleProps> resourceList;

			// System.out.println("\tjson response body=" + json);

			ResourceSimpleProps[] arr = gson.fromJson(json.toString(), ResourceSimpleProps[].class);
			resourceList = Arrays.asList(arr);

			if (resourceList != null) {
				if (printToConsole) {
					System.out.println("\tresources found__: " + resourceList.size() + " " + resourceList.toString());
				}
				for (ResourceSimpleProps res : resourceList) {
					// System.out.println("\t\tresourceName=" + res.resourceName + "
					// resourceTypeName=" +
					// res.resourceTypeName + " include=" +
					// resourcesTypesToMonitor.contains(res.resourceTypeName));

					if (this.resourcesTypesToMonitor.contains(res.resourceTypeName)) {
						// also filter on resource names
						if (this.resourcesToWatch.size() > 0) {
							if (resourcesToWatch.contains(res.resourceName)) {
								filteredResourceList.add(res.resourceName);
							}
						} else {
							// no resource filtering - add it
							filteredResourceList.add(res.resourceName);
						}
					}
				}
			}

		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (printToConsole) {
			System.out.println("\tresources to monitor: " + filteredResourceList.size() + " " + filteredResourceList);
		}
		return filteredResourceList;

	}

	/**
	 * get a list of resources by filtering the resource type
	 * 
	 * @param isMonitorMode - if it is the first time (print information to
	 *                      log/console) otherwise do not print
	 * @return List of jobs in a Map formatted as <resourceName>,<jobId> Note: also
	 *         creates a collection of <jobid>:<jobstartdatemillis>
	 * 
	 *         * the boolean passed to getJobList identifies the processing mode -
	 *         false = startup mode means we not only get the current list of jobs
	 *         for each monitored resource it will check to see if the structure has
	 *         already been written to file for that resource and if not, will
	 *         generate it if generating - it will also look for the most recent
	 *         previous structure (based on file - that has a datestamp) and use
	 *         that to pass to the diff process it will store a Map of
	 *         key=resourceName val=jobid for the monitor mode to use - true =
	 *         monitor mode the difference here is that we already know the previous
	 *         jobs, so only look for new jobs (a new scan) to use
	 * 
	 */
	protected Map<String, String> getJobList(boolean isMonitorMode) {
		// System.out.println("### dw debug: + getJobList called parm:isQuietMode=" +
		// isMonitorMode);
		Map<String, String> resourceJobs = new HashMap<String, String>();

		CredentialsProvider provider = new BasicCredentialsProvider();
		UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(userName, pwd);
		provider.setCredentials(AuthScope.ANY, credentials);

		HttpClient client = HttpClientBuilder.create().setDefaultCredentialsProvider(provider).build();

		String eicUrlV1CatalogResources = restURL + "/1/catalog/resources/jobs";
		if (!isMonitorMode) {
			// init/startup mode - be more verbose in printing to the console
			System.out.println("\tconnecting to... " + eicUrlV1CatalogResources);
		}

		Map<String, Long> purgedMap = new HashMap<String, Long>();

		HttpResponse response;
		try {
			response = client.execute(new HttpGet(eicUrlV1CatalogResources));
			int statusCode = response.getStatusLine().getStatusCode();
			if (!isMonitorMode) {
				System.out.println("\tstatusCode=" + statusCode);
				// System.out.println("\tresponse:" + response.toString());
			}
			BufferedReader br = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));

			// get the json resultset from the api call
			String output;
			StringBuffer json = new StringBuffer();
			while ((output = br.readLine()) != null) {
				json.append(output);
			}

			// note: java sucks at reading json natively, so we use gson (google json libs)
			// & the JobSimpleProps class
			// store the results into an array of JobSimpleProps, then convert to a List for
			// easier iterating
			Gson gson = new GsonBuilder().create();
			List<JobSimpleProps> jobList;
			JobSimpleProps[] arr = gson.fromJson(json.toString(), JobSimpleProps[].class);
			jobList = Arrays.asList(arr);

			if (jobList != null) {
				if (!isMonitorMode) {
					System.out.println("\ttotal jobs found__: " + jobList.size());
				}

				for (JobSimpleProps res : jobList) {
					// store any deleted job's
					if ("PURGE_DELETE_JOB".equals(res.jobType)) {
						purgedMap.put(res.resourceName, res.endTime);
					}
				}
				// if (purgedMap.size()>0) {
				// System.out.println("\tpurged jobs=" + purgedMap);
				// }

				// do somwthing for each job in the jobList
				for (JobSimpleProps res : jobList) {

					// if the jobtype = "SCAN_JOB" and status="completed" and the resource is one we
					// want to monitor, then add it to the hashmap
					// debug printing
					/**
					 * if (resourcesToMonitor.contains(res.resourceName)) {
					 * System.out.println("\tchecking " + res.resourceName + " job=" + res.jobId + "
					 * jobType=" + res.jobType + " status=" + res.status + " resource Monitored?=" +
					 * resourcesToMonitor.contains(res.resourceName));
					 * System.out.println("monitorMode=" + isMonitorMode + " currentResourceJob=" +
					 * resourceJobMap.get(res.resourceName)); }
					 */

					// // store any deleted job's
					// if ("PURGE_DELETE_JOB".equals(res.jobType)) {
					// System.out.println("possible purge in the way..." + res.resourceName);
					// purgedMap.put(res.resourceName, res.endTime);
					// }

					if ("SCAN_JOB".equals(res.jobType) && "Completed".equals(res.status)
							&& resourcesToMonitor.contains(res.resourceName)) {
						// bug here - commenting this line + moving to after structure extracted
						// resourceJobs.put(res.resourceName, res.jobId);
						// boolean isPurgedLast = false;
						if (purgedMap.containsKey(res.resourceName)) {
							Long lastPurged = purgedMap.get(res.resourceName);
							if (lastPurged > res.endTime) {
								// System.out.println("\tresource was purged after last load, skipping.. " +
								// lastPurged + " " + res.endTime + " res=" + res.resourceName);
								// isPurgedLast = true;
								continue;
							}
						}

						// test - store the start time of the job (used for the name of the dbstructure
						// System.out.println("job start=" + res.startTime);
						Date startDate = new Date(res.startTime);
						// System.out.println("start time=" + df.format(startDate));

						jobStartTimes.put(res.jobId, df.format(startDate));
						//
						resourcesInProgress.remove(res.resourceName);

						// check to see if there is a db structure extract job already existing for this
						// resource
						// (let the watcher process do it)
						// but - if a new resource is added, we probably want to treat it like
						// initmode...

						String fileName = formatStructFileName(res.resourceName, res.jobId);

						if (!isMonitorMode) {
							// String fileName = dbsOutFolder + "/" + res.resourceName + ":" + res.jobId +
							// ".txt";
							// String fileName = formatStructFileName(res.resourceName, res.jobId);

							// System.out.println("checking if file exists:" + fileName + " " + new
							// File(fileName).exists());

							if (!new File(fileName).exists()) {
								// get the most recent db structure file - used for the compare
								// we need to do this before we call the structure extract (since it creates a
								// new one)
								String prevDBStrucFile = this.getLastDbStructureFile(res.resourceName);

								// System.out.println(this.getClass().getName() + " calling dbExtract for newly
								// completed scan: " + res.resourceName + " job=" + res.jobId);
								// this.dbStructureExtract(res.resourceName, res.jobId);
								// check for true result - (if file is actually written)
								if (dbStructureExtract(res.resourceName, res.jobId)) {
									// add the job as one to monitor (only if a file was created)
									resourceJobs.put(res.resourceName, res.jobId);

									if (!prevDBStrucFile.equals("")) {
										/**
										 * we do not know the name of the previous load for this resource, so we need to
										 * find the list of files for this resource, ordered by name and get the most
										 * recent file as the compare from...
										 */
										File fromFile = new File(prevDBStrucFile);
										File toFile = new File(fileName);
										System.out.println(
												"ready to call structure diff..." + fromFile + " <> " + toFile);
										try {
											StructureDiff sd = new StructureDiff(fromFile, toFile, propertyFileName);
											sd.processDiffs();

										} catch (IOException e) {
											e.printStackTrace();
										}
									} // end if prev structure != ""
								} // if structure was extracted
							} else {
								// end if filename
								// System.out.println("file does exist: " + fileName + " it should be
								// monitored...");
								resourceJobs.put(res.resourceName, res.jobId);
							}
						} else {
							// in monitor mode (vs in startup mode)
							// if the job that is completed - is not in the resourceJobMap
							// then we have a case for a newly created resource (after the watch started)
							// that we need to write the structure to file & when another run happens - use
							// that for comparing
							if (resourceJobMap.get(res.resourceName) == null) {
								System.out.println("\nnew resource/first scan detected: " + res.resourceName + " job="
										+ res.jobId);
								if (dbStructureExtract(res.resourceName, res.jobId)) {
									// add the job as one to monitor (only if a file was created)
									resourceJobs.put(res.resourceName, res.jobId);
									// also add to the map of current jobs - so it will monitor from here on out
									resourceJobMap.put(res.resourceName, res.jobId);
								}
							}
							// we need to add the job here
							// if first time running this (setup mode, vs monitor mode)
							// if (new File(fileName).exists() ) {
							// add to the resourceJobs collection
							resourceJobs.put(res.resourceName, res.jobId);
							// }
						}
					} // end - if it is a job we want to watch (a scan_job & completed & a resource we
						// are monitoring)
				} // for each job found

			} // if there are jobs to process

		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (!isMonitorMode) {
			System.out.println("\tjobs to monitor: " + resourceJobs.size() + " " + resourceJobs);
		}

		// System.out.println("returning from getJobList " + resourceJobs.size() + " " +
		// resourceJobs);
		return resourceJobs;

	}

	/**
	 * 
	 * used when we have a new scan of the resource - execute while the watcher was
	 * not running so we need to get the last db structure for the resource to
	 * compare to
	 * 
	 * @param resourceNameToCheck
	 * @return fileName of the dbstructure (latest for the resource)
	 */
	public String getLastDbStructureFile(String resourceNameToCheck) {
		String dbStructFile = "";

		// System.out.println("older dbs files check: folder=" + dbsOutFolder);
		File dbStructDir = new File(dbsOutFolder);
		File[] files = dbStructDir.listFiles();
		if (files != null) {
			// bugfix - sort by file name, not by lastModified (if someone touched/edited an
			// older file)
			// Arrays.sort(files, Comparator.comparingLong(File::lastModified).reversed());
			Arrays.sort(files, NameFileComparator.NAME_INSENSITIVE_REVERSE);

			// now iterate over all files - looking for starts with resourceNameToCheck
			for (File aFile : files) {
				// System.out.println("checking..." + aFile.getName() + " starts with " +
				// resourceNameToCheck);
				if (aFile.getName().startsWith(resourceNameToCheck + "__")
						&& aFile.getName().indexOf("_differences_") == -1 && aFile.getName().endsWith(".txt")) {

					// System.out.println("latest file..." + aFile.getName());
					dbStructFile = aFile.getAbsolutePath();
					break;
				}
			}
		}
		// hack for now
		// if (resourceNameToCheck.equals("Instruments_DB") ) {
		// dbStructFile = dbsOutFolder + "/" + "Instruments_DB__20171218T151553Z.txt";
		// }
		return dbStructFile;
	}

	/**
	 * inner class - helps read the json resultset for EIC resources
	 * 
	 * @author dwrigley
	 *
	 */
	class ResourceSimpleProps {
		public String resourceName;
		public String description;
		public String resourceTypeId;
		public String resourceTypeName;
		public String resourceTypeVersion;
		public String createdBy;
		public String createdTime;
		public String modifiedBy;
		public String modifiedTime;

		public String toString() {
			// return the resourceName
			return resourceName;
		}
	}

	/**
	 * inner class - helps read the json resultset for EIC jobs
	 * 
	 * @author dwrigley
	 *
	 */
	class JobSimpleProps {
		public String jobId;
		public String resourceName;
		public String status;
		public Long startTime;
		public Long endTime;
		public Long nextSchedule;
		public String jobType;
		public String scheduleName;
		public String triggerMode;
		public String triggerModeLabel;

		public String toString() {
			// return the resourceName
			return jobId;
		}
	}

	public static boolean showDisclaimer() {
		System.out.println(DISCLAIMER);
		Console c = System.console();
		String response;
		boolean hasAgreed = false;
		if (c == null) { // IN ECLIPSE IDE (prompt for password using swing ui
			System.out.println("no console found...");
			final JPasswordField pf = new JPasswordField();
			String message = "Do you agree to this disclaimer? Y or N ";
			response = JOptionPane.showConfirmDialog(null, pf, message, JOptionPane.OK_CANCEL_OPTION,
					JOptionPane.QUESTION_MESSAGE) == JOptionPane.OK_OPTION ? new String(pf.getPassword())
							: "response (Y|N)";
		} else { // Outside Eclipse IDE (e.g. windows/linux console)
			response = new String(c.readLine("agree (Y|N)? "));
		}
		System.out.println("user entered:" + response);
		if (response != null && response.equalsIgnoreCase("Y")) {
			hasAgreed = true;
		}

		return hasAgreed;
	}

}
