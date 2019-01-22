package com.infa.eic.sample;

import java.io.BufferedReader;
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
    public static final String version="1.2";

	Integer waitTime = 0;
//	Integer initialTime = 300;
	long startMillis;
	Connection con;
	Map<String, String> processedObjects = new HashMap<String, String>();
	String restURL;
	Logger logger;
	FileHandler fh;
	String glossary;
	String resourceTypes;
	String resourceFilter;
	String userName;
	String pwd;
	String dbsOutFolder;
	Integer pageSize = 100;
	boolean includeAxonTermLink=false;
	
	public void setOutFolder(String theFolder) {
		this.dbsOutFolder = theFolder;
	}
	
	/** 
	 * for querying the wait time (in main)
	 * @return seconds that the system should wait before checking again
	 * 			0 wait time - means initialize - but do not begin the constant watch process
	 */
	protected int getWaitTime() {
		return waitTime.intValue();
	}
	
	
	List<String> resourcesTypesToMonitor = new ArrayList<String>();
	List<String> resourcesToWatch = new ArrayList<String>();
	List<String> resourcesToMonitor = new ArrayList<String>();
	Map<String,String> resourceJobMap = new HashMap<String,String>();
	DateFormat df = new SimpleDateFormat("yyyyMMdd'T'HHmmssX");

	/**
	 * map of job ids to job start times - for formatting db structure file names 
	 */
	Map<String,String> jobStartTimes = new HashMap<String,String>();
	
	String propertyFileName;
	

	/** 
	 * start the resource watch process
	 * keep running until killed
	 * TODO:  stop automatically if connection to the server is lost?
	 * 
	 * @param args property file that controls the process
	 */
	public static void main(String[] args) {
		if (args.length==0) {
			System.out.println("EIC Resource watcher: missing configuration properties file: usage:  ResourceWatch <folder>/config.properties");	
		} else {
			System.out.println("EIC Resource watcher: " + args[0] + " currentTimeMillis=" +System.currentTimeMillis());
			
		
			// pass the property file - the constructor will read all input properties
			ResourceWatch watcher = new ResourceWatch(args[0]);
			if (watcher.resourcesTypesToMonitor.isEmpty()) {
				System.out.println("no resources found to watch, exiting");
				System.exit(0);
			}
			// call the watch process
			
			if (watcher.getWaitTime()>0) {
				System.out.println("starting watch process for " + watcher.resourceJobMap.size() + " resources");
				watcher.watchForNewResourceJobsToComplete(); 
			} else {
				System.out.println("wait_time set to 0, exiting (no continuous monitoring)");				
			}
		}	
	}


	public ResourceWatch() {
	};
	


	/**
	 * constructor 
	 * @param propertyFile name/location of the propoerty file to use for controlling settings
	 */
	ResourceWatch(String propertyFile) {
//		System.out.println("Constructor:" + propertyFile);
        System.out.println(this.getClass().getSimpleName() + " " + version +  " initializing properties from: " + propertyFile);
		
		// store the property file - for passing to the diff process (email properties)
		propertyFileName = propertyFile;
		
/**		
		// setup the logger
		// @todo add file based logger (currently messages are only written to the console
		try {
			logger = Logger.getLogger("EICResourceWatch");  
			logger.setUseParentHandlers(false);

		     // This block configure the logger with handler and formatter  
		     fh = new FileHandler("eicResourceWatcher_" + String.valueOf(System.currentTimeMillis())+ ".log");  
		     logger.addHandler(fh);
		     SimpleFormatter formatter = new SimpleFormatter();  
		     fh.setFormatter(formatter);  
			 logger.info("reading property file: " +propertyFile);

	    } catch (SecurityException e) {  
	        e.printStackTrace();  
	    } catch (IOException e) {  
	        e.printStackTrace();  
	    }  
**/
		try {
			
			
			File file = new File(propertyFile);
			FileInputStream fileInput = new FileInputStream(file);
			Properties prop;
			prop = new Properties();
			prop.load(fileInput);
			fileInput.close();
			
			waitTime = Integer.parseInt(prop.getProperty("wait_time_seconds"));
			restURL = prop.getProperty("rest_service");
			resourceTypes =  prop.getProperty("resourceTypesToWatch");
			resourceFilter =  prop.getProperty("resourcesToWatch");
			userName = prop.getProperty("user");
			pwd = prop.getProperty("password");
			if (pwd.equals("<prompt>")) {
				System.out.println("password set to <prompt> for user " + userName  + " - waiting for user input...");
				pwd = APIUtils.getPassword();
				System.out.println("pwd chars entered (debug):  " + pwd.length());
			}
			resourcesTypesToMonitor = new ArrayList<String>(Arrays.asList(resourceTypes.split(",")));
			// only add to resourcesToWatch - if an entry was made for resourceFilter
			if (resourceFilter.length() > 0) {
				resourcesToWatch = new ArrayList<String>(Arrays.asList(resourceFilter.split(",")));
			}
			
			pageSize=Integer.parseInt(prop.getProperty("pagesize", "300"));
			System.out.println("Resource types filter: "  + resourcesTypesToMonitor);
			System.out.println("Resource names filter: "  + resourcesToWatch);
			
			includeAxonTermLink=Boolean.parseBoolean(prop.getProperty("includeAxonTermLink"));				

			dbsOutFolder=prop.getProperty("dbstruct.outfolder");
			if (!Files.exists(Paths.get(dbsOutFolder), new LinkOption[]{ LinkOption.NOFOLLOW_LINKS})) {
				System.out.println("path does not exist..." + dbsOutFolder + " exiting");
				// @todo - refactor separate the constructior from the initial phase of connecting to eic
				System.exit(0);
			} else {
	
				// call initializeWatchProcess-  gets a list of resources/jobs...
				this.initializeWatchProcess();
				
				System.out.println("eicResourceWatch=" + " wait_time=" + waitTime + " seconds" + " rest.service=" + restURL + " includeAxonTermLink=" + includeAxonTermLink);
//				logger.info("eicResourceWatch=" + "wait_time=" + waitTime + " seconds" + " rest.service=" + restURL);
			}
			
			
	     } catch(Exception e) {
	     	System.out.println("error reading properties file: " + propertyFile);
	     	e.printStackTrace();
	     }

	}
	
	/**
	 * get a list of resources & jobs
	 */
	private void initializeWatchProcess() {
		System.out.println("init watch service:-");
		resourcesToMonitor = getResourceList(resourcesTypesToMonitor);
		
		System.out.println("\tget list of jobs (store with resources, as baseline or when new jobs are submitted");
		boolean beQuiet = false;
		resourceJobMap = getJobList(beQuiet);
		
		return;
	}
	
	/**
	 * looks for new resources in eic to call the db structure compare/diff processes
	 * structure:
	 * 
	 * prints a . to the console - so you see that it is still running
	 * @todo  refactor to log to file (better for monitoring)
	 * after finishing will sleep for <waitTime> seconds
	 */
	private void watchForNewResourceJobsToComplete() {
		startMillis = System.currentTimeMillis();
//		logger.info("Watching for eic resources to complete, every " + waitTime + " seconds starting from: " + startMillis );
		System.out.println("Watching for eic resources to complete, every " + waitTime + " seconds");
		
		try {
			while(true) {
//				logger.info("looking for newly completed scans... since: " + startMillis);
				System.out.print(".");
				this.processWatchInterval();
				
//				startMillis = System.currentTimeMillis() - (waitTime *2)*1000;
//				System.out.println("keyset: " + processedObjects.keySet().toString());
				System.gc();  // clean up file locks Files.write(toPath, theStructure, charset); - does not seem to release the lock
//				logger.info("sleeping for: " + waitTime + " seconds");
				Thread.sleep(waitTime * 1000);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
		
	private List<String> resourcesInProgress = new ArrayList<String>();
	
	
	/**
	 * where the work really is - looks for changes.. if there are - calls the structure extract & compare
	 */
	private void processWatchInterval () {
		// get the current list of job ids for completed (or running???)
		// compare to the inital list - if different - then kick off the diff process
		Map<String,String> currentJobMap = this.getJobList(true);
		// any differences????
//		System.out.println("checking... " + currentJobMap.size());
		for(String resName: resourceJobMap.keySet()) {
			// check if the jobname is different....
			if (!resourceJobMap.get(resName).equals(currentJobMap.get(resName))) {
				// if the current job is null - then the scan is still running...
				// could switch and only monitory for complete (iterate over current job map vs resourceJobMap)
				if (currentJobMap.get(resName)!=null) {
//					System.out.println("different jobs...  time to kick off a new process..." + resName + " currentJob=" + currentJobMap.get(resName));
					if (dbStructureExtract(resName, currentJobMap.get(resName)) ) {
						// update the jobid in the existing resource colleciton - so we don't call this process again
						
						// + execute the diff report here too.....
						this.formatStructFileName(resName, resourceJobMap.get(resName));
//						File fromFile=new File(dbsOutFolder + "/" + resName + ":" + resourceJobMap.get(resName) + ".txt");
//						File toFile  =new File(dbsOutFolder + "/" + resName + ":" + currentJobMap.get(resName) + ".txt");
						File fromFile=new File(formatStructFileName(resName, resourceJobMap.get(resName)));
						File toFile  =new File(formatStructFileName(resName, currentJobMap.get(resName)));
				    	try {
				        	StructureDiff sd = new StructureDiff(fromFile, toFile, propertyFileName);
				        	sd.processDiffs();
							
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

						System.out.println("\tupdating resource|job map key=" +resName + " value=" +currentJobMap.get(resName) );
						resourceJobMap.put(resName, currentJobMap.get(resName));
						System.out.println("\tresuming watch process");

					}
					
				} else {
					// the resource dropped from 'completed' - so it is queued/running
					// only print the message 1 time - the resource could take a few cycles to complete, but we don't want to print a message every time
					if (!this.resourcesInProgress.contains(resName)) {
						System.out.print("\nwaiting for " + resName + " to finish");
						resourcesInProgress.add(resName);
					}
					
				
				}
			}
			
		}
		
				
//        logger.info("end of watch cycle");
	}
	
	/**
	 * common method for formatting the filename used for the db structure extract process
	 * format will be:  resource:jobstartdate.txt
	 * 
	 * @param resourceName
	 * @param jobName
	 * @return filename including full path
	 * 
	 */
	private String formatStructFileName(String resourceName, String jobName) {
		String formattedName;
		String jobStart = this.jobStartTimes.get(jobName);
		if (jobStart==null) {
			jobStart=jobName;
		}
		formattedName = dbsOutFolder + "/" + resourceName + "__" + jobStart + ".txt";
		
		return formattedName;
	}
	
	/**
	 * calls the db Structure extract process
	 * @param resourceName resource to get the structure for
	 * @param jobName name of the job (the last scan)
	 * @return true if the file was created
	 */
	private boolean dbStructureExtract(String resourceName, String jobName) {
		// get the job start time
		String jobStart = this.jobStartTimes.get(jobName);
		if (jobStart==null) {
			jobStart=jobName;
		}
		try {
			// format the path/name of the file that contains the db structure
			String fileName = this.formatStructFileName(resourceName, jobName);
			System.out.println("\n\t" + this.getClass().getSimpleName() + " calling db structure extract for: " + resourceName + " job=" + jobName);
			// we need to add the /2 here - since this watcher uses v1 for resource stuff
			DBStructureExport dbs = new DBStructureExport(restURL + "/2", userName, pwd);
			dbs.setIncludeAxonTermLinks(includeAxonTermLink);
//			List<String> dbStruct = dbs.getResourceStructure(resourceName, this.pageSize);
			List<String> dbStruct = dbs.getResourceStructureUsingRel(resourceName, this.pageSize);
			dbs.writeStructureToFile(fileName, dbStruct);
			return true;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
		
	}

	/**
	 * get a list of resources by filtering the resource type
	 * @param resourceTypes (list of resource types)
	 * @return List of resources that match the filter condition
	 */
	protected ArrayList<String> getResourceList(List<String> resourceTypes) {
		ArrayList<String> filteredResourceList = new ArrayList<String>();
		
		CredentialsProvider provider = new BasicCredentialsProvider();
		UsernamePasswordCredentials credentials
		 = new UsernamePasswordCredentials(userName, pwd);
		provider.setCredentials(AuthScope.ANY, credentials);
		  
		HttpClient client = HttpClientBuilder.create()
		  .setDefaultCredentialsProvider(provider)
		  .build();
		
		String eicUrlV1CatalogResources = restURL + "/1/catalog/resources";
		System.out.println("\tconnecting to... " + eicUrlV1CatalogResources + " user=" + this.userName);
		 
		HttpResponse response;
		try {
			response = client.execute(
			  new HttpGet(eicUrlV1CatalogResources ));
			int statusCode = response.getStatusLine()
					  .getStatusCode();
			System.out.println("\tstatusCode=" + statusCode);
//			System.out.println("\tresponse:" + response.toString());
            BufferedReader br = new BufferedReader(new InputStreamReader(
                    (response.getEntity().getContent())));

            String output;
            StringBuffer json = new StringBuffer();
//            System.out.println("\tOutput from Server .... \n");
            while ((output = br.readLine()) != null) {
//                System.out.println("\tline: " + output);
                json.append(output);
            }
            
            Gson gson = new GsonBuilder().create();
        	List<ResourceSimpleProps> resourceList;
        	
//        	System.out.println("\tjson response body=" + json);
        	
        	ResourceSimpleProps[] arr = gson.fromJson(json.toString(), ResourceSimpleProps[].class);
        	resourceList = Arrays.asList(arr);

        	if (resourceList != null) {
        		System.out.println("\tresources found__: " + resourceList.size() + " " + resourceList.toString());
				for(ResourceSimpleProps res: resourceList) {
//					System.out.println("\t\tresourceName=" + res.resourceName + " resourceTypeName=" + 
//									res.resourceTypeName + " include=" + 
//									resourcesTypesToMonitor.contains(res.resourceTypeName));
					
					if (this.resourcesTypesToMonitor.contains(res.resourceTypeName)) {
						// also filter on resource names						
						if (this.resourcesToWatch.size() > 0) {
							if (resourcesToWatch.contains(res.resourceName) ) {
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
        System.out.println("\tresources to monitor: " + filteredResourceList.size() + " " + filteredResourceList);
		return filteredResourceList;
		
	}
	
	
	/**
	 * get a list of resources by filtering the resource type
	 * 
	 * @param initialCall - if it is the first time (print information to log/console) otherwise do not print
	 * @return List of jobs in a Map formatted as <resourceName>,<jobId>
	 * Note:  also creates a collection of <jobid>:<jobstartdatemillis>
	 */
	protected Map<String,String> getJobList(boolean initialCall) {
		Map<String,String> resourceJobs = new HashMap<String,String>();
		
		CredentialsProvider provider = new BasicCredentialsProvider();
		UsernamePasswordCredentials credentials
		 = new UsernamePasswordCredentials(userName, pwd);
		provider.setCredentials(AuthScope.ANY, credentials);
		  
		HttpClient client = HttpClientBuilder.create()
		  .setDefaultCredentialsProvider(provider)
		  .build();
		
		String eicUrlV1CatalogResources = restURL + "/1/catalog/resources/jobs";
		if (!initialCall) {
			System.out.println("\tconnecting to... " + eicUrlV1CatalogResources);
		}
		 
		HttpResponse response;
		try {
			response = client.execute(
			  new HttpGet(eicUrlV1CatalogResources ));
			int statusCode = response.getStatusLine()
					  .getStatusCode();
			if (!initialCall) {
				System.out.println("\tstatusCode=" + statusCode);
//				System.out.println("\tresponse:" + response.toString());
			}
			BufferedReader br = new BufferedReader(new InputStreamReader(
                    (response.getEntity().getContent())));

            String output;
            StringBuffer json = new StringBuffer();
//            System.out.println("\tOutput from Server .... \n");
            while ((output = br.readLine()) != null) {
//                System.out.println("\tline: " + output);
                json.append(output);
            }
            
            Gson gson = new GsonBuilder().create();
        	List<JobSimpleProps> jobList;
    		if (!initialCall) {
//    			System.out.println("\tjson response body=" + json);
    		}
        	JobSimpleProps[] arr = gson.fromJson(json.toString(), JobSimpleProps[].class);
        	jobList = Arrays.asList(arr);

        	if (jobList != null) {
        		if (!initialCall) {
        			System.out.println("\ttotal jobs found__: " + jobList.size());
        		}
				for(JobSimpleProps res: jobList) {
					
					// if the jobtype = "SCAN_JOB" and status="completed" and the resource is one we want to monitor, then add it to the hashmap
					
//					System.out.println("\tchecking " + res.resourceName + " job=" + res.jobId + " jobType=" + res.jobType + " status=" + res.status);
//					System.out.println("\t\t" + "SCAN_JOB".equals(res.jobType));
//					System.out.println("\t\t" + "Completed".equals(res.status));
//					System.out.println("\t\t" + resourcesToMonitor.contains(res.resourceName) + " " + resourcesToMonitor);
					if ("SCAN_JOB".equals(res.jobType) && "Completed".equals(res.status) && resourcesToMonitor.contains(res.resourceName)) {
//						System.out.println("\t\t!!!! winner here...");
						resourceJobs.put(res.resourceName, res.jobId);
						// test - store the start time of the job (used for the name of the dbstructure
//						System.out.println("job start=" + res.startTime);
						Date startDate = new Date(res.startTime);
//						System.out.println("start time=" + df.format(startDate));
						
						jobStartTimes.put(res.jobId, df.format(startDate));
						// 
						resourcesInProgress.remove(res.resourceName);

//						if (this.resourcesToMonitor.contains(res.resourceName)) {
	//						filteredJobList.add(res.resourceName);
						// check to see if there is a db structure extract job already existing for this resource
						//TODO - fix this - if a re-scan happens - we don't call the dbStructureExtract (let the watcher process do it)
						if (!initialCall) {
//							String fileName = dbsOutFolder + "/" + res.resourceName + ":" + res.jobId + ".txt";
							String fileName = formatStructFileName(res.resourceName, res.jobId);
							
//							System.out.println("checking if file exists:" + fileName + " " + new File(fileName).exists());
							
							if (!new File(fileName).exists() ) {
								// get the most recent db structure file - used for the compare
								// we need to do this before we call the structure extract (since it creates a new one)
								String prevDBStrucFile = this.getLastDbStructureFile(res.resourceName);

	//							System.out.println(this.getClass().getName() + " calling dbExtract for newly completed scan: " + res.resourceName + " job=" + res.jobId);
								this.dbStructureExtract(res.resourceName, res.jobId);
								
//								String prevDBStrucFile = this.getLastDbStructureFile(res.resourceName);
								if (!prevDBStrucFile.equals("")) {
//									System.out.println("ready to call structure diff..." + prevDBStrucFile + " <> " + fileName);
									// todo ---  
									/**
									 * we do not know the name of the previous load for this resource, so we need to find the list of files 
									 * for this resource, ordered by name and get the most recent file as the compare from...
									 */
									File fromFile=new File(prevDBStrucFile);
									File toFile  =new File(fileName);
									System.out.println("ready to call structure diff..." + fromFile + " <> " + toFile);
							    	try {
							        	StructureDiff sd = new StructureDiff(fromFile, toFile, propertyFileName);
							        	sd.processDiffs();
										
									} catch (IOException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}

								}
								

							}
						}
							
//						}
					}
				}
        	}
            

		} catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (!initialCall) {
			System.out.println("\tjobs to monitor: " + resourceJobs.size() + " " + resourceJobs);
		}
		return resourceJobs;
		
	}	
	
	/**
	 * 
	 * used when we have a new scan of the resource - execute while the watcher was not running
	 * so we need to get the last db structure for the resource to compare to
	 * 
	 * @param resourceNameToCheck
	 * @return fileName of the dbstructure (latest for the resource)
	 */
	public String getLastDbStructureFile(String resourceNameToCheck) {
		String dbStructFile = "";
		
//		System.out.println("older dbs files check: folder=" + dbsOutFolder);
		File dbStructDir = new File(dbsOutFolder);
		File[] files = dbStructDir.listFiles();
		if (files != null) {
			// bugfix - sort by file name, not by lastModified (if someone touched/edited an older file)
			// Arrays.sort(files, Comparator.comparingLong(File::lastModified).reversed());
			Arrays.sort(files, NameFileComparator.NAME_INSENSITIVE_REVERSE);

			
			// now iterate over all files - looking for starts with resourceNameToCheck
			for (File aFile: files) {
	//			System.out.println("checking..." + aFile.getName() + " starts with " + resourceNameToCheck);
				if (aFile.getName().startsWith(resourceNameToCheck + "__") && aFile.getName().indexOf("_differences_")==-1 && aFile.getName().endsWith(".txt")) {
					
	//				System.out.println("latest file..." + aFile.getName());
					dbStructFile = aFile.getAbsolutePath();
					break;
				}
			}
		}		
		// hack for now
//		if (resourceNameToCheck.equals("Instruments_DB") ) {
//			dbStructFile = dbsOutFolder + "/" + "Instruments_DB__20171218T151553Z.txt";
//		}
		return dbStructFile;
	}


	
	
	/**
	 * inner class - helps read the json resultset for EIC resources
	 * @author dwrigley
	 *
	 */
	class ResourceSimpleProps  {
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
			//return the resourceName
			return resourceName;
		}
	}

	/**
	 * inner class - helps read the json resultset for EIC jobs
	 * @author dwrigley
	 *
	 */
	class JobSimpleProps  {
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
			//return the resourceName
			return jobId;
		}
	}
	
	



}
