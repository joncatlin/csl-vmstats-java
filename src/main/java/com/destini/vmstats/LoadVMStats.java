package com.destini.vmstats;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.function.BiConsumer;

import com.couchbase.client.core.message.kv.subdoc.multi.Lookup;
import com.couchbase.client.core.message.kv.subdoc.multi.Mutation;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.subdoc.DocumentFragment;

import java.util.logging.Level;
import java.util.logging.Logger;

public class LoadVMStats {
	
	// Metrics are located in \\dciop\VMmetrics
	
	// Logger initialization
//	final Logger LOG = LoggerFactory.getLogger(Start.class);
	private final Logger LOGGER = Logger.getLogger(this.getClass().getName());
	
	// Lambda expressions for printing a TreeMap contents
	BiConsumer<Date, Stats> printStats = (date, stat) -> {
		LOGGER.log(Level.FINEST, "\t\tDateTime: {0} - Values: {1}", new Object[] {date, stat});
	};

	BiConsumer<Date, TreeMap<Date, Stats>> printDates = (date, map) -> {
		LOGGER.log(Level.FINEST, "\tDate: {0}", date);
		map.forEach(printStats);
	};
	

	// Constants
	static final String ENV_COUCHBASE_NODES = "COUCHBASE_NODES";
	static final String ENV_STATSD_SERVER = "STATSD_SERVER";
	static final String ENV_DIRNAME = "DIRNAME";
	static final String ENV_FILETYPE = "FILETYPE";
	static final String ENV_VMNAME_PATTERN = "VMNAME_PATTERN";
	static final long A_LONG_TIME = Long.MAX_VALUE;
//	private static final int NUM_MEASUREMENTS = (int) (24*60/5*0.8);
//	private static final int NUM_MEASUREMENTS = (int) (24*60)-30;
	private static final int NUM_MEASUREMENTS = (int) 660;
	private static final String SEPERATOR = ",";
	
	// Instance variables
	static String couchbaseNodes = null;
	static String fileType = null;
	static String statsdServer = null;
	static String dirname = "dummyfilename";
	static String vmNamePattern = null;
	SimpleDateFormat sm = new SimpleDateFormat("yyyyMMdd");
	long sleepTime = ONE_HOUR;
	String processedFilesDirname = null;
	
	
	// TODO private static final long ONE_HOUR = 1000*60*60;
	private static final long ONE_HOUR = 10000;

	
	
	// TODO check that the following are in place or the code will not work
	// CREATE INDEX `vmstats-processed` ON vmstats(processed) WHERE processed = false
	// CREATE PRIMARY INDEX ON vmstats
			
	public static void main(String[] args) {
		
		LoadVMStats loadVMStats = new LoadVMStats();
		loadVMStats.getEnvVariables();

		// Open a connection to the DB
		Bucket bucket = loadVMStats.getConnection();
		
		loadVMStats.load (bucket);

	}

	
	private Bucket getConnection() {
		
		// Create a connection to the DB
		CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
                .connectTimeout(30000) //10000ms = 10s, default is 5s
                .managementTimeout(30000) // 30s
                .socketConnectTimeout(10000) //10s
                .build();
		Cluster cluster = CouchbaseCluster.create(env, couchbaseNodes);	
		Bucket bucket = cluster.openBucket("vmstats");
		return bucket;
//		return null;
	}


	private void getEnvVariables() {

		// Get all the environment variables
        Map<String, String> env = System.getenv();

        // Get the couchbase nodes to connect to
        if (env.containsKey(ENV_COUCHBASE_NODES)) {
        	couchbaseNodes = env.get(ENV_COUCHBASE_NODES);
        	LOGGER.log(Level.INFO, "Environment variable {0} = {1}", new Object[]{ ENV_COUCHBASE_NODES, couchbaseNodes});
        } else {
        	LOGGER.log(Level.SEVERE, "Missing environment variable: {}", ENV_COUCHBASE_NODES);
        	System.exit(0);
        }
        
        if (env.containsKey(ENV_DIRNAME)) {
        	dirname = env.get(ENV_DIRNAME);
        	LOGGER.log(Level.INFO, "Environment variable {0} = {1}", new Object[]{ ENV_DIRNAME, dirname});
        } else {
        	LOGGER.log(Level.SEVERE, "Missing environment variable: {0}", ENV_DIRNAME);
        	System.exit(0);
        }
        
        if (env.containsKey(ENV_FILETYPE)) {
        	fileType = env.get(ENV_FILETYPE);
        	LOGGER.log(Level.INFO, "Environment variable {0} = {1}", new Object[]{ ENV_FILETYPE, fileType});
        } else {
        	LOGGER.log(Level.SEVERE, "Missing environment variable: {0}", ENV_FILETYPE);
        	System.exit(0);
        }

        if (env.containsKey(ENV_STATSD_SERVER)) {
        	statsdServer = env.get(ENV_STATSD_SERVER);
        	LOGGER.log(Level.INFO, "Environment variable {0} = {1}", new Object[]{ ENV_STATSD_SERVER, statsdServer});
        } else {
        	LOGGER.log(Level.SEVERE, "Missing environment variable: {0}", ENV_STATSD_SERVER);
        	System.exit(0);
        }

        if (env.containsKey(ENV_VMNAME_PATTERN)) {
        	vmNamePattern = env.get(ENV_VMNAME_PATTERN);
        	LOGGER.log(Level.INFO, "Environment variable {0} = {1}", new Object[]{ ENV_VMNAME_PATTERN, vmNamePattern});
        } else {
        	LOGGER.log(Level.SEVERE, "Missing environment variable: {0}", ENV_VMNAME_PATTERN);
        	System.exit(0);
        }

	}

	
	public void load(Bucket bucket) {

		processedFilesDirname = dirname + "/ProcessedFiles";

		// Create a directory to store the processed files in
		File processedFilesDir = new File(processedFilesDirname);
		if (!processedFilesDir.exists()) {
	    
		    // attempt to create the directory here
		    if (!processedFilesDir.mkdir()) {
				LOGGER.log(Level.SEVERE, "Cannot create directory to move processed files to. Aborting.");
				System.exit(1);
		    }
		}
		
    	// Create the structure to hold the processed files metrics
		HashMap<String, TreeMap<Date, TreeMap<Date, Stats>>> readData = new HashMap<String, TreeMap<Date, TreeMap<Date, Stats>>>(300);

		// Loop forever and process any files found
		while (true) {
			try {
				processDir(bucket, readData);
				
				LOGGER.log(Level.INFO, "Going to sleep for {0} milli seconds", sleepTime);
				Thread.sleep(sleepTime);

			} catch (InterruptedException e) {
				// Stop the program
				System.exit(1);
			}
		}
	}
		
	
	public void processDir (Bucket bucket, HashMap<String, TreeMap<Date, TreeMap<Date, Stats>>>readData) {

		boolean filesProcessed = false;
		VMDocument vmStats = null;
		HashMap<String, Date> readDataToRemove = new HashMap<String, Date>();
		
		// Set up the filter to find the files of the correct type
		File dir = new File(dirname);
		FileFilter filter = new FileFilter() {
		    public boolean accept(File file) {
			      return file.getName().endsWith(fileType);
		    }
		};
		  
		  
		// Scan the directory for the matching files
		File[] files = dir.listFiles(filter);
		filesProcessed = files.length > 0;
		for (int i=0; i< files.length; i++) {
			
            try {
				// Process the file and build an internal data structure
	        	processFile(files[i].getAbsolutePath(), readData);
	        	
	        	// Move the file to the processed directory to prevent processing it again
	        	File renameFile = new File(processedFilesDirname + "/" + files[i].getName());
	        	if (!files[i].renameTo(renameFile)) {
	        		// Cannot move the file so attempt to delete it
	        		if (!files[i].delete()) {
	    				LOGGER.log(Level.SEVERE, "Cannot move the file {0} or delete it. Aborting.", files[i].getAbsolutePath());
	    				System.exit(1);
	        		}
	        	}
			} catch (ParseException e) {
				LOGGER.log(Level.SEVERE, "An error occurred while processing the file.", e);
			} catch (IOException e) {
				LOGGER.log(Level.SEVERE, "An error occurred while processing the file.", e);
			}
		} 
		
		// If there were some files processed then check to see if there are enough stats for a date
		// so they can be stored in the DB otherwise skip and wait until there are enough stats
		if (filesProcessed) {

			Iterator<String> i = readData.keySet().iterator();
			while (i.hasNext()) {
				String vmName = (String)i.next();
				TreeMap<Date, TreeMap<Date, Stats>> metricDates = readData.get(vmName);

				Iterator<Date> j = metricDates.keySet().iterator();
				Date previousDate = null;
				while (j.hasNext()) {
					Date date = (Date)j.next();
					TreeMap<Date, Stats> metrics = metricDates.get(date);
					
					LOGGER.log(Level.FINE, "\t\tvmName {0} for date {1} contains {2} metrics.", new Object[] {vmName, date, metrics.size()});
					if (LOGGER.isLoggable(Level.FINEST)) metrics.forEach(printStats);

					
					// If the date has changed then check to see if there are any old metrics and if so remove them
					if (previousDate != null && (!previousDate.equals(date))) {
						
						// Remove the previous days metrics as they will not be processed now a new day has been found
						readDataToRemove.put(vmName,  previousDate);
					}
					
					// Transform the data if there is at least a certain number of measurements captured
					int temp = metrics.size();
					if (metrics.size() > NUM_MEASUREMENTS) {
						vmStats = transformData (vmName, date, metrics);

						// Store the metrics in the DB and then remove them from the data structure to prevent
						// metrics from hanging around
						upsertMetrics(bucket, vmStats);
						j.remove();
					
					} else {
						// Save this to check for partial day metrics that can be removed
						previousDate = date;
						
					}
/*					
					// Transform the data if there is at least a certain number of measurements captured
					if (metrics.size() > NUM_MEASUREMENTS) {
						vmStats = transformData (vmName, date, metrics);

						// Store the metrics in the DB and then remove them from the data structure to prevent
						// metrics from hanging around
						upsertMetrics(bucket, vmStats);
						j.remove();
						
						// If there were metrics for the previous date then they are likely not going to be used so remove them
						if (previousDate != null && (!previousDate.equals(date))) {
							readDataToRemove.put(vmName,  previousDate);
							LOGGER.log(Level.FINE, "\t\tMarking metrics for removal. VmName {0} for date {1}.", new Object[] {vmName, previousDate});
						}
					} else {
						// Save this to check for partial day metrics that can be removed
						previousDate = date;
					}
*/
				}
			}
		}
		
		// Tidy up all those dates that have metrics less than a full day and that will likely never have enough metrics to process
		Iterator<String> i = readDataToRemove.keySet().iterator();
		while (i.hasNext()) {
			String vmName = (String)i.next();
			Date date = (Date)readDataToRemove.get(vmName);
			
			// Remove this date from the readData with the correct vmName, to prevent memory from leaking
			TreeMap<Date, TreeMap<Date, Stats>> metricDates = readData.get(vmName);
			LOGGER.log(Level.FINE, "Removing metrics for vmName {0} for date {1}", new Object[] {vmName, date});
			
			if (metricDates.remove(date) == null) {
				LOGGER.log(Level.SEVERE, 
						"Tried to remove a group of metrics for vmName {0} with date {1} and nothing found. This indicates a logic error that might cause a memory leak.", 
						new Object[] {vmName, date});
			}
		}
	}
	
	
	public void processFile (String fileName, HashMap<String, TreeMap<Date, TreeMap<Date, Stats>>> readData) throws ParseException, IOException {
		
		BufferedReader br = null;
		String line = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		SimpleDateFormat dateTimeFormat = new SimpleDateFormat("MM/dd/yyyy H:m");

		try {
			// Read the first line to get the column headings
			br = new BufferedReader(new FileReader(fileName));
			line = br.readLine();
			while ((line = br.readLine()) != null) {
		    	processLine(line, readData, dateFormat, dateTimeFormat);
		    }
		    
		    LOGGER.log(Level.INFO, "Processed file: {0}", fileName);
		} catch (FileNotFoundException e) {
			LOGGER.log(Level.SEVERE, "An error occurred while processing the lines in a file.", e);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, "An error occurred while processing the lines in a file.", e);
		} finally {
			br.close();
		}
	}
	
	
	public void processLine(String line, HashMap<String, TreeMap<Date, TreeMap<Date, Stats>>> readData, 
			SimpleDateFormat dateFormat, SimpleDateFormat dateTimeFormat) throws ParseException {

		// WARNING !!!!! This code assumes a fixed file format !!!!!
		StringTokenizer st = new StringTokenizer(line, SEPERATOR);
		
		// The various structures used to store the information
		TreeMap<Date, Stats>treeMapOfDateTimes = null;
		TreeMap<Date, TreeMap<Date, Stats>> treeMapOfDates = null; 
		
		// Only process the line if the virtual machine name matches the supplied pattern
		String vmName = st.nextToken().replace("\"", "");
		if (isMatchVMNamePattern(vmName)) {
			String date = st.nextToken().replace("\"", "");
			String hour = st.nextToken().replace("\"", "");
			String dateTime = date + ' ' + hour;
		    Date parsedDate, parsedDateTime;
			parsedDate = dateFormat.parse(dateTime);
			parsedDateTime = dateTimeFormat.parse(dateTime);
		    
			Stats eventMetrics = new Stats(
					st.nextToken().replace("\"", ""), 
					st.nextToken().replace("\"", ""), 
					st.nextToken().replace("\"", ""), 
					st.nextToken().replace("\"", ""), 
					st.nextToken().replace("\"", ""), 
					st.nextToken().replace("\"", ""), 
					st.nextToken().replace("\"", ""), 
					st.nextToken().replace("\"", ""), 
					st.nextToken().replace("\"", ""), 
					parsedDateTime.getTime());
	
			// Get the TreeMap that holds the events for the date of the line in the file just read
			// Create any necessary structures along the way if they are missing due to this being the first item for the vm or the first 
			// item for the date
			treeMapOfDates = readData.get(vmName); 
			if (treeMapOfDates == null) {
				treeMapOfDates = new TreeMap<Date, TreeMap<Date, Stats>>();
				readData.put(vmName, treeMapOfDates);
			}
			
			treeMapOfDateTimes = treeMapOfDates.get(parsedDate); 
			if (treeMapOfDateTimes == null) {
				treeMapOfDateTimes = new TreeMap<Date, Stats>();
				treeMapOfDates.put(parsedDate, treeMapOfDateTimes);
			}
						
			treeMapOfDateTimes.put(parsedDateTime, eventMetrics);
		}			
	}


	public ArrayList<String> getHeadings(String line) {
		ArrayList<String> headings = new ArrayList<String>();
		
		StringTokenizer st = new StringTokenizer(line, SEPERATOR);
		while (st.hasMoreTokens()) {
			headings.add(st.nextToken());
		}
		
		return headings;
	}



	private void upsertMetrics(Bucket bucket, VMDocument stats) {

		boolean metricDateExists;
	    long cas = 0;

		try {
		    DocumentFragment<Lookup> result = bucket
			    .lookupIn(stats.getVmName())
			    .exists(stats.getDateFieldName())
//			    .get("dates")
			    .execute();
		    
			metricDateExists = result.content(stats.getDateFieldName(), Boolean.class);
			
			if (!metricDateExists) {
				// Create the sub document we are going to add
				JsonObject events = createJsonEvents(stats);
				
				// Add the new metrics to the existing ones and update the dates array to contain the new date
				// the metrics were captured
				DocumentFragment<Mutation> updateResult = bucket
				    .mutateIn(stats.getVmName())
				    .arrayAppend("eventDates", stats.getLastUpdate())
				    .arrayAppend("toBeProcessedEventDates", stats.getLastUpdate())
				    .upsert("lastUpdate", stats.getLastUpdate())
				    .upsert("processed", false)
				    .insert(stats.getDateFieldName(), events)
				    .execute();				
				cas = updateResult.cas();
				LOGGER.log(Level.INFO, "Document for vMName: {0} updated with cas: {1}", new Object[] {stats.getVmName(), cas});
			} else {
				// IGNORE this update as it already exists in the DB
			}
			
	    } catch (DocumentDoesNotExistException e) {
	    	// Insert the metrics into a new document
	    	insertMetrics(bucket, stats);
	    }
	}	

	public JsonObject createJsonEvents(VMDocument stats) {

		// Create a JSON document for the Events
		JsonArray timestamp = JsonArray.from(stats.metricEvents[0].timestamp);
		JsonArray memMax = JsonArray.from(stats.metricEvents[0].memMax);
		JsonArray memMin= JsonArray.from(stats.metricEvents[0].memMin);
		JsonArray memAvg = JsonArray.from(stats.metricEvents[0].memAvg);
		JsonArray cpuMax = JsonArray.from(stats.metricEvents[0].cpuMax);
		JsonArray cpuMin = JsonArray.from(stats.metricEvents[0].cpuMin);
		JsonArray cpuAvg = JsonArray.from(stats.metricEvents[0].cpuAvg);
		JsonArray netMax = JsonArray.from(stats.metricEvents[0].netMax);
		JsonArray netMin= JsonArray.from(stats.metricEvents[0].netMin);
		JsonArray netAvg = JsonArray.from(stats.metricEvents[0].netAvg);
		JsonObject events = JsonObject.create()
				.put("timestamp", timestamp)
				.put("memMax", memMax)
				.put("memMin", memMin)
				.put("memAvg", memAvg)
				.put("cpuMax", cpuMax)
				.put("cpuMin", cpuMin)
				.put("cpuAvg", cpuAvg)
				.put("netMax", netMax)
				.put("netMin", netMin)
				.put("netAvg", netAvg);

		return events;		
	}
	
	
	private VMDocument transformData (String vmName, Date sampleDate, TreeMap<Date, Stats> metrics) {
		
//		HashMap<String, VMStats> transformed = new HashMap<String, VMStats>();
		MetricEvents e = new MetricEvents();
		
		// Transform the data into a series of arrays which should make it easier to process
		// Loop through each VMFound in the data
		for (Map.Entry<Date, Stats>data : metrics.entrySet()) {

			Date key = data.getKey();
			Stats value = data.getValue();

			// Fill the structure used to store the metrics
			e.addTimestamp(key.getTime());
			e.addCpuAvg(value.cpuAvg);
			e.addCpuMax(value.cpuMax);
			e.addCpuMin(value.cpuMin);
			e.addMemAvg(value.memAvg);
			e.addMemMax(value.memMax);
			e.addMemMin(value.memMin);
			e.addNetMin(value.netMin);
			e.addNetAvg(value.netAvg);
			e.addNetMax(value.netMax);
		}

		// Create the structure to store the metrics for a given date in
		String[] eventDates = new String[1];
		eventDates[0] = sm.format(sampleDate);
		MetricEvents[] metricEvents = new MetricEvents[1];
		metricEvents[0] = e;
		VMDocument stats = new VMDocument(vmName, sm.format(sampleDate), eventDates, metricEvents);

		return stats;
	}

	
	private void insertMetrics(Bucket bucket, VMDocument stats) {

	    long cas = 0;
		
	    // Create a JSON document for the Events
	    MetricEvents[] metricEvents = stats.getMetricEvents();
		JsonArray timestamp = JsonArray.from(metricEvents[0].timestamp);
		
		JsonArray memMax = JsonArray.from(metricEvents[0].memMax);
		JsonArray memMin= JsonArray.from(metricEvents[0].memMin);
		JsonArray memAvg = JsonArray.from(metricEvents[0].memAvg);
		JsonArray cpuMax = JsonArray.from(metricEvents[0].cpuMax);
		JsonArray cpuMin = JsonArray.from(metricEvents[0].cpuMin);
		JsonArray cpuAvg = JsonArray.from(metricEvents[0].cpuAvg);
		JsonArray netMax = JsonArray.from(metricEvents[0].netMax);
		JsonArray netMin= JsonArray.from(metricEvents[0].netMin);
		JsonArray netAvg = JsonArray.from(metricEvents[0].netAvg);
		JsonObject events = JsonObject.create()
				.put("timestamp", timestamp)
				.put("memMax", memMax)
				.put("memMin", memMin)
				.put("memAvg", memAvg)
				.put("cpuMax", cpuMax)
				.put("cpuMin", cpuMin)
				.put("cpuAvg", cpuAvg)
				.put("netMax", netMax)
				.put("netMin", netMin)
				.put("netAvg", netAvg);
		
	    // Create a JSON document for the main item as this is an insert 
		JsonArray eventDatesArray = JsonArray.from((Object[])stats.getEventDates());
		JsonObject doc = JsonObject.create()
            .put("vMName", stats.getVmName())
            .put("processed", false)
            .put("lastUpdate", stats.getLastUpdate())
            .put("eventDates", eventDatesArray)
            .put("toBeProcessedEventDates", eventDatesArray)
            .put(stats.getDateFieldName(), events);

        // Store the Document
		JsonDocument inserted = bucket.insert(JsonDocument.create(stats.getVmName(), doc));
		cas = inserted.cas();
		LOGGER.log(Level.INFO, "Document for vMName: {0} inserted with cas: {1}", new Object[] {stats.getVmName(), cas});
	}


    private boolean isMatchVMNamePattern(String vmName) {

    	return vmName.matches(vmNamePattern);
    }
	
}
