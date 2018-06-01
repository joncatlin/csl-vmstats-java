package com.destini.vmstats;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.couchbase.client.core.message.kv.subdoc.multi.Mutation;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.error.CASMismatchException;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.subdoc.DocumentFragment;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.collections4.ListUtils;

public class ProcessVMStatistics {
	
	// Constants
	static final String ENV_COUCHBASE_NODES = "COUCHBASE_NODES";
	static final String ENV_STATSD_SERVER = "STATSD_SERVER";
	static final String ENV_DIRNAME = "DIRNAME";
	static final String ENV_FILETYPE = "FILETYPE";
	static final long A_LONG_TIME = Long.MAX_VALUE;
	private static final int NUM_RETIRES = 3;
	private static final int RETRY_TIME = 1000; // One second
	
	// Instance variables
	static String couchbaseNodes = null;
	static String fileType = null;
	static String statsdServer = null;
	static String dirname = "dummyfilename";

	
	// TODO private static final long ONE_HOUR = 1000*60*60;
	private static final long ONE_HOUR = 10000;

	SimpleDateFormat sm = new SimpleDateFormat("yyyyMMdd");
	long sleepTime = ONE_HOUR;


	
	// Statistics classes
	StatisticSum sSum = new StatisticSum();
	StatisticRemoveBase rBase = new StatisticRemoveBase();
	StatisticRemoveSpikes rSpikes = new StatisticRemoveSpikes();
	StatisticPercentiles sPercentile = new StatisticPercentiles();
	StatisticCompressTime sCTime= new StatisticCompressTime();
	
	
	// TODO check that the following are in place or the code will not work
	// CREATE INDEX `vmstats-processed` ON vmstats(processed) WHERE processed = false
	// CREATE PRIMARY INDEX ON vmstats
			
	
	// Logger initialization
//	final Logger LOG = LoggerFactory.getLogger(Start.class);
	private final Logger LOGGER = Logger.getLogger(this.getClass().getName());
	
	
	public static void main(String[] args) {
		
		ProcessVMStatistics pvmStats = new ProcessVMStatistics();
		pvmStats.getEnvVariables();

		// Open a connection to the DB
		Bucket bucket = pvmStats.getConnection();
		pvmStats.process(bucket);
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
	}


	private void process(Bucket bucket) {

		// Loop forever checking to see if there are any stats to process
		while (true) {

			try {
				// Find any unprocessed stats in the DB and process them
				String n1qlStatement = "SELECT META().cas, eventDates, toBeProcessedEventDates, lastProcessed, lastUpdate, vMName FROM vmstats WHERE processed = false";
		    	N1qlQuery q = N1qlQuery.simple(n1qlStatement);
		    	for (N1qlQueryRow row : bucket.query(q)) {

		    		JsonObject json = row.value();
		    		JsonArray jsonEventDates = json.getArray("eventDates");
		    		JsonArray jsonToBeProcessedEventDates = json.getArray("toBeProcessedEventDates");
		    		String vmName = json.getString("vMName");
		    		String lastProcessed = json.getString("lastProcessed");
		    		long cas = json.getLong("cas").longValue();
		    		
		    		// If the row has never been processed or their are dates in the toBeProcessedDates array
		    		// Then process the data that has not already been processed
	    			ArrayList<String> toBeProcessedEventDates = (ArrayList) jsonToBeProcessedEventDates.toList();
		    		if ((lastProcessed == null) || (toBeProcessedEventDates.size() > 0)) {
				    	LOGGER.log(Level.INFO, "Found unprocessed data. vMName: {0} lastProcessed: {1} eventDates: {2}", 
				    			new Object[] {vmName, lastProcessed, toBeProcessedEventDates});

				    	// For each unprocessed date get the metrics for it and process them
				    	for (String date : toBeProcessedEventDates) {

				    		processEventDate (vmName, date, bucket);
				    		
				    	}
				    	
			    		// Remove all the dates from the unprocessed array
				    	// TODO
			    		//removeEventDates (bucket, vmName, cas, toBeProcessedEventDates);
				    	
		    		}
		    	}

		    	
		    	LOGGER.log(Level.INFO, "Going to sleep for {0} milli seconds", sleepTime);
				Thread.sleep(sleepTime);

			} catch (InterruptedException e) {
				// Stop the program
				System.exit(1);
			}
		}
	}
	
	
	private void processEventDate (String vmName, String date, Bucket bucket ) {
    	LOGGER.log(Level.FINE, "Processing event data for vMName: {0} date: {1}", new Object[] {vmName, date}); 

		int sampleSize = 240;
		float percentage = 1F;

		// Create the name of the field that holds the date of the event to be processed
    	String dateFieldName = VMDocument.createDateFieldName(date);

    	String n1qlStatement = "SELECT " + dateFieldName + ".timestamp, "  + dateFieldName + ".memMax, " + dateFieldName + ".memMin, " + dateFieldName + ".memAvg, " 
				+ dateFieldName + ".cpuMax, " + dateFieldName + ".cpuMin, " + dateFieldName + ".cpuAvg, " 
				+ dateFieldName + ".netMax, " + dateFieldName + ".netMin, " + dateFieldName + ".netAvg FROM vmstats WHERE META(vmstats).id = \"" + vmName + "\"";
		N1qlQuery q = N1qlQuery.simple(n1qlStatement);
    	for (N1qlQueryRow row : bucket.query(q)) {
    		
    		// Create a MetricEvents object so it can be passed to the data processing algorithms
    		JsonObject json = row.value();
    		@SuppressWarnings({ "unchecked", "rawtypes" })
			MetricEvents me = new MetricEvents(
    				(ArrayList)json.getArray("timestamp").toList(),
    				(ArrayList)json.getArray("memMax").toList(),
    				(ArrayList)json.getArray("memAvg").toList(),
    				(ArrayList)json.getArray("memMin").toList(),
    				(ArrayList)json.getArray("cpuMax").toList(),
    				(ArrayList)json.getArray("cpuAvg").toList(),
    				(ArrayList)json.getArray("cpuMin").toList(),
    				(ArrayList)json.getArray("netMin").toList(),
    				(ArrayList)json.getArray("netAvg").toList(),
    				(ArrayList)json.getArray("netMax").toList());

    		
    		// TODO It seems that memAvg and netAvg have the same values in the first 4 positions of each array.
    		// Something is wrong!!
    		
    		
    		// RemoveBase and Spikes from each of the avg metrics
//    		ArrayList<ArrayList<String>>results = new ArrayList<ArrayList<String>>();
//    		results.add(amRemoveBaseAndSpikes (vmName, date, bucket, me.cpuAvg, "cpuAvg", sampleSize, percentage));
//    		results.add(amRemoveBaseAndSpikes (vmName, date, bucket, me.memAvg, "memAvg", sampleSize, percentage));
    		

// TODO remove the net from the analysis since the mem and net stats are identical. Replace when stats capture script corrected    		
//    		results.add(amRemoveBaseAndSpikes (vmName, date, bucket, me.netAvg, "netAvg", sampleSize, percentage));

    		// Generate the activity for the metrics
//    		amActivity (vmName, date, bucket, results);
    		
    		// Combine the results
//    		ArrayList<String> combinationResult = amCombine (vmName, date, bucket, results, "Avg");

    		// RemoveBase and Spikes from the combination
//    		amRemoveBaseAndSpikes (vmName, date, bucket, combinationResult, "Combination-240-1F", 240, 1F);    		
//    		amRemoveBaseAndSpikes (vmName, date, bucket, combinationResult, "Combination-240-2F", 240, 2F);    		
//    		amRemoveBaseAndSpikes (vmName, date, bucket, combinationResult, "Combination-240-4F", 240, 4F);    		
//    		amRemoveBaseAndSpikes (vmName, date, bucket, combinationResult, "Combination-240-8F", 240, 8F);  
    		
    		// Calculate the percentiles for each of the avg metrics
    		ArrayList<String> cpuAvgP = amPercentiles (vmName, date, bucket, me.cpuAvg, "cpuAvg");
    		ArrayList<String> netAvgP = amPercentiles (vmName, date, bucket, me.netAvg, "netAvg");
//    		ArrayList<String> memAvgP = amPercentiles (vmName, date, bucket, me.memAvg, "memAvg");
    		
    		// Remove the base and spikes for each percentile
    		ArrayList<ArrayList<String>>results2;
    		results2 = new ArrayList<ArrayList<String>>();
    		results2.add(amRemoveBaseAndSpikes (vmName, date, bucket, cpuAvgP, "cpuAvg-P", sampleSize, percentage));
      		results2.add(amRemoveBaseAndSpikes (vmName, date, bucket, netAvgP, "netAvg-P", sampleSize, percentage));
//      		results.add(amRemoveBaseAndSpikes (vmName, date, bucket, memAvgP, "memAvg-P", sampleSize, percentage));
      	    		
    		// Combine the percentile results
      		ArrayList<String> combinationResult2 = amCombine (vmName, date, bucket, results2, "Percentile");
    		
    		// Remove the base and spikes from the combination
    		ArrayList<String> combinationRBRS = amRemoveBaseAndSpikes (vmName, date, bucket, combinationResult2, "Percentile-Combination", 360, 5);
    		
    		// Compress the time scale to 15 minute increments. Average the values across that period 
    		amCompressTime(vmName, date, bucket, me.timestamp, combinationRBRS);
    		
    		
    		// There is only one of this document 
    		break;
    	}

	}


	private ArrayList<String> amRemoveBaseAndSpikes (String vmName, String date, Bucket bucket, ArrayList<String> metric, String metricName,
			int sampleSize, float percentage) {
		
		DocumentFragment<Mutation> updateResult = null;
		JsonArray jsonMetric = null;
		String newMetricName = metricName + "-RB-RS";

		// Calculate the RemoveBase array from the metric values for this date
		ArrayList<String> result = rBase.removeBase(metric, percentage, sampleSize);

/*
		// Add the new metrics to the existing ones and update the dates array to contain the new date
		// the metrics were captured
		updateResult = bucket
		    .mutateIn(vmName)
		    .upsert(VMDocument.createDateFieldName(date) + ".netAvgRB-"+sampleSize, netAvgRB)
		    .execute();				
		LOGGER.log(Level.INFO, "Document for vMName: {0}, date {1}, upserted netAvgRB with sampleSize of: {2}", 
				new Object[] {vmName, date, sampleSize});
*/
		// Remove any spikes in the data using several sampleSizes and numberOfZeros
		result = rSpikes.removeSpikes(result, 3, 2);
		result = rSpikes.removeSpikes(result, 5, 3);
		jsonMetric = JsonArray.from(result);
		
		// Save the processed data
		updateResult = bucket
		    .mutateIn(vmName)
		    .upsert(VMDocument.createDateFieldName(date) + "." + newMetricName, jsonMetric)
		    .execute();				

		LOGGER.log(Level.INFO, "Created metric named {0} by removing base and spikes. vMName: {1}, date {2} with sampleSize of: {3}", 
				new Object[] {newMetricName, vmName, date, sampleSize});
		
		return result;
	}


	private ArrayList<String> amCombine (String vmName, String date, Bucket bucket, ArrayList<ArrayList<String>> metrics, String metricName) {
		
		DocumentFragment<Mutation> updateResult = null;
		JsonArray jsonMetric = null;
		String newMetricName = metricName + "-Combination";
		ArrayList<String> result = new ArrayList<String>(metrics.get(0).size());
		float sum;
		
		// Loop through each metric and create a new array from the combination of all the arrays
		for (int index = 0; index < metrics.get(0).size(); index++) {
			sum = 0;
			for (int numArrays = 0; numArrays < metrics.size(); numArrays++) {
				sum += Float.valueOf(metrics.get(numArrays).get(index));
			}
			
			result.add(Float.toString(sum));
		}
		
		
		// Save the processed data
		jsonMetric = JsonArray.from(result);
		updateResult = bucket
		    .mutateIn(vmName)
		    .upsert(VMDocument.createDateFieldName(date) + "." + newMetricName, jsonMetric)
		    .execute();				

		LOGGER.log(Level.INFO, "Created combination metric for vMName: {0}, date {1}", 
				new Object[] {vmName, date});
		
		return result;
	}

	
	private ArrayList<String> amActivity (String vmName, String date, Bucket bucket, ArrayList<ArrayList<String>> metrics) {
		
		DocumentFragment<Mutation> updateResult = null;
		JsonArray jsonMetric = null;
		String newMetricName = "Activity";
		ArrayList<String> result = new ArrayList<String>(metrics.get(0).size());
		int sum;
		final String ZERO = "0.0";

		// Loop through each metric and create a new array from the activity of all the arrays
		for (int index = 0; index < metrics.get(0).size(); index++) {
			sum = 0;
			for (int numArrays = 0; numArrays < metrics.size(); numArrays++) {
				sum += (metrics.get(numArrays).get(index).equals(ZERO)) ? 0 : 1;
			}
			
			result.add(Integer.toString(sum));
		}
		
		
		// Save the processed data
		jsonMetric = JsonArray.from(result);
		updateResult = bucket
		    .mutateIn(vmName)
		    .upsert(VMDocument.createDateFieldName(date) + "." + newMetricName, jsonMetric)
		    .execute();				

		LOGGER.log(Level.INFO, "Created activity metric for vMName: {0}, date {1}", 
				new Object[] {vmName, date});
		
		return result;
	}

	
	private ArrayList<String> amPercentiles (String vmName, String date, Bucket bucket, ArrayList<String> metric, String metricName) {
		
		DocumentFragment<Mutation> updateResult = null;
		JsonArray jsonMetric = null;
		String newMetricName = metricName + "-P";
		ArrayList<String> result = new ArrayList<String>(metric.size());

		// Convert the metric to percentiles
		result = sPercentile.percentiles (metric);
		
		// Save the processed data
		jsonMetric = JsonArray.from(result);
		updateResult = bucket
		    .mutateIn(vmName)
		    .upsert(VMDocument.createDateFieldName(date) + "." + newMetricName, jsonMetric)
		    .execute();				

		LOGGER.log(Level.INFO, "Created percentiles for metric: {0} for vMName: {1}, date {2}", 
				new Object[] {metricName, vmName, date});
		
		return result;
	}


	private void amCompressTime (String vmName, String date, Bucket bucket, ArrayList<Long> time, ArrayList<String> values) {
		
		DocumentFragment<Mutation> updateResult = null;
		JsonArray jsonTimes = null;
		JsonArray jsonValues = null;
		int timeIncrement = 15; 	// 15 minute increments
		
		// Compress the time scale to 15 minute increments. Average the values across that period 
		try {
			StatisticCompressTime.Results ctResults = sCTime.compressTime(date, time, values, timeIncrement);

			// Save the processed data
			jsonTimes = JsonArray.from(ctResults.times);
			jsonValues = JsonArray.from(ctResults.values);
			updateResult = bucket
			    .mutateIn(vmName)
			    .upsert(VMDocument.createDateFieldName(date) + ".compressedTimestamp", jsonTimes)
			    .upsert(VMDocument.createDateFieldName(date) + ".compressedValues", jsonValues)
			    .execute();				

			LOGGER.log(Level.INFO, "Compressed the times for vMName: {0}, date {1}, into {2} minute increments", 
					new Object[] {vmName, date, timeIncrement});

		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	
	

	
	
	
	
	
	private void analyzeMetricsTest1 (String vmName, String date, Bucket bucket, MetricEvents me) {
		
		int[] sampleSizes = {20, 60, 120, 240};
		int sampleSize = 0;
		
//		sSum.processSum(vmName, date, me);

		for (int i=0; i < sampleSizes.length; i++) {
			sampleSize = sampleSizes[i];
			
			// Calculate the RB array from the netAvg values for this date
			ArrayList<String> result = rBase.removeBase(me.netAvg, 1F, sampleSize);
			JsonArray netAvgRB = JsonArray.from(result);

			// Add the new metrics to the existing ones and update the dates array to contain the new date
			// the metrics were captured
			DocumentFragment<Mutation> updateResult = bucket
			    .mutateIn(vmName)
			    .upsert(VMDocument.createDateFieldName(date) + ".netAvgRB"+sampleSize, netAvgRB)
			    .execute();				
			LOGGER.log(Level.INFO, "Document for vMName: {0}, date {1}, upserted netAvgRB with sampleSize of: {2}", 
					new Object[] {vmName, date, sampleSize});

			// Remove any spikes in the data
			ArrayList<String> resultSpikes = rSpikes.removeSpikes(result, 3, 2);
			resultSpikes = rSpikes.removeSpikes(resultSpikes, 5, 3);
			JsonArray netAvgRBRS = JsonArray.from(resultSpikes);
			
			// Save the new data without spikes
			updateResult = bucket
			    .mutateIn(vmName)
			    .upsert(VMDocument.createDateFieldName(date) + ".netAvgRB-RS", netAvgRBRS)
			    .execute();				
		}
	}

	
	private void removeEventDates (Bucket bucket, String vmName, long originalCas, ArrayList<String> toBeProcessedEventDates) throws InterruptedException {

		long cas = originalCas;
		boolean processed = true; // On the first attempt set the processed flag, if this fails then it will be set to false
		
		// Remove the date from the list of unprocessed dates. If the document has been modified then get the new list
		// of unprocessed event dates and merge them before trying to update the document again
		ArrayList<String> emptyArray = new ArrayList<String>();
		JsonArray newToBeProcessedEventDates = JsonArray.from(emptyArray);

		// Try to update the array but if it fails because the document has been updated in the background then retry but first 
		// merge the arrays
		for (int i=0; i < NUM_RETIRES; i++) {

			try {
				DocumentFragment<Mutation> result = bucket
				    .mutateIn(vmName)
				    .withCas(cas)
				    .upsert("toBeProcessedEventDates", newToBeProcessedEventDates)
				    .upsert("processed", processed)
				    .execute();
				
				// Success so return
				return;

			} catch (CASMismatchException e) {
			
		    	// Wait and try again
		    	Thread.sleep(RETRY_TIME);

		    	processed = false;
		    	
		    	// Get the array from the DB to merge the changes
				String n1qlStatement = "SELECT META().cas, toBeProcessedEventDates FROM vmstats WHERE META(vmstats).id = \"" + vmName + "\"";
		    	N1qlQuery q = N1qlQuery.simple(n1qlStatement);
		    	for (N1qlQueryRow row : bucket.query(q)) {

		    		JsonObject json = row.value();
		    		JsonArray jsonToBeProcessedEventDates = json.getArray("toBeProcessedEventDates");
		    		cas = json.getLong("cas").longValue();
		    		
		    		// Create the merged array by subtracting the old array from the one just retrieved from the DB
		    		ArrayList<String> newEventDates = (ArrayList) jsonToBeProcessedEventDates.toList();
		    		List<String> resultList = ListUtils.subtract(newEventDates, toBeProcessedEventDates);
		    		newToBeProcessedEventDates = JsonArray.from(resultList);

		    		// There should only be a single row so finish the loop
		    		break;
		    	}
			}		
		} 

		// Something has gone very wrong. Exhausted the number of retries
		LOGGER.log(Level.SEVERE, "Exhausted the number of retires attempting to merge toBeProcessedEvents array after detecting a document update. Aborting");
		System.exit(1);

	}
	
	
	

    
    
    
    // TODO compare days of the week when people are in the office to days when they are not
    // TODO use IO as a measure of use, better than CPU
    // TODO find the number of hours over the base during the working part of the day
    // TODO find the number of contiguous hours over base during the working part of the day
    // TODO compare the morning with the afternoon
    // TODO look at lunch times and how long they take for lunch
    // TODO try to determine when they start their day, increase of 10% from base 
    

}
	
