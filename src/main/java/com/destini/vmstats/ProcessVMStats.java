package com.destini.vmstats;

import com.couchbase.client.core.message.kv.subdoc.multi.Lookup;
import com.couchbase.client.core.message.kv.subdoc.multi.Mutation;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.subdoc.DocumentFragment;
import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.query.dsl.Expression.*;

public class ProcessVMStats implements Runnable {

	String statsdServer = null;
	Bucket bucket = null;
	
	
	public ProcessVMStats (Bucket bucket, String statsdServer) {
		
		// Save the various parameters in instance variables
		this.bucket = bucket;
		this.statsdServer = statsdServer;
	}

	@Override
	public void run() {
		
		while (true) {

			// Find any unprocessed stats in the DB and process them
		    	Statement st = select("eventDates", "lastProcessed", "vMName").from(i("vmstats")).where(x("processed").eq(false));
		    	N1qlQuery q = N1qlQuery.simple(st);
		    	for (N1qlQueryRow row : bucket.query(q)) {
		    	    System.out.println(row);
		    	}
		}
	}

	
    private void findBase () {
    	String json = "[[1484068080000,8,9,6,20,24,16],[1484071680000,6,7,4,21,25,18],[1484075280000,4,4,4,15,16,15],[1484078880000,5,5,5,16,16,16],[1484082480000,4,4,4,13,13,12],[1484086080000,5,6,4,14,15,12],[1484089680000,4,4,4,14,15,13],[1484093280000,4,5,4,13,15,12],[1484096880000,5,5,4,13,14,13],[1484100480000,4,4,4,13,14,13],[1484104080000,4,4,4,14,14,14],[1484107680000,5,6,4,18,19,16],[1484111280000,13,15,11,19,20,18],[1484114880000,21,36,5,23,28,18],[1484118480000,4,4,4,18,19,18],[1484122080000,4,4,4,17,17,17],[1484125680000,5,6,5,18,20,17],[1484129280000,4,4,4,14,16,11],[1484132880000,5,5,4,12,13,12],[1484136480000,4,5,4,16,17,16],[1484140080000,4,4,4,13,13,12],[1484143680000,5,6,4,14,17,12],[1484147280000,4,4,4,12,12,12]]";
    	Gson gson = new Gson();
    	long[][] array = gson.fromJson(json, long[][].class);

    	float rollingAvg = 1000000; // Large number so first average is always less
    	float max = 0;
    	float avg = 0;
    	
    	int rollingAvgLength = 3;
    	
    	// Calculate the smallest rolling average, overall average and a max for the datapoints
    	for(int i = 0; i < array.length - rollingAvgLength; i++) {
    		
    		int sum = 0;
    		for (int offset = 0; offset < rollingAvgLength; offset++) {
    			sum += array[i+offset][1];
    		}

    		// Calculate the new values for the comparison
    		float newRollingAvg = sum / rollingAvgLength;
    		float newMax = array[i+rollingAvgLength][1];

    		// Store the new values if appropriate
    		if (newRollingAvg < rollingAvg) rollingAvg = newRollingAvg;
    		if (newMax > max) max = newMax;

    		// Add to the overall average
    		avg += array[i][1];
    	}
    	
    	// The overall average is missing values so add them before calculating the avg
    	for(int i = array.length - rollingAvgLength; i < array.length; i++) avg += array[i][1];
    	avg = avg/array.length;
    	
    	System.out.println("rolling average : " + rollingAvg + " max : " + max + " avg : " + avg);
    	
    	// Find the locations where the data points 
    }
    
    // TODO compare days of the week when people are in the office to days when they are not
    // TODO use IO as a measure of use, better than CPU

}
