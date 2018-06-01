package com.destini.vmstats;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryRow;

public class ExportVMStatistics {
	
	// Constants
	static final String ENV_COUCHBASE_NODES = "COUCHBASE_NODES";
	static final String ENV_STATSD_SERVER = "STATSD_SERVER";
	static final String ENV_OUTPUT_DIRNAME = "OUTPUT_DIRNAME";
	static final String ENV_FILETYPE = "FILETYPE";
	static final String ENV_VMNAME_PATTERN = "VMNAME_PATTERN";
	static final String ENV_COLUMN_NAME_PATTERN = "COLUMN_NAME_PATTERN";
	static final long A_LONG_TIME = Long.MAX_VALUE;
	private static final int NUM_RETIRES = 3;
	private static final int RETRY_TIME = 1000; // One second
	
	// Instance variables
	static String couchbaseNodes = null;
	static String statsdServer = null;
	static String outputDirname = "dummyfilename";
	static String vmNamePattern = null;
	static String columnNamePattern = null;

	
	SimpleDateFormat sm = new SimpleDateFormat("yyyyMMdd");

	// TODO check that the following are in place or the code will not work
	// CREATE INDEX `vmstats-processed` ON vmstats(processed) WHERE processed = false
	// CREATE PRIMARY INDEX ON vmstats
			
	
	// Logger initialization
	private final Logger LOGGER = Logger.getLogger(this.getClass().getName());
	
	
	public static void main(String[] args) {
		
		ExportVMStatistics pvmStats = new ExportVMStatistics();
		pvmStats.getEnvVariables();

		// Open a connection to the DB
		Bucket bucket = pvmStats.getConnection();

		pvmStats.export(bucket);
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
        
        if (env.containsKey(ENV_OUTPUT_DIRNAME)) {
        	outputDirname = env.get(ENV_OUTPUT_DIRNAME);
        	LOGGER.log(Level.INFO, "Environment variable {0} = {1}", new Object[]{ ENV_OUTPUT_DIRNAME, outputDirname});
        } else {
        	LOGGER.log(Level.SEVERE, "Missing environment variable: {0}", ENV_OUTPUT_DIRNAME);
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

        if (env.containsKey(ENV_COLUMN_NAME_PATTERN)) {
        	columnNamePattern = env.get(ENV_COLUMN_NAME_PATTERN);
        	LOGGER.log(Level.INFO, "Environment variable {0} = {1}", new Object[]{ ENV_COLUMN_NAME_PATTERN, columnNamePattern});
        } else {
        	LOGGER.log(Level.SEVERE, "Missing environment variable: {0}", ENV_COLUMN_NAME_PATTERN);
        	System.exit(0);
        }

	}


	/*
	 * Find all the vms that match the pattern and output the statistics for them so they can be analyzed by different tools
	 */
	private void export(Bucket bucket) {

		// Find all stats that match the pattern
		String n1qlStatement = "SELECT eventDates, vMName FROM vmstats WHERE REGEX_CONTAINS(vMName, \"" + vmNamePattern + "\")";
    	N1qlQuery q = N1qlQuery.simple(n1qlStatement);
    	for (N1qlQueryRow row : bucket.query(q)) {

    		JsonObject json = row.value();
    		JsonArray jsonEventDates = json.getArray("eventDates");
    		String vmName = json.getString("vMName");
 
    		// exportStats (vmName, jsonEventDates.toList(), bucket);
    		exportStats (vmName, jsonEventDates.toList(), bucket);
     	}
	}
	

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void exportStats (String vmName, List<Object> dates, Bucket bucket ) {
    	LOGGER.log(Level.FINE, "Exporting stats for vMName: {0} dates: {1}", new Object[] {vmName, dates}); 

    	BufferedWriter out = null;
		String fileName = outputDirname + "\\" + vmName + ".csv";
		String columnName = null;
		StringBuilder outputLine = new StringBuilder(300);
		boolean firstTime = true;
		
		try {
    		FileWriter fstream = new FileWriter(fileName, false);
    	    out = new BufferedWriter(fstream);    		
    	    
    	    // For each date in the dates array get the statistics for that date then export them
        	for (int i=0; i < dates.size(); i++) {
            	String dateFieldName = VMDocument.createDateFieldName((String)dates.get(i));
        		ArrayList<String> columnNamesToOutput = new ArrayList<String>(20);
        		ArrayList<ArrayList> columnsToOutput = new ArrayList<ArrayList>(20);

            	String n1qlStatement = "SELECT " + dateFieldName + ".* FROM vmstats WHERE META(vmstats).id = \"" + vmName + "\"";
        		N1qlQuery q = N1qlQuery.simple(n1qlStatement);
            	for (N1qlQueryRow row : bucket.query(q)) {

            		// Create a MetricEvents object from the data
            		JsonObject json = row.value();

            		// Add the timestamp to the columns to be output
//    				columnsToOutput.add((ArrayList)json.getArray("timestamp").toList());	
//   				columnNamesToOutput.add("date");	

            		Set<String> names = json.getNames();
            		Iterator it = names.iterator();
            		while (it.hasNext()) {
            			columnName = (String)it.next();
            			
            			// If the column name matches the filter then add it to the ones to output
            			if (columnName.matches(columnNamePattern)) {
            				
            				// If the column is a timestamp then put it at the beginning. Saves time when looking 
            				// at the data in Excel
            				if (columnName.toLowerCase().contains("timestamp")) {
            					ArrayList<ArrayList> temp = new ArrayList<ArrayList>();
            					temp.add((ArrayList)json.getArray(columnName).toList());
                				columnsToOutput.addAll(0, temp);	
            					ArrayList<String> temp2 = new ArrayList<String>();
            					temp2.add(columnName);
                				columnNamesToOutput.addAll(0, temp2);	
            				} else {
                				columnsToOutput.add((ArrayList)json.getArray(columnName).toList());	
                				columnNamesToOutput.add(columnName);	
            					
            				}
            			}
            		}

            		// There is only one of this document 
            		break;
            	}
            	
            	// Write the headings for the columns to the file
            	if (firstTime) {
	        		for (int index = 0; index < columnNamesToOutput.size(); index++) {
	        			if (index != 0) outputLine.append(",");
	        			outputLine.append("\"");
	        			outputLine.append(columnNamesToOutput.get(index));
	        			outputLine.append("\"");
	        		}
	        		outputLine.append("\n");
	        		out.write(outputLine.toString());
	        		firstTime = false;
            	}
            	
            	// Write the metrics for each column to the file
            	for (int index = 0; index < columnsToOutput.get(0).size(); index++) {
            		
            		outputLine.setLength(0);
            		boolean firstCol = true;
            		
            		for (int col = 0; col < columnNamesToOutput.size(); col++) {
            			// Only add a comma field seoerator if not the first column
            			if (!firstCol) outputLine.append(",");
            			
            			// If the column contains timestamp in the name then format it differently
            			if (columnNamesToOutput.get(col).toLowerCase().contains("timestamp")) {
                			outputLine.append(String.format("\"%tD %1$tH:%1$tM\"", columnsToOutput.get(col).get(index)));
            			} else {
	            			outputLine.append("\"");
	            			outputLine.append(columnsToOutput.get(col).get(index));
	            			outputLine.append("\"");
            			}
            			
            			firstCol = false;
            		}
            		outputLine.append("\n");
            		out.write(outputLine.toString());
            	}
        	}
    	}
    	catch (IOException e) {
        	LOGGER.log(Level.SEVERE, "Error while writing to file: {0}, exception {1}", new Object[] {fileName, e}); 
    	}
    	finally {
    		try { if(out != null) out.close(); } catch (IOException e) { /* IGNORE */ }
    	}
	}



	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void exportStatsOld (String vmName, List<Object> dates, Bucket bucket ) {
    	LOGGER.log(Level.FINE, "Exporting stats for vMName: {0} dates: {1}", new Object[] {vmName, dates}); 

    	BufferedWriter out = null;
		String fileName = outputDirname + "\\" + vmName + ".csv";
		String columnName = null;
		StringBuilder outputLine = new StringBuilder(300);
		boolean firstTime = true;
		
		try {
    		FileWriter fstream = new FileWriter(fileName, false);
    	    out = new BufferedWriter(fstream);    		
    	    
    	    // For each date in the dates array get the statistics for that date then export them
        	for (int i=0; i < dates.size(); i++) {
            	String dateFieldName = VMDocument.createDateFieldName((String)dates.get(i));
        		ArrayList<String> columnNamesToOutput = new ArrayList<String>(20);
        		ArrayList<ArrayList> columnsToOutput = new ArrayList<ArrayList>(20);

            	String n1qlStatement = "SELECT " + dateFieldName + ".timestamp, " + dateFieldName + ".* FROM vmstats WHERE META(vmstats).id = \"" + vmName + "\"";
        		N1qlQuery q = N1qlQuery.simple(n1qlStatement);
            	for (N1qlQueryRow row : bucket.query(q)) {

            		// Create a MetricEvents object from the data
            		JsonObject json = row.value();

            		// Add the timestamp to the columns to be output
    				columnsToOutput.add((ArrayList)json.getArray("timestamp").toList());	
    				columnNamesToOutput.add("date");	

            		Set<String> names = json.getNames();
            		Iterator it = names.iterator();
            		while (it.hasNext()) {
            			columnName = (String)it.next();
            			
            			// If the column name matches the filter then add it to the ones to output
            			if (columnName.matches(columnNamePattern)) {
            				columnsToOutput.add((ArrayList)json.getArray(columnName).toList());	
            				columnNamesToOutput.add(columnName);	
            			}
            		}

            		// There is only one of this document 
            		break;
            	}
            	
            	// Write the headings for the columns to the file
            	if (firstTime) {
	        		for (int index = 0; index < columnNamesToOutput.size(); index++) {
	        			if (index != 0) outputLine.append(",");
	        			outputLine.append("\"");
	        			outputLine.append(columnNamesToOutput.get(index));
	        			outputLine.append("\"");
	        		}
	        		outputLine.append("\n");
	        		out.write(outputLine.toString());
	        		firstTime = false;
            	}
            	
            	// Write the metrics for each column to the file
            	for (int index = 0; index < columnsToOutput.get(0).size(); index++) {
            		
            		outputLine.setLength(0);
                	outputLine.append(String.format("\"%tD %1$tH:%1$tM\"", columnsToOutput.get(0).get(index)));

            		for (int col = 1; col < columnNamesToOutput.size(); col++) {
            			outputLine.append(",\"");
            			outputLine.append(columnsToOutput.get(col).get(index));
            			outputLine.append("\"");
            		}
            		outputLine.append("\n");
            		out.write(outputLine.toString());
            	}
        	}
    	}
    	catch (IOException e) {
        	LOGGER.log(Level.SEVERE, "Error while writing to file: {0}, exception {1}", new Object[] {fileName, e}); 
    	}
    	finally {
    		try { if(out != null) out.close(); } catch (IOException e) { /* IGNORE */ }
    	}
	}





}
	
