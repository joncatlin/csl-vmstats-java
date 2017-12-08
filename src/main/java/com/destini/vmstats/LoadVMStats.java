package com.destini.vmstats;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.stream.Stream;

import com.couchbase.client.core.message.kv.subdoc.multi.Lookup;
import com.couchbase.client.core.message.kv.subdoc.multi.Mutation;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.subdoc.DocumentFragment;
import com.google.gson.Gson;

public class LoadVMStats implements Runnable {

	public class Stats {

		public Stats(int memMax, int memAvg, int memMin, int cpuMax, int cpuAvg, int cpuMin, int netMin,
				int netAvg, int netMax) {
			super();
			this.memMax = memMax;
			this.memAvg = memAvg;
			this.memMin = memMin;
			this.cpuMax = cpuMax;
			this.cpuAvg = cpuAvg;
			this.cpuMin = cpuMin;
			this.netMin = netMin;
			this.netAvg = netAvg;
			this.netMax = netMax;
		}

		int memMax, memAvg, memMin, cpuMax, cpuAvg, cpuMin, netMin, netAvg, netMax;
	}

	final String SEPERATOR = ",";
	String fileType = null;
	String statsdServer = null;
	String dirname = null;
	SimpleDateFormat sm = new SimpleDateFormat("yyyymmdd");
	Bucket bucket = null;
	
	
	public LoadVMStats (Bucket bucket, String fileType, String dirname, String statsdServer) {
		
		// Save the various parameters in instance variables
		this.bucket = bucket;
		this.fileType = fileType;
		this.dirname = dirname;
		this.statsdServer = statsdServer;
	}

	
	public void run() {
		
		// Loop forever and process any files found
		while (true) {
			try {
				processDir(bucket);
			} catch (IOException e) {

				// TODO Log the error properly
				e.printStackTrace();
			}
		}
	}
		
	
	public void processDir (Bucket bucket) throws IOException {

		// For each file found in the directory
		try (Stream<Path> filePathStream=Files.walk(Paths.get(dirname))) {
		    filePathStream.forEach(filePath -> {
		        if (filePath.getFileName().toString().endsWith(fileType)) {
		            try {
		            	// Create the structure to hold the processed file
		        		HashMap<String, TreeMap<Date, Stats>> processedData = new HashMap<String, TreeMap<Date, Stats>>(300);
		            	
		            	// Process the file and build an internal data structure
		            	processFile(filePath.toString(), processedData);
						
		            	// Transform the data so it is easier to perform calculations on it
		            	HashMap<String, VMStats> transformedData = transformData (processedData);

		            	// Put the data to the DB
		            	upsertMetrics(bucket, transformedData);
						
					} catch (ParseException e) {
						e.printStackTrace();
					}
		        }
		    });
		}
	}
	
	
	public void processFile (String fileName, HashMap<String, TreeMap<Date, Stats>> processedData) throws ParseException {
		
		BufferedReader br = null;
		String line = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("mm/dd/yyy H");

		try {
			// Read the first line to get the column headings
			br = new BufferedReader(new FileReader(fileName));
			line = br.readLine();
			while ((line = br.readLine()) != null) {
		    	processLine(line, processedData, dateFormat);
		    }
		    
		    System.out.println("Finished");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public void processLine(String line, HashMap<String, TreeMap<Date, Stats>> processedData, SimpleDateFormat dateFormat) throws ParseException {

		StringTokenizer st = new StringTokenizer(line, SEPERATOR);
		
		// This code assumes a fixed file format
		String vmName = st.nextToken().replace("\"", "");
		String date = st.nextToken().replace("\"", "");
		String hour = st.nextToken().replace("\"", "");
		String dateTime = date + ' ' + hour;
	    Date parsedDate;
		parsedDate = dateFormat.parse(dateTime);
	    
		Stats eventMetrics = new LoadVMStats.Stats(
				Integer.parseInt(st.nextToken().replace("\"", "")), 
				Integer.parseInt(st.nextToken().replace("\"", "")), 
				Integer.parseInt(st.nextToken().replace("\"", "")), 
				Integer.parseInt(st.nextToken().replace("\"", "")), 
				Integer.parseInt(st.nextToken().replace("\"", "")), 
				Integer.parseInt(st.nextToken().replace("\"", "")), 
				Integer.parseInt(st.nextToken().replace("\"", "")), 
				Integer.parseInt(st.nextToken().replace("\"", "")), 
				Integer.parseInt(st.nextToken().replace("\"", "")));

		// Add the new event data to the structure
		TreeMap<Date, Stats> event = processedData.get(vmName); 
		if (event != null) {
			// An event already exists for this vm so add this to the list
			event.put(parsedDate, eventMetrics);
		} else {
			// This is the first event for the vm
			event = new TreeMap<Date, Stats>();
			event.put(parsedDate, eventMetrics);
			processedData.put(vmName, event);
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



	private void upsertMetrics(Bucket bucket, HashMap<String, VMStats> metrics) {

		boolean metricDateExists;
		String key;
		VMStats stats;
	    long cas = 0;
		
		for (Map.Entry<String, VMStats> metric : metrics.entrySet()) {
		    key = metric.getKey();
		    stats = metric.getValue();
		    
		    try {
			    DocumentFragment<Lookup> result = bucket
				    .lookupIn(key)
				    .exists(stats.lastUpdate)
				    .get("dates")
				    .execute();
			    
				metricDateExists = result.content(stats.lastUpdate, Boolean.class);
				
				if (!metricDateExists) {
					// Create the sub document we are going to add
					JsonObject events = createJsonEvents(stats);
					
					// Add the new metrics to the existing ones and update the dates array to contain the new date
					// the metrics were captured
					DocumentFragment<Mutation> updateResult = bucket
					    .mutateIn(key)
					    .arrayAppend("eventDates", stats.lastUpdate)
					    .upsert("lastUpdate", stats.lastUpdate)
					    .upsert("processed", false)
					    .insert(stats.lastUpdate, events)
					    .execute();				
					cas = updateResult.cas();
					System.out.println("Document inserted with cas : " + cas);
				} else {
					// IGNORE this update as it already exists in the DB
				}
				
		    } catch (DocumentDoesNotExistException e) {
		    	// Insert the metrics into a new document
		    	insertMetrics(bucket, stats);
		    }
		}
	}	

	public JsonObject createJsonEvents(VMStats stats) {

		// Create a JSON document for the Events
		JsonArray timestamp = JsonArray.from(stats.events[0].timestamp);
		JsonArray memMax = JsonArray.from(stats.events[0].memMax);
		JsonArray memMin= JsonArray.from(stats.events[0].memMin);
		JsonArray memAvg = JsonArray.from(stats.events[0].memAvg);
		JsonArray cpuMax = JsonArray.from(stats.events[0].cpuMax);
		JsonArray cpuMin = JsonArray.from(stats.events[0].cpuMin);
		JsonArray cpuAvg = JsonArray.from(stats.events[0].cpuAvg);
		JsonArray netMax = JsonArray.from(stats.events[0].netMax);
		JsonArray netMin= JsonArray.from(stats.events[0].netMin);
		JsonArray netAvg = JsonArray.from(stats.events[0].netAvg);
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
	
	
	private HashMap<String, VMStats> transformData (HashMap<String, TreeMap<Date, Stats>> processedData) {
		
		HashMap<String, VMStats> transformed = new HashMap<String, VMStats>();
		
		// Transform the data into a series of arrays which should make it easier to process
		// Loop through each VMFound in the data
		for (Map.Entry<String, TreeMap<Date, Stats>>data : processedData.entrySet()) {
		    String key = data.getKey();
		    TreeMap<Date, Stats> events = data.getValue();

			// Create the date for indexing the document
		    Date date = (Date)events.firstEntry().getKey();
		    String index = sm.format(date);
			
		    // Loop through all the events found for the VM
			Events e = new Events();
		    Iterator<Date> i = events.keySet().iterator();
		    while (i.hasNext()) {
		    
				// Create the structure for the metrics to be stored in the document
		    	Date eventTime = (Date)i.next();
				Stats singleEvent = events.get(eventTime);
			
				// Fill the structure used to store the metrics
				e.addTimestamp(eventTime.getTime());
				e.addCpuAvg(singleEvent.cpuAvg);
				e.addCpuMax(singleEvent.cpuMax);
				e.addCpuMin(singleEvent.cpuMin);
				e.addMemAvg(singleEvent.memAvg);
				e.addMemMax(singleEvent.memMax);
				e.addMemMin(singleEvent.memMin);
				e.addNetMin(singleEvent.netMin);
				e.addNetAvg(singleEvent.netAvg);
				e.addNetMax(singleEvent.netMax);
			}

			// Create the sub document to store the metrics for a given date in
			VMStats stats = new VMStats();
			stats.lastUpdate = index;
			stats.vmName = key;
			stats.eventDates = new String[1];
			stats.eventDates[0] = index;
			stats.events = new Events[1];
			stats.events[0] = e;
			
			// Store the transformed data
			transformed.put(key, stats);
		}
	 	
		return transformed;
	}

	
	private void insertMetrics(Bucket bucket, VMStats stats) {

	    long cas = 0;
		
	    // Create a JSON document for the Events
		JsonArray timestamp = JsonArray.from(stats.events[0].timestamp);
		JsonArray memMax = JsonArray.from(stats.events[0].memMax);
		JsonArray memMin= JsonArray.from(stats.events[0].memMin);
		JsonArray memAvg = JsonArray.from(stats.events[0].memAvg);
		JsonArray cpuMax = JsonArray.from(stats.events[0].cpuMax);
		JsonArray cpuMin = JsonArray.from(stats.events[0].cpuMin);
		JsonArray cpuAvg = JsonArray.from(stats.events[0].cpuAvg);
		JsonArray netMax = JsonArray.from(stats.events[0].netMax);
		JsonArray netMin= JsonArray.from(stats.events[0].netMin);
		JsonArray netAvg = JsonArray.from(stats.events[0].netAvg);
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
		JsonArray eventDatesArray = JsonArray.from((Object[])stats.eventDates);
		JsonObject doc = JsonObject.create()
            .put("vMName", stats.vmName)
            .put("processed", false)
            .put("lastUpdate", stats.lastUpdate)
            .put("eventDates", eventDatesArray)
            .put(stats.lastUpdate, events);

        // Store the Document
		JsonDocument inserted = bucket.insert(JsonDocument.create(stats.vmName, doc));
		cas = inserted.cas();
		System.out.println("Document inserted with cas : " + cas);;
	}


    
}






