package com.destini.vmstats;

import java.text.SimpleDateFormat;
import java.util.Map;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

public class Start {

	
	// Constants
	static final String ENV_COUCHBASE_NODES = "COUCHBASE_NODES";
	static final String ENV_STATSD_SERVER = "STATSD_SERVER";
	static final String ENV_DIRNAME = "DIRNAME";
	static final String ENV_FILTYPE = "FILETYPE";
	static final long A_LONG_TIME = Long.MAX_VALUE;
	
	// Instance variables
	static String couchbaseNodes = null;
	static String fileType = null;
	static String statsdServer = null;
	static String dirname = "dummyfilename";
	SimpleDateFormat sm = new SimpleDateFormat("yyyymmdd");


	public static void main(String[] args) {
		
		getEnvVariables();

		// Open a connection to the DB
		Bucket bucket = getConnection();

		// Create the thread that will process files and load any statistics found into the DB
		Thread loadVMStats = new Thread(new LoadVMStats (bucket, fileType, dirname, statsdServer));
		loadVMStats.start();

		// Wait 
		while (true) {
			try {
				Thread.sleep(A_LONG_TIME);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}		
	}

	
	public static Bucket getConnection() {
		
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


	private static void getEnvVariables() {

		// Get all the environment variables
        Map<String, String> env = System.getenv();

        // Get the couchbase nodes to connect to
        if (env.containsKey(ENV_COUCHBASE_NODES)) {
        	couchbaseNodes = env.get(ENV_COUCHBASE_NODES);
        } else {
        	System.out.println("Missing environment variable: " + ENV_COUCHBASE_NODES);
        	System.exit(0);
        }
        
        if (env.containsKey(ENV_DIRNAME)) {
        	dirname = env.get(ENV_DIRNAME);
        } else {
        	System.out.println("Missing environment variable: " + ENV_DIRNAME);
        	System.exit(0);
        }
        
        
        if (env.containsKey(ENV_FILTYPE)) {
        	fileType = env.get(ENV_FILTYPE);
        } else {
        	System.out.println("Missing environment variable: " + ENV_FILTYPE);
        	System.exit(0);
        }

        if (env.containsKey(ENV_STATSD_SERVER)) {
        	statsdServer = env.get(ENV_STATSD_SERVER);
        } else {
        	System.out.println("Missing environment variable: " + ENV_STATSD_SERVER);
        	System.exit(0);
        }
	}

	
}
