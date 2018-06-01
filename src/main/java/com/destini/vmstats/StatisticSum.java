package com.destini.vmstats;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StatisticSum {

	// Initialize the logger
	private final Logger LOGGER = Logger.getLogger(this.getClass().getName());

	
	public void processSum (String vmName, String date, MetricEvents me) {

		float sum = 0;

		// TODO store the sums in the DB
		sum = calcSum(vmName, date, me.cpuAvg, 0, -1);
		LOGGER.log(Level.FINE, "vmName: {0}, date: {1}, sum for cpuAvg: {2}", new Object[] {vmName, date, sum}); 
		sum = calcSum(vmName, date, me.cpuMax, 0, -1);
		LOGGER.log(Level.FINE, "vmName: {0}, date: {1}, sum for cpuMax: {2}", new Object[] {vmName, date, sum}); 
		sum = calcSum(vmName, date, me.cpuMin, 0, -1);
		LOGGER.log(Level.FINE, "vmName: {0}, date: {1}, sum for cpuMin: {2}", new Object[] {vmName, date, sum}); 
		sum = calcSum(vmName, date, me.memAvg, 0, -1);
		LOGGER.log(Level.FINE, "vmName: {0}, date: {1}, sum for memAvg: {2}", new Object[] {vmName, date, sum}); 
		sum = calcSum(vmName, date, me.memMax, 0, -1);
		LOGGER.log(Level.FINE, "vmName: {0}, date: {1}, sum for memMax: {2}", new Object[] {vmName, date, sum}); 
		sum = calcSum(vmName, date, me.memMin, 0, -1);
		LOGGER.log(Level.FINE, "vmName: {0}, date: {1}, sum for memMin: {2}", new Object[] {vmName, date, sum}); 
		sum = calcSum(vmName, date, me.netAvg, 0, -1);
		LOGGER.log(Level.FINE, "vmName: {0}, date: {1}, sum for netAvg: {2}", new Object[] {vmName, date, sum}); 
		sum = calcSum(vmName, date, me.netMax, 0, -1);
		LOGGER.log(Level.FINE, "vmName: {0}, date: {1}, sum for netMax: {2}", new Object[] {vmName, date, sum}); 
		sum = calcSum(vmName, date, me.netMin, 0, -1);
		LOGGER.log(Level.FINE, "vmName: {0}, date: {1}, sum for netMin: {2}", new Object[] {vmName, date, sum}); 
	}
    
    
    public float calcSum (String vmName, String date, ArrayList<String> metrics, int start, int end) {

    	float sum = 0;
    	
    	// Set the end to the end of the array if -1
    	if (end == -1) end = metrics.size();
    	
    	// Ensure the end point is within the number of data points
    	if (end > metrics.size()) {
        	LOGGER.log(Level.WARNING, "vmName: {0}, date: {1}, end: {2}, datapoints.size(): {3}, Value of end is outside datapoints array, IGNORING this metric.", new Object[] {vmName, date, end, metrics.size()}); 
    	}

    	// Ensure the start point is within the number of data points
    	if (start > metrics.size()) {
        	LOGGER.log(Level.WARNING, "vmName: {0}, date: {1}, end: {2}, datapoints.size(): {3}, Value of start is outside datapoints array, IGNORING this metric.", new Object[] {vmName, date, start, metrics.size()}); 
    	}

    	// Calculate the sum of the datapoints
    	for(int i = start; i < metrics.size(); i++) {
			sum += Float.valueOf(metrics.get(i));
    	}
    	
    	return sum;
    }

    
    

}
