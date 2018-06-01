package com.destini.vmstats;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StatisticPercentiles {

	// Initialize the logger
	private final Logger LOGGER = Logger.getLogger(this.getClass().getName());
	
	public ArrayList<String> percentiles (ArrayList<String>metrics) {

		ArrayList<String> transformed = new ArrayList<String>(metrics.size());
		float max = 0F;
		float percentile = 0F;
		int value = 0;
		
		// First find the max value
		for (int i=0; i < metrics.size(); i++) max = Math.max(max, Float.valueOf(metrics.get(i)));
		
		// Convert the metrics to percentiles 
		for (int i=0; i < metrics.size(); i++) {
			value = Math.round(Float.valueOf(metrics.get(i)) / max * 100); 
			transformed.add(String.valueOf(value));
		}
		
		return transformed;
		
	}
}
