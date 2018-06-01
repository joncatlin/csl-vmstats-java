package com.destini.vmstats;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StatisticRemoveSpikes {

	// Initialize the logger
	private final Logger LOGGER = Logger.getLogger(this.getClass().getName());
	
	public ArrayList<String> removeSpikes (ArrayList<String>metrics, int sampleSize, int numberOfZeros) {

		ArrayList<String> transformed = new ArrayList<String>(metrics.size());
		final String ZERO = "0.0";
		
		// Loop through the data looking at the sample size. When a sample size contains the correct
		// number of zeros, with a zero at the beginning and the end then remove all points in the sample
		for (int index = 1; index < metrics.size()-sampleSize; index++) {

			if (!metrics.get(index).equals(ZERO)) {
				// Check the preceding value and the end of the sample for zeros.
				// If they are zeros then determine if the sample contains the correct number of zeros. 
				// If it does then zero all the samples
				if ((metrics.get(index-1).equals(ZERO)) && (metrics.get(index-1+sampleSize-1).equals(ZERO))) {
					int zeroCount=2;
					for (int i=1; i < sampleSize-2; i++) {
						if (metrics.get(index+i).equals(ZERO)) zeroCount++;
					}

					// If the number of zeros matches or greater than the required amount, then zeroize
					// all the values in the sample to remove any spikes in the data
					if (zeroCount >= numberOfZeros) {
						for (int i=0; i < sampleSize-2; i++) {
							metrics.set(index+i, ZERO);
						}
					}
					
					// Increment the index in the array so we jump those elements just set to zero
					index+=sampleSize-1;
				}
			}
		}

		return metrics;
	}
	

}
