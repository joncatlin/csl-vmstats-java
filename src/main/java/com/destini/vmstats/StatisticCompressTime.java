package com.destini.vmstats;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StatisticCompressTime {

	// Initialize the logger
	private final Logger LOGGER = Logger.getLogger(this.getClass().getName());
	
	public class Results {
		ArrayList<Long> times;
		ArrayList<String> values;
	}


	public Results compressTime (String date, ArrayList<Long>time, ArrayList<String>metrics, int timeWindowInMins) 
			throws ParseException {

		int increments = 24*60/timeWindowInMins;
		
		SimpleDateFormat sm = new SimpleDateFormat("yyyyMMdd");
		long dateInMillis = sm.parse(date).getTime();

    	LOGGER.log(Level.FINEST, "Compressing metrics, date: {0}, timeWindowInMillis: {1}, dateInMillis: {2}", 
    			new Object[] {date, timeWindowInMins, dateInMillis}); 
		
		final int timeWindowInMillis = timeWindowInMins * 60 * 1000;
		
		ArrayList<String> transformed = new ArrayList<String>(increments);
		ArrayList<Long> newTimes = new ArrayList<Long>(increments);
		
		int timeCount = 0;
		for (int incIndex = 0; incIndex < increments; incIndex++) {
			
			float incValue = 0F;
			int numValues = 0;
			long startOfRange = dateInMillis + (incIndex * timeWindowInMillis);
			long endOfRange = dateInMillis + ((incIndex+1) * timeWindowInMillis);

			// Store the start of the time range
			newTimes.add(new Long(startOfRange));
			
			for (int tick = 0; tick < timeWindowInMins; tick++) {
				long tempTime = (timeCount < time.size()) ? time.get(timeCount) : 0;
				if ((tempTime >= startOfRange) && (tempTime < endOfRange)) {
					incValue += Float.valueOf(metrics.get(timeCount));
					numValues++;
					timeCount++;
				} else {
					break;
				}
			}
			
			// Calculate and store the average of the values in the time period
			if (incValue != 0F) {
				incValue = incValue/numValues;
				transformed.add(Float.toString(incValue));
			} else {
				transformed.add(Float.toString(0F));
			}
		}
		
		Results results = new Results();
		results.times = newTimes;
		results.values = transformed;
		return results;
		
	}
	
	public static void main(String[] args) {
		StatisticCompressTime ct = new StatisticCompressTime();

		String date = "20170912";
		SimpleDateFormat sm = new SimpleDateFormat("yyyyMMdd");
		long timeInMillis = 0;
		try {
			timeInMillis = sm.parse(date).getTime();
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		int oneMinuteInMillis = 1000*60;
		int tenMinutesInMillis = oneMinuteInMillis * 10;
		
		ArrayList<Long> times = new ArrayList<Long>();
		ArrayList<String> metrics = new ArrayList<String>();
		times.add(new Long(timeInMillis));						// 00:00
		times.add(new Long(timeInMillis+=oneMinuteInMillis));
		times.add(new Long(timeInMillis+=oneMinuteInMillis));
		times.add(new Long(timeInMillis+=oneMinuteInMillis));
		times.add(new Long(timeInMillis+=oneMinuteInMillis));
		times.add(new Long(timeInMillis+=oneMinuteInMillis));	// 00:05
		// Added ten mins
		times.add(new Long(timeInMillis+=tenMinutesInMillis)); // 00:15
		times.add(new Long(timeInMillis+=oneMinuteInMillis));
		times.add(new Long(timeInMillis+=oneMinuteInMillis));
		times.add(new Long(timeInMillis+=oneMinuteInMillis));
		// Added ten mins
		times.add(new Long(timeInMillis+=tenMinutesInMillis));	// 00:28
		times.add(new Long(timeInMillis+=oneMinuteInMillis));	// 00:29
		times.add(new Long(timeInMillis+=oneMinuteInMillis));	// 00:30
		times.add(new Long(timeInMillis+=oneMinuteInMillis));
		times.add(new Long(timeInMillis+=oneMinuteInMillis));
		times.add(new Long(timeInMillis+=oneMinuteInMillis));

		for (int i=0; i < times.size(); i++) {
			if (i > 12) {
				metrics.add(Long.toString(0));
			} else {
				metrics.add(Long.toString(i));
			}
		}
		
		try {
			Results results = ct.compressTime(date, times, metrics, 15);
			
			// Print out the results
			for (int i=0; i < results.times.size(); i++) {
				Date temp = new Date();
				
				temp.setTime(Long.valueOf(results.times.get(i)));
				System.out.println("dateTime: " + temp + " value: " + results.values.get(i));
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
