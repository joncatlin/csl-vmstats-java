package com.destini.vmstats;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TestDates {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String testTime = "09/08/2017 15:59:40";
		SimpleDateFormat dateTimeFormat = new SimpleDateFormat("M/dd/yyyy H:m");
		long timeInMillis;
		Date dateFromMillis;
		
		long[] times = {1483945200000L, 1483945260000L, 1483948800000L, 1483948860000L, 1483948920000L,
				      1483948980000L, 1483949040000L, 1483949100000L};
		for (int i=0; i < times.length; i++) {
			dateFromMillis = new Date (times[i]);
			System.out.println("Date:" + dateFromMillis);
		}
		Date parsedDate, parsedDateTime;
//		parsedDate = dateFormat.parse(dateTime);
		try {
			parsedDateTime = dateTimeFormat.parse(testTime);
			timeInMillis = parsedDateTime.getTime();
			
			System.out.println("timeInMillis: " + timeInMillis);

			dateFromMillis = new Date (timeInMillis);

			System.out.println("dateFromL=Millis: " + dateFromMillis);
			System.out.println("testTime: " + testTime);

		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
