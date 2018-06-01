package com.destini.vmstats;

public class VMDocument {

	private String vmName;
	private String lastUpdate;
	private String[] eventDates;
	public MetricEvents[] metricEvents;
	private String dateFieldName = null;
	
	public VMDocument(String vmName, String lastUpdate, String[] eventDates, MetricEvents[] metricEvents) {
		super();
		this.vmName = vmName;
		this.lastUpdate = lastUpdate;
		this.eventDates = eventDates;
		this.metricEvents = metricEvents;

		// Create the name of the field to hold the metrics. It must start with an alpha as all numerics cause
		// the query to fail to find the field.
		this.dateFieldName = VMDocument.createDateFieldName(lastUpdate);
	}

	public static String createDateFieldName(String date) {
		return "date_" + date;
	}

	public String getVmName() {
		return vmName;
	}

	public String getLastUpdate() {
		return lastUpdate;
	}

	public String[] getEventDates() {
		return eventDates;
	}

	public MetricEvents[] getMetricEvents() {
		return metricEvents;
	}

	public String getDateFieldName() {
		return dateFieldName;
	}
	
	
}
