package com.destini.vmstats;

import java.util.ArrayList;

public class MetricEvents {

	public ArrayList<Long> timestamp = null;
	public ArrayList<String> memMax = null;
	public ArrayList<String> memAvg = null;
	public ArrayList<String> memMin = null;
	public ArrayList<String> cpuMax = null;
	public ArrayList<String> cpuAvg = null;
	public ArrayList<String> cpuMin = null;
	public ArrayList<String> netMin = null;
	public ArrayList<String> netAvg = null;
	public ArrayList<String> netMax = null;

	
	public MetricEvents(ArrayList<Long> timestamp, ArrayList<String> memMax, ArrayList<String> memAvg,
			ArrayList<String> memMin, ArrayList<String> cpuMax, ArrayList<String> cpuAvg, ArrayList<String> cpuMin,
			ArrayList<String> netMin, ArrayList<String> netAvg, ArrayList<String> netMax) {
		super();
		this.timestamp = timestamp;
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

	
	public MetricEvents() {
		super();
		this.timestamp = new ArrayList<Long>(2000);
		this.memMax = new ArrayList<String>(2000);
		this.memAvg = new ArrayList<String>(2000);
		this.memMin = new ArrayList<String>(2000);
		this.cpuMax = new ArrayList<String>(2000);
		this.cpuAvg = new ArrayList<String>(2000);
		this.cpuMin = new ArrayList<String>(2000);
		this.netMin = new ArrayList<String>(2000);
		this.netAvg = new ArrayList<String>(2000);
		this.netMax = new ArrayList<String>(2000);
	}

	
	public void addTimestamp(long timestamp) {
		this.timestamp.add(new Long(timestamp));
	}

	public void addMemMax(String i) {
		this.memMax.add(new String(i));
	}
	
	public void addMemMin(String i) {
		this.memMin.add(new String(i));
	}
	
	public void addMemAvg(String i) {
		this.memAvg.add(new String(i));
	}
	
	public void addCpuMax(String i) {
		this.cpuMax.add(new String(i));
	}
	
	public void addCpuMin(String i) {
		this.cpuMin.add(new String(i));
	}
	
	public void addCpuAvg(String i) {
		this.cpuAvg.add(new String(i));
	}
	
	public void addNetMax(String i) {
		this.netMax.add(new String(i));
	}
	
	public void addNetMin(String i) {
		this.netMin.add(new String(i));
	}
	
	public void addNetAvg(String i) {
		this.netAvg.add(new String(i));
	}
	
	
}
