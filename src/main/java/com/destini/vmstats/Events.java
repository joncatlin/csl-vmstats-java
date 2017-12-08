package com.destini.vmstats;

import java.util.ArrayList;

public class Events {

	public ArrayList<Long> timestamp = new ArrayList<Long>(300);
	public ArrayList<Integer> memMax = new ArrayList<Integer>(300);
	public ArrayList<Integer> memAvg = new ArrayList<Integer>(300);
	public ArrayList<Integer> memMin = new ArrayList<Integer>(300);
	public ArrayList<Integer> cpuMax = new ArrayList<Integer>(300);
	public ArrayList<Integer> cpuAvg = new ArrayList<Integer>(300);
	public ArrayList<Integer> cpuMin = new ArrayList<Integer>(300);
	public ArrayList<Integer> netMin = new ArrayList<Integer>(300);
	public ArrayList<Integer> netAvg = new ArrayList<Integer>(300);
	public ArrayList<Integer> netMax = new ArrayList<Integer>(300);

	public void addTimestamp(long timestamp) {
		this.timestamp.add(new Long(timestamp));
	}

	public void addMemMax(int i) {
		this.memMax.add(new Integer(i));
	}
	
	public void addMemMin(int i) {
		this.memMin.add(new Integer(i));
	}
	
	public void addMemAvg(int i) {
		this.memAvg.add(new Integer(i));
	}
	
	public void addCpuMax(int i) {
		this.cpuMax.add(new Integer(i));
	}
	
	public void addCpuMin(int i) {
		this.cpuMin.add(new Integer(i));
	}
	
	public void addCpuAvg(int i) {
		this.cpuAvg.add(new Integer(i));
	}
	
	public void addNetMax(int i) {
		this.netMax.add(new Integer(i));
	}
	
	public void addNetMin(int i) {
		this.netMin.add(new Integer(i));
	}
	
	public void addNetAvg(int i) {
		this.netAvg.add(new Integer(i));
	}
	
	
}
