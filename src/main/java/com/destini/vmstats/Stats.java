package com.destini.vmstats;

public class Stats {
	
	public Stats(String memMax, String memAvg, String memMin, String cpuMax, String cpuAvg, String cpuMin, String netMin,
			String netAvg, String netMax, long timestamp) {
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
		this.timestamp = timestamp;
	}

	String memMax, memAvg, memMin, cpuMax, cpuAvg, cpuMin, netMin, netAvg, netMax;
	long timestamp;

	@Override
	public String toString() {
		return "Stats [memMax=" + memMax + ", memAvg=" + memAvg + ", memMin=" + memMin + ", cpuMax=" + cpuMax
				+ ", cpuAvg=" + cpuAvg + ", cpuMin=" + cpuMin + ", netMin=" + netMin + ", netAvg=" + netAvg
				+ ", netMax=" + netMax + ", timestamp=" + timestamp + "]";
	}
}