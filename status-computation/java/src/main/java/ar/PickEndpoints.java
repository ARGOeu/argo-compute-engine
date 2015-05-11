package ar;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import ops.ConfigManager;

import org.apache.log4j.Logger;
import org.apache.pig.FilterFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

import sync.AvailabilityProfiles;
import sync.EndpointGroups;
import sync.GroupsOfGroups;
import sync.MetricProfiles;

public class PickEndpoints extends FilterFunc {
	
	private static final Logger LOG = Logger.getLogger(PickEndpoints.class.getName());
	public EndpointGroups egMgr;
	public GroupsOfGroups ggMgr;
	public MetricProfiles mpsMgr;
	public AvailabilityProfiles apsMgr;
	
	
	public ConfigManager cfgMgr;
	
	private String fnEgrp;
	private String fnMps;
	private String fnGgrp;
	private String fnCfg;
	private String fnAps;
	private int filter;
	
	private String fsUsed; // local,hdfs,cache (distrubuted_cache)
	
	private boolean initialized = false;
	
	public PickEndpoints(String fnEgrp, String fnMps, String fnAps, String fnGgrp, String fnCfg, String filter, String fsUsed) throws IOException{
		// set first the filenames
		this.fnEgrp = fnEgrp;
		this.fnMps = fnMps;
		this.fnGgrp = fnGgrp;
		this.fnCfg = fnCfg;
		this.fnAps = fnAps;
		// set the Structures
		this.egMgr=new EndpointGroups();
		this.ggMgr=new GroupsOfGroups();
		this.mpsMgr = new MetricProfiles();
		this.cfgMgr = new ConfigManager();
		this.apsMgr = new AvailabilityProfiles();
		this.fsUsed = fsUsed;
		
		this.filter = Integer.parseInt(filter);
		
	}
	
	public void init() throws IOException
	{
		if (this.fsUsed.equalsIgnoreCase("cache")){
			this.egMgr.loadAvro(new File("./egroups"));
			this.ggMgr.loadAvro(new File("./ggroups"));
			this.mpsMgr.loadAvro(new File("./mps"));
			this.cfgMgr.loadJson(new File("./cfg"));
			this.apsMgr.loadJson(new File("./aps"));
		}
		else if (this.fsUsed.equalsIgnoreCase("local")) {
			this.egMgr.loadAvro(new File(this.fnEgrp));
			this.ggMgr.loadAvro(new File(this.fnGgrp));
			this.mpsMgr.loadAvro(new File(this.fnMps));
			this.cfgMgr.loadJson(new File(this.fnCfg));
			this.apsMgr.loadJson(new File(this.fnAps));
			
		}
		
		//Apply tag filters
		applyFilters();
		
		this.initialized=true;
	}
	
	public void applyFilters(){
		this.egMgr.filter(cfgMgr.egroupTags);
		this.ggMgr.filter(cfgMgr.ggroupTags);
	}
	
	public List<String> getCacheFiles() { 
		List<String> list = new ArrayList<String>(); 
		list.add(this.fnEgrp.concat("#egroups"));
		list.add(this.fnGgrp.concat("#ggroups"));
		list.add(this.fnCfg.concat("#cfg"));
		list.add(this.fnMps.concat("#mps"));
		list.add(this.fnAps.concat("#aps"));
		return list; 
	} 
	
	@Override
	public Boolean exec(Tuple input) {
		if (this.initialized == false)
		{
			try {
				this.init();
			} catch (IOException e) {
				LOG.error("Could not initialize sync structures");
				throw new RuntimeException("pig Eval Init Error");
			} 
		}
		
		if (input == null || input.size() == 0) return false;
		
		// Check and get tuple input
		String hostname;
		String service;
		String metric;
		
		try {
			//Get Arguments
			hostname = (String)input.get(0);
			service = (String)input.get(1);
			metric = (String)input.get(2);
		} catch (ClassCastException e) {
			LOG.error("Failed to cast input to approriate type");
			LOG.error("Bad tuple input:" + input.toString());
			throw new RuntimeException("pig Eval bad input");
		} catch (IndexOutOfBoundsException e) {
			LOG.error("Malformed tuple schema");
			LOG.error("Bad tuple input:" + input.toString());
			throw new RuntimeException("pig Eval bad input");
		} catch (ExecException e) {
			LOG.error("Execution error");
			throw new RuntimeException("pig Eval bad input");
		}
		
		//Only 1 profile per job
		String prof = mpsMgr.getProfiles().get(0);
		String aprof = apsMgr.getAvProfiles().get(0);
		
		// If filtering by profiles is enabled
		if (this.filter == 1) {
			
			//Filter By availability profile
			if (apsMgr.checkService(aprof, service) == false) return false;
			//Filter By metric profile 
			if (mpsMgr.checkProfileServiceMetric(prof, service, metric) == false ) return false;
			//Filter By endpoint if belongs to an endpoint group
			
		}
		
		if (egMgr.checkEndpoint(hostname, service) == false) return false;
		
		//Filter By endpoint group if belongs to supergroup
		String groupname = egMgr.getGroup(this.cfgMgr.egroup, hostname, service);
		if (ggMgr.checkSubGroup(groupname) == false) return false;
		
		return true;
		
	}
	
	
}
