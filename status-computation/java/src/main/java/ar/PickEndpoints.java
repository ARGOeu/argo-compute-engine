package ar;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

import sync.EndpointGroups;
import sync.MetricProfiles;

public class PickEndpoints extends FilterFunc {
    
	public EndpointGroups endpointMgr;
	public MetricProfiles metricMgr;
	
	public String fnEndpointGroups;
	public String fnMetricProfiles;
	
	public boolean initialized = false;
	
	public PickEndpoints(String fnEndpointGroups, String fnMetricProfiles) throws IOException{
		// set first the filenames
		this.fnEndpointGroups = fnEndpointGroups;
		this.fnMetricProfiles = fnMetricProfiles;
		// set the Structures
		this.endpointMgr=new EndpointGroups();
		this.metricMgr = new MetricProfiles();
		
	}
	
	public void init() throws IOException
	{
		this.endpointMgr.loadAvro(new File("./endpoint_groups"));
		this.metricMgr.loadAvro(new File("./metric_profiles"));
		this.initialized=true;
	}
	
	public List<String> getCacheFiles() { 
        List<String> list = new ArrayList<String>(); 
        list.add(this.fnEndpointGroups.concat("#endpoint_groups"));
        list.add(this.fnMetricProfiles.concat("#metric_profiles"));
        return list; 
	} 
	
	@Override
    public Boolean exec(Tuple input) throws IOException {
        if (this.initialized==false)
        {
        	this.init();
        }
		
		if (input == null || input.size() == 0) return null;
        
        //Get Arguments
        String hostname = (String)input.get(0);
        String service = (String)input.get(1);
        String metric = (String)input.get(2);
        
        //Only 1 profile per job
        String prof = metricMgr.getProfiles().get(0);
        //Filter By profile first
        if (metricMgr.checkProfileServiceMetric(prof, service, metric) == false ) return null;
        //Filter By topology
        if (endpointMgr.checkEndpoint(hostname, service) == false) return null;
        
        //Filter in the future by tags
        
        
        return true;
        
    }
	
	
	 
	
	
}
