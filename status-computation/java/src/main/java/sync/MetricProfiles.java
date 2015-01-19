package sync;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class MetricProfiles {
	
	private ArrayList<MetricProfile> list;
	private Map<String,HashMap<String,ArrayList<String>>>  index;
	
	private class MetricProfile
	{
		String profile;	//Name of the profile
		String service; //Name of the service type
		String metric;  //Name of the metric
		HashMap<String,String> tags; //Tag list
		
		public MetricProfile(){
			// Initializations
			this.profile="";
			this.service="";
			this.metric="";
			this.tags = new HashMap<String,String>();
		}
		
		public MetricProfile(String _profile, String _service, String _metric, HashMap<String,String> _tags)
		{
			this.profile = _profile;
			this.service = _service;
			this.metric = _metric;
			this.tags = _tags;
		}
	}
	
	public MetricProfiles(){
		this.list = new ArrayList<MetricProfile>();
		this.index = new HashMap<String,HashMap<String,ArrayList<String>>>();
	}
	
	// Clear all profile data (both list and indexes)
	public void clear(){
		this.list = new ArrayList<MetricProfile>();
		this.index = new HashMap<String,HashMap<String,ArrayList<String>>>();
	}
	
	// Indexed List Functions
	public int indexInsertProfile(String profile) {
		if (!index.containsKey(profile)) {
			index.put(profile, new HashMap<String,ArrayList<String>>());
			return 0;
		}
		return -1;
	}
	
	public int indexInsertService(String profile, String service){
		if (index.containsKey(profile)) {
			if (index.get(profile).containsKey(service)) {
				return -1;
			} 
			else { 
				index.get(profile).put(service, new ArrayList<String>());
				return 0;
			}	
			
		} 
		
		index.put(profile, new HashMap<String,ArrayList<String>>());
		index.get(profile).put(service, new ArrayList<String>());
		return 0;
		
	}
	
	public int indexInsertMetric(String profile, String service, String metric){
		if (index.containsKey(profile)) {
			if (index.get(profile).containsKey(service)) {
				if (index.get(profile).get(service).contains(metric)) {
					// Metric exists so no insertion
					return -1;
				}
				// Metric doesn't exist and must be added
				index.get(profile).get(service).add(metric);
				return 0;
			} 
			else { 
				// Create the service and the metric
				index.get(profile).put(service, new ArrayList<String>());
				index.get(profile).get(service).add(metric);
				return 0;
			}
			
		} 
		// No profile - service - metric so add them all
		index.put(profile, new HashMap<String,ArrayList<String>>());
		index.get(profile).put(service, new ArrayList<String>());
		index.get(profile).get(service).add(metric);
		return 0;
		
	}
	
	// Getter Functions
	
	public ArrayList<String> getProfileServices(String profile){
		if (index.containsKey(profile)){
			ArrayList<String> ans = new ArrayList<String>();
			ans.addAll(index.get(profile).keySet());
			return ans;
		}
		return null;
		
	}
	
	public ArrayList<String> getProfiles(){
		if (index.size()>0){
			ArrayList<String> ans = new ArrayList<String>();
			ans.addAll(index.keySet());
			return ans;
		}
		return null;
	}
	
	public ArrayList<String> getProfileServiceMetrics(String profile, String service){
		if (index.containsKey(profile)){
			if (index.get(profile).containsKey(service)){
				return  index.get(profile).get(service);
			}
		}
		return null;
	}
	
	public boolean checkProfileServiceMetric(String profile, String service, String metric)
	{
		if (index.containsKey(profile)){
			if (index.get(profile).containsKey(service)){
				if (index.get(profile).get(service).contains(metric))
					return true;
			}
		}
		
		return false;
	}
	
	

}
