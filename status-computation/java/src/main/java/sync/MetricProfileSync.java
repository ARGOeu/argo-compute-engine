package sync;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class MetricProfileSync {
	
	private ArrayList<MetricProfile> profileList;
	private Map<String,HashMap<String,ArrayList<String>>>  profileIndex;
	
	private class MetricProfile
	{
		String profile;	//Name of the profile
		String service; //Name of the service type
		String metric;  //Name of the metric
		Map<String,String> tags; //Tag list
		
		public MetricProfile(){
			// Initializations
			this.profile="";
			this.service="";
			this.metric="";
			this.tags = new HashMap<String,String>();
		}
	}
	
	public MetricProfileSync(){
		this.profileList = new ArrayList<MetricProfile>();
		this.profileIndex = new HashMap<String,HashMap<String,ArrayList<String>>>();
	}
	
	// Clear all profile data (both list and indexes)
	public void clear(){
		this.profileList = new ArrayList<MetricProfile>();
		this.profileIndex = new HashMap<String,HashMap<String,ArrayList<String>>>();
	}
	
	// Indexed List Functions
	public int indexInsertProfile(String profile) {
		if (!profileIndex.containsKey(profile)) {
			profileIndex.put(profile, new HashMap<String,ArrayList<String>>());
			return 0;
		}
		return -1;
	}
	
	public int indexInsertService(String profile, String service){
		if (profileIndex.containsKey(profile)) {
			if (profileIndex.get(profile).containsKey(service)) {
				return -1;
			} 
			else { 
				profileIndex.get(profile).put(service, new ArrayList<String>());
				return 0;
			}	
			
		} 
		
		profileIndex.put(profile, new HashMap<String,ArrayList<String>>());
		profileIndex.get(profile).put(service, new ArrayList<String>());
		return 0;
		
	}
	
	public int indexInsertMetric(String profile, String service, String metric){
		if (profileIndex.containsKey(profile)) {
			if (profileIndex.get(profile).containsKey(service)) {
				if (profileIndex.get(profile).get(service).contains(metric)) {
					// Metric exists so no insertion
					return -1;
				}
				// Metric doesn't exist and must be added
				profileIndex.get(profile).get(service).add(metric);
				return 0;
			} 
			else { 
				// Create the service and the metric
				profileIndex.get(profile).put(service, new ArrayList<String>());
				profileIndex.get(profile).get(service).add(metric);
				return 0;
			}
			
		} 
		// No profile - service - metric so add them all
		profileIndex.put(profile, new HashMap<String,ArrayList<String>>());
		profileIndex.get(profile).put(service, new ArrayList<String>());
		profileIndex.get(profile).get(service).add(metric);
		return 0;
		
	}
	
	// Getter Functions
	
	public ArrayList<String> getProfileServices(String profile){
		if (profileIndex.containsKey(profile)){
			ArrayList<String> ans = new ArrayList<String>();
			ans.addAll(profileIndex.get(profile).keySet());
			return ans;
		}
		return null;
		
	}
	
	public ArrayList<String> getProfiles(){
		if (profileIndex.size()>0){
			ArrayList<String> ans = new ArrayList<String>();
			ans.addAll(profileIndex.keySet());
			return ans;
		}
		return null;
	}
	
	public ArrayList<String> getProfileServiceMetrics(String profile, String service){
		if (profileIndex.containsKey(profile)){
			if (profileIndex.get(profile).containsKey(service)){
				return  profileIndex.get(profile).get(service);
			}
		}
		return null;
	}
	
	public boolean checkProfileServiceMetric(String profile, String service, String metric)
	{
		if (profileIndex.containsKey(profile)){
			if (profileIndex.get(profile).containsKey(service)){
				if (profileIndex.get(profile).get(service).contains(metric))
					return true;
			}
		}
		
		return false;
	}
	
	

}
