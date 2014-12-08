package utils;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class MetricProfileManager {
	
	private Map<String,HashMap<String,ArrayList<String>>>  profileList;

	public MetricProfileManager(){
		profileList = new HashMap<String,HashMap<String,ArrayList<String>>>();
	}
	
	public int insertProfile(String profile) {
		if (!profileList.containsKey(profile)) {
			profileList.put(profile, new HashMap<String,ArrayList<String>>());
			return 0;
		}
		return -1;
	}
	
	public int insertService(String profile, String service){
		if (profileList.containsKey(profile)) {
			if (profileList.get(profile).containsKey(service)) {
				return -1;
			} 
			else { 
				profileList.get(profile).put(service, new ArrayList<String>());
				return 0;
			}	
			
		} 
		
		profileList.put(profile, new HashMap<String,ArrayList<String>>());
		profileList.get(profile).put(service, new ArrayList<String>());
		return 0;
		
	}
	
	public int loadFromMongo(String mongo_host, int mongo_port) throws UnknownHostException{
		
		profileList.clear();
		
		
		MongoClient mongo = new MongoClient( mongo_host , mongo_port );
		DB db = mongo.getDB("AR");
		DBCollection table = db.getCollection("poem_details");
		DBCursor cursor = table.find();
		while (cursor.hasNext()) {
			DBObject cur_item = cursor.next();
			this.insertMetric(cur_item.get("p").toString(), cur_item.get("s").toString(), cur_item.get("m").toString());
		}
		
		return 0;
	}
	
	public int insertMetric(String profile, String service, String metric){
		if (profileList.containsKey(profile)) {
			if (profileList.get(profile).containsKey(service)) {
				if (profileList.get(profile).get(service).contains(metric)) {
					// Metric exists so no insertion
					return -1;
				}
				// Metric doesn't exist and must be added
				profileList.get(profile).get(service).add(metric);
				return 0;
			} 
			else { 
				// Create the service and the metric
				profileList.get(profile).put(service, new ArrayList<String>());
				profileList.get(profile).get(service).add(metric);
				return 0;
			}
			
		} 
		// No profile - service - metric so add them all
		profileList.put(profile, new HashMap<String,ArrayList<String>>());
		profileList.get(profile).put(service, new ArrayList<String>());
		profileList.get(profile).get(service).add(metric);
		return 0;
		
	}
	
	public void clearProfiles(){
		profileList = new HashMap<String,HashMap<String,ArrayList<String>>>();
	}
	
	public ArrayList<String> getProfileServices(String profile){
		if (profileList.containsKey(profile)){
			ArrayList<String> ans = new ArrayList<String>();
			ans.addAll(profileList.get(profile).keySet());
			return ans;
		}
		return null;
		
	}
	
	public ArrayList<String> getProfiles(){
		if (profileList.size()>0){
			ArrayList<String> ans = new ArrayList<String>();
			ans.addAll(profileList.keySet());
			return ans;
		}
		return null;
	}
	
	public ArrayList<String> getProfileServiceMetrics(String profile, String service){
		if (profileList.containsKey(profile)){
			if (profileList.get(profile).containsKey(service)){
				return  profileList.get(profile).get(service);
			}
		}
		return null;
	}
	
	public boolean checkProfileServiceMetric(String profile, String service, String metric)
	{
		if (profileList.containsKey(profile)){
			if (profileList.get(profile).containsKey(service)){
				if (profileList.get(profile).get(service).contains(metric))
					return true;
			}
		}
		
		return false;
	}
	
	
}
