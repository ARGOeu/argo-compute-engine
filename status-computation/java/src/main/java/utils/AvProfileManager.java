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

import org.bson.types.BasicBSONList;

public class AvProfileManager {
	
	private Map<String,HashMap<String,ArrayList<String>>>  profileList;
	
	
	public AvProfileManager(){
		profileList = new HashMap<String,HashMap<String,ArrayList<String>>>();
	}
	
	
	
	public int insertProfile(String profile) {
		if (!profileList.containsKey(profile)) {
			profileList.put(profile, new HashMap<String,ArrayList<String>>());
			return 0;
		}
		return -1;
	}
	
	public int insertGroup(String profile, String group){
		if (profileList.containsKey(profile)) {
			if (profileList.get(profile).containsKey(group)) {
				return -1;
			} 
			else { 
				profileList.get(profile).put(group, new ArrayList<String>());
				return 0;
			}	
			
		} 
		
		profileList.put(profile, new HashMap<String,ArrayList<String>>());
		profileList.get(profile).put(group, new ArrayList<String>());
		return 0;
		
	}
	
	public int loadFromMongo(String mongo_host, int mongo_port) throws UnknownHostException{
		
		profileList.clear();
		
		
		MongoClient mongo = new MongoClient( mongo_host , mongo_port );
		DB db = mongo.getDB("AR");
		DBCollection table = db.getCollection("aps");
		DBCursor cursor = table.find();
		
		
		
		while (cursor.hasNext()) {
			DBObject cur_item = cursor.next();
			//this.insertService(cur_item.get("p").toString(), cur_item.get("s").toString(), cur_item.get("m").toString());
			
			// get the group
			int i=0;
			BasicBSONList grp_list = (BasicBSONList)cur_item.get("groups");
	        for (i=0;i<grp_list.size();i++)
	        {
	        	
	        	ArrayList el_list = (ArrayList)grp_list.get(i);
	        	int j=0;
	        	for (j=0;j<el_list.size();j++){
	        		this.insertService(cur_item.get("name").toString(), "group_"+i,el_list.get(j).toString());
	        	}
	        	

	        }
			
		}
		
		return 0;
	}
	
	public String getServiceGroup(String profile, String service)
	{
		if (profileList.containsKey(profile)) 
		{
			
			for (String item: profileList.get(profile).keySet())
			{
				
				if (profileList.get(profile).get(item).contains(service))
				{
					return item;
				}
				
			}
		}
		
		return "";
	}
	
	
	
	public int insertService(String profile, String group, String service){
		if (profileList.containsKey(profile)) {
			if (profileList.get(profile).containsKey(group)) {
				if (profileList.get(profile).get(group).contains(service)) {
					// Metric exists so no insertion
					return -1;
				}
				// Metric doesn't exist and must be added
				profileList.get(profile).get(group).add(service);
				
				return 0;
			} 
			else { 
				// Create the service and the metric
				profileList.get(profile).put(group, new ArrayList<String>());
				profileList.get(profile).get(group).add(service);
				
				return 0;
			}
			
		} 
		// No profile - service - metric so add them all
		profileList.put(profile, new HashMap<String,ArrayList<String>>());
		profileList.get(profile).put(group, new ArrayList<String>());
		profileList.get(profile).get(group).add(service);
		
		return 0;
		
	}
	
	public void clearProfiles(){
		profileList = new HashMap<String,HashMap<String,ArrayList<String>>>();
	}
	
	public ArrayList<String> getProfileGroups(String profile){
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
	
	public ArrayList<String> getProfileGroupServices(String profile, String group){
		if (profileList.containsKey(profile)){
			if (profileList.get(profile).containsKey(group)){
				return  profileList.get(profile).get(group);
			}
		}
		return null;
	}
	
	public boolean checkProfileServiceMetric(String profile, String group, String service)
	{
		if (profileList.containsKey(profile)){
			if (profileList.get(profile).containsKey(group)){
				if (profileList.get(profile).get(group).contains(service))
					return true;
			}
		}
		
		return false;
	}
	
}
