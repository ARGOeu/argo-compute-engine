package sync;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map.Entry;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class AvailabilityProfiles {

	private HashMap<String,AvProfileItem> list;
	
	
	AvailabilityProfiles(){
		
		this.list = new HashMap<String,AvProfileItem>();
	}
	
	private class AvProfileItem {
			
		private String name;
		private String namespace;
		private String metricProfile;
		private String groupType;
		private String op;
		
		private HashMap<String,ServGroupItem> groups;
		
		AvProfileItem(){
			this.groups=new HashMap<String,ServGroupItem>();
		}
		
		private class ServGroupItem{
			
			String op;
			HashMap<String,String> services;
			
			ServGroupItem(String op)
			{
				this.op = op;
				this.services = new HashMap<String,String>();
			}
		}
		
		// ServGroupItem Declaration Ends Here
		
		public void insertGroup(String group, String op){
			if (!this.groups.containsKey(group))
			{
				this.groups.put(group, new ServGroupItem(op));
			}
		}
		
		public void insertService(String group, String service, String op) {
			if (this.groups.containsKey(group))
			{
				this.groups.get(group).services.put(service, op);
			}
		}
	}
	// AvProfileItem Declaration Ends Here
	
	public void clearProfiles(){
		this.list.clear();
	}
	
	public void loadProfileJson(File jsonFile) throws FileNotFoundException{
		
		BufferedReader br = new BufferedReader(new FileReader(jsonFile));
		JsonParser jsonParser = new JsonParser();
		JsonElement jRootElement = jsonParser.parse(br);
		JsonObject jRootObj = jRootElement.getAsJsonObject();
		
		JsonObject apGroups = jRootObj.getAsJsonObject("groups");
		
		// Create new entry for this availability profile
		AvProfileItem tmpAvp = new AvProfileItem();
		
		tmpAvp.name= jRootObj.get("name").getAsString();
		tmpAvp.namespace = jRootObj.get("group_type").getAsString();
		tmpAvp.metricProfile = jRootObj.get("metric_profile").getAsString();
		tmpAvp.groupType = jRootObj.get("group_type").getAsString();
		tmpAvp.op = jRootObj.get("operation").getAsString();
		
		for (Entry<String,JsonElement> item : apGroups.entrySet()){
			// service name
			String itemName = item.getKey();
			JsonObject itemObj = item.getValue().getAsJsonObject();
			String itemOp = itemObj.get("operation").getAsString();
			JsonObject itemServices = itemObj.get("services").getAsJsonObject();
			tmpAvp.insertGroup(itemName,itemOp);
			
			for (Entry<String,JsonElement> subItem : itemServices.entrySet())
			{
				tmpAvp.insertService(itemName, subItem.getKey(),subItem.getValue().getAsString());
			}
			
			
		}
		
		
	}
	
	
}
