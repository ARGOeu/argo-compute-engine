package sync;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

public class MetricProfiles {
	
	private ArrayList<ProfileItem> list;
	private Map<String,HashMap<String,ArrayList<String>>>  index;
	
	private class ProfileItem
	{
		String profile;	//Name of the profile
		String service; //Name of the service type
		String metric;  //Name of the metric
		HashMap<String,String> tags; //Tag list
		
		public ProfileItem(){
			// Initializations
			this.profile="";
			this.service="";
			this.metric="";
			this.tags = new HashMap<String,String>();
		}
		
		public ProfileItem(String profile, String service, String metric, HashMap<String,String> tags)
		{
			this.profile = profile;
			this.service = service;
			this.metric = metric;
			this.tags = tags;
		}
	}
	
	public MetricProfiles(){
		this.list = new ArrayList<ProfileItem>();
		this.index = new HashMap<String,HashMap<String,ArrayList<String>>>();
	}
	
	// Clear all profile data (both list and indexes)
	public void clear(){
		this.list = new ArrayList<ProfileItem>();
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
	
	public void insert(String profile, String service, String metric, HashMap<String,String> tags )
	{
		ProfileItem tmpProfile = new ProfileItem(profile,service,metric,tags);
		this.list.add(tmpProfile);
		this.indexInsertMetric(profile, service, metric);
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
	
	public int loadAvro(File avroFile) throws IOException{
		
		
		// Prepare Avro File Readers
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(avroFile, datumReader);
		
		// Grab avro schema 
		Schema avroSchema = dataFileReader.getSchema();
		
		// Generate 1st level generic record reader (rows)
		GenericRecord avroRow = new GenericData.Record(avroSchema);
		
		// For all rows in file repeat
		while (dataFileReader.hasNext()) {
			// read the row
			avroRow = dataFileReader.next(avroRow);
			HashMap<String,String> tagMap = new HashMap<String,String>();
			System.out.println(avroRow); 
			// Generate 2nd level generic record reader (tags)
			GenericRecord tags = (GenericRecord) avroRow.get("tags");
			// Grab all available tag fields
			if (tags != null)
			{
				List<Field> tagList = tags.getSchema().getFields();
				// Prepare Hashmap
				
				// Iterate over tag fields & values 
				for (Field item : tagList)
				{
					String fieldName = item.name(); // grab field name
					String fieldValue = null;  
					// if field value not null store it as string value 
					if (tags.get(fieldName) != null)
					{
						fieldValue = tags.get(fieldName).toString();
					}
					tagMap.put(fieldName, fieldValue); // update the tag hashmap
				}
			}
			// Grab 1st level mandatory fields
			String profile = avroRow.get("profile").toString();
			String service = avroRow.get("service").toString();
			String metric = avroRow.get("metric").toString();
			
			
			// Insert data to list
			this.insert(profile,service,metric,tagMap);
			
		} // end of avro rows
	
		dataFileReader.close();
		
		return 0; // allgood
	}

}
