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


public class EndpointGroups {

	private ArrayList<EndpointItem> list;
	
	
	private class EndpointItem
	{
		String type; 	  			//type of group
		String group; 	  			// name of the group
		String service;   			//type of the service
		String hostname;  			// name of host
		HashMap<String,String> tags; //Tag list
		
		public EndpointItem(){
			// Initializations
			this.type=""; 
			this.group=""; 	  
			this.service="";   
			this.hostname="";  
			this.tags = new HashMap<String,String>();
		}
		
		public EndpointItem(String type, String group, String service, String hostname, HashMap<String,String> tags){
			this.type = type;
			this.group = group;
			this.service = service;
			this.hostname = hostname;
			this.tags = tags;

		}
		
	}
	
	public EndpointGroups(){
		list = new ArrayList<EndpointItem>();
	}
	
    public int insert(String type, String group, String service, String hostname, HashMap<String,String> tags){
    	EndpointItem new_item = new EndpointItem(type,group,service,hostname,tags);
    	this.list.add(new_item);
    	return 0; //All good
    }
    
    public boolean checkEndpoint(String hostname, String service)
    {
    	for (EndpointItem item : list)
    	{
    		if (item.hostname.equals(hostname) && item.service.equals(service))
    		{
    			return true;
    		}
    	}
    	
    	return false;
    }
    
    public String getGroup(String type, String hostname, String service)
    {
    	for (EndpointItem item : list)
    	{
    		if (item.type.equals(type) && item.hostname.equals(hostname) && item.service.equals(service))
    		{
    			return item.group; 
    		}
    	}
    	
    	return null;
    }
	
	public int loadAvro(File avroFile) throws IOException{
	
		// Prepare Avro File Readers
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(avroFile, datumReader);
		
		// Grab Avro schema 
		Schema avroSchema = dataFileReader.getSchema();
		
		// Generate 1st level generic record reader (rows)
		GenericRecord avroRow = new GenericData.Record(avroSchema);
		
		// For all rows in file repeat
		while (dataFileReader.hasNext()) {
			// read the row
			avroRow = dataFileReader.next(avroRow);
			HashMap<String,String> tagMap = new HashMap<String,String>();
			
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
			String type = avroRow.get("type").toString();
			String group = avroRow.get("group").toString();
			String service = avroRow.get("service").toString();
			String hostname = avroRow.get("hostname").toString();
			
			// Insert data to list
			this.insert(type,group,service,hostname,tagMap);
			
		} // end of avro rows
	
		dataFileReader.close();
		
		return 0; // allgood
	}
	
}
