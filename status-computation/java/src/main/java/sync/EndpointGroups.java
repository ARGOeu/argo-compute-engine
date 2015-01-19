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
		
		public EndpointItem(String _type, String _group, String _service, String _hostname, HashMap<String,String> _tags){
			this.type = _type;
			this.group = _group;
			this.service = _service;
			this.hostname = _hostname;
			this.tags = _tags;

		}
		
	}
	
	public EndpointGroups(){
		list = new ArrayList<EndpointItem>();
	}
	
    public int insert(String _type, String _group, String _service, String _hostname, HashMap<String,String> _tags){
    	EndpointItem new_item = new EndpointItem(_type,_group,_service,_hostname,_tags);
    	this.list.add(new_item);
    	return 0; //All good
    }
    
	
	public int loadAvro(File avro_file) throws IOException{
		
	
		// Prepare Avro File Readers
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(avro_file, datumReader);
		
		// Grab avro schema 
		Schema avro_schema = dataFileReader.getSchema();
		
		// Generate 1st level generic record reader (rows)
		GenericRecord avro_row = new GenericData.Record(avro_schema);
		
		// For all rows in file repeat
		while (dataFileReader.hasNext()) {
			// read the row
			avro_row = dataFileReader.next(avro_row);
			HashMap<String,String> tag_map = new HashMap<String,String>();
			System.out.println(avro_row); 
			// Generate 2nd level generic record reader (tags)
			GenericRecord tags = (GenericRecord) avro_row.get("tags");
			// Grab all available tag fields
			if (tags != null)
			{
				List<Field> tag_list = tags.getSchema().getFields();
				// Prepare Hashmap
				
				// Iterate over tag fields & values 
				for (Field item : tag_list)
				{
					String field_name = item.name(); // grab field name
					String field_value = null;  
					// if field value not null store it as string value 
					if (tags.get(field_name) != null)
					{
						field_value = tags.get(field_name).toString();
					}
					tag_map.put(field_name, field_value); // update the tag hashmap
				}
			}
			// Grab 1st level mandatory fields
			String type = avro_row.get("type").toString();
			String group = avro_row.get("group").toString();
			String service = avro_row.get("service").toString();
			String hostname = avro_row.get("hostname").toString();
			
			// Insert data to list
			this.insert(type,group,service,hostname,tag_map);
			
		} // end of avro rows
	
		dataFileReader.close();
		
		return 0; // allgood
	}
	
}
