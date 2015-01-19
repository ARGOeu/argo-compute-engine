package sync;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;



public class Downtimes {
	

private ArrayList<DowntimeItem> list;
	
	
	private class DowntimeItem
	{
		String hostname; 	  //type of group
		String service; 	  // name of the group
		String start_time;   //type of the service
		String end_time;   //type of the service
		
		
		public DowntimeItem(){
			// Initializations
			this.hostname=""; 
			this.service=""; 	  
			this.start_time="";   
			this.end_time="";
		}
		
		public DowntimeItem(String _hostname, String _service, String _start_time, String _end_time){
			this.hostname = _hostname;
			this.service = _service;
			this.start_time = _start_time;
			this.end_time = _end_time;

		}
		
	}
	
	public Downtimes(){
		list = new ArrayList<DowntimeItem>();
	}
	
    public int insert(String _hostname, String _service, String _start_time,  String _end_time){
    	DowntimeItem new_item = new DowntimeItem(_hostname,_service,_start_time,_end_time);
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
			String hostname = avro_row.get("hostname").toString();
			String service = avro_row.get("service").toString();
			String start_time = avro_row.get("start_time").toString();
			String end_time = avro_row.get("end_time").toString();
			
			// Insert data to list
			this.insert(hostname,service,start_time,end_time);
			
		} // end of avro rows
	
		dataFileReader.close();
		
		return 0; // allgood
	}

}
