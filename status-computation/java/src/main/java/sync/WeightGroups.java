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



public class WeightGroups {

	private ArrayList<WeightItem> list;
	
	
	private class WeightItem
	{
		String type; 	  //type of group
		String group; 	  //name of the group
		String weight;    //weight value
		
		
		
		public WeightItem(){
			// Initializations
			this.type=""; 
			this.group=""; 	  
			this.weight="";   
			
		}
		
		public WeightItem(String _type, String _group, String _weight ){
			this.type = _type;
			this.group = _group;
			this.weight = _weight;
			
		}
		
	}
	
	public WeightGroups(){
		list = new ArrayList<WeightItem>();
	}
	
    public int insert(String _type, String _group, String _weight){
    	WeightItem new_item = new WeightItem(_type,_group,_weight);
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
			String weight = avro_row.get("weight").toString();
			
			
			// Insert data to list
			this.insert(type,group,weight);
			
		} // end of avro rows
	
		dataFileReader.close();
		
		return 0; // allgood
	}
	
}
