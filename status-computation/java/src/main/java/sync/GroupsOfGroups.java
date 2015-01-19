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


public class GroupsOfGroups {

private ArrayList<GroupItem> list;
	
	
	private class GroupItem
	{
		String type; 	  			//type of group
		String group; 	  			// name of the group
		String subgroup;   			// name of sub-group
		HashMap<String,String> tags; //Tag list
		
		public GroupItem(){
			// Initializations
			this.type=""; 
			this.group=""; 	  
			this.subgroup="";   
			this.tags = new HashMap<String,String>();
		}
		
		public GroupItem(String _type, String _group, String _subgroup, HashMap<String,String> _tags){
			this.type = _type;
			this.group = _group;
			this.subgroup = _subgroup;
			this.tags = _tags;

		}
		
	}
	
	public GroupsOfGroups(){
		list = new ArrayList<GroupItem>();
	}
	
    public int insert(String _type, String _group, String _subgroup, HashMap<String,String> _tags){
    	GroupItem new_item = new GroupItem(_type,_group,_subgroup,_tags);
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
			String service = avro_row.get("subgroup").toString();
			
			
			// Insert data to list
			this.insert(type,group,service,tag_map);
			
		} // end of avro rows
	
		dataFileReader.close();
		
		return 0; // allgood
	}
	
	
}
