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
		
		public GroupItem(String type, String group, String subgroup, HashMap<String,String> tags){
			this.type = type;
			this.group = group;
			this.subgroup = subgroup;
			this.tags = tags;

		}
		
	}
	
	public GroupsOfGroups(){
		list = new ArrayList<GroupItem>();
	}
	
    public int insert(String type, String group, String subgroup, HashMap<String,String> tags){
    	GroupItem new_item = new GroupItem(type,group,subgroup,tags);
    	this.list.add(new_item);
    	return 0; //All good
    }
    
    public String getGroup(String type, String subgroup)
    {
    	for (GroupItem item : list)
    	{
    		if (item.type.equals(type) && item.subgroup.equals(subgroup))
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
		
		// Grab avro schema 
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
			String subgroup = avroRow.get("subgroup").toString();
			
			
			// Insert data to list
			this.insert(type,group,subgroup,tagMap);
			
		} // end of avro rows
	
		dataFileReader.close();
		
		return 0; // allgood
	}
	
	
}
