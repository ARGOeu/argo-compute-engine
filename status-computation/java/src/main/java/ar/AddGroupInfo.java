package ar;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import sync.EndpointGroups;
import sync.GroupsOfGroups;

public class AddGroupInfo extends EvalFunc<Tuple>{
	
	public String fnEndpointGroups;
	public String fnGroupGroups;
	
	public String type;
	
	
	public EndpointGroups endpointMgr;
	//public GroupsOfGroups groupMgr;
	
	private String fsUsed;  // local,hdfs,cache (distrubuted_cache)
	
	private boolean initialized;

	public AddGroupInfo(String fnEndpointGroups, String fnGroupGroups, String type, String fsUsed)
	{
		this.fnEndpointGroups = fnEndpointGroups;
		this.fnGroupGroups = fnGroupGroups;
		
		this.fsUsed = fsUsed;
		
		this.type = type; //type of group
		
		this.endpointMgr = new EndpointGroups();
		//this.groupMgr = new GroupsOfGroups();
		
		
		
	}
	
	
	public void init() throws IOException
	{
		if (this.fsUsed.equalsIgnoreCase("cache")){
			this.endpointMgr.loadAvro(new File("./endpoint_groups"));
			//this.groupMgr.loadAvro(new File("./group_groups"));
		}
		
		this.initialized=true;
	}
	
	public List<String> getCacheFiles() { 
        List<String> list = new ArrayList<String>(); 
        list.add(this.fnEndpointGroups.concat("#endpoint_groups"));
        //list.add(this.fnGroupGroups.concat("#group_groups"));
        return list; 
	}
	
	@Override
	public Tuple exec(Tuple input) throws IOException {
		
		// Check if cache files have been opened 
		if (this.initialized==false)
        {
        	this.init(); // If not open them 
        }
		
		if (input == null || input.size() == 0) return null;

		String service = (String) input.get(0);
		String hostname = (String) input.get(1);
		
		input.append(endpointMgr.getGroup(this.type,hostname,service));
		
		return input;
		
		
	}
	
	@Override
    public Schema outputSchema(Schema input) {
		   
		Schema.FieldSchema service = new Schema.FieldSchema("service", DataType.CHARARRAY);
		Schema.FieldSchema hostname = new Schema.FieldSchema("hostname", DataType.CHARARRAY);
		Schema.FieldSchema groupname = new Schema.FieldSchema("groupname", DataType.CHARARRAY);
		//Schema.FieldSchema slot = new Schema.FieldSchema("slot", DataType.INTEGER);
		Schema.FieldSchema statusInt = new Schema.FieldSchema("status", DataType.INTEGER);
        
        Schema endpoint = new Schema();
        Schema timeline = new Schema();
       
        endpoint.add(service);
        endpoint.add(hostname);
        
        //timeline.add(slot);
        timeline.add(statusInt);

        Schema.FieldSchema tl = null;
        try {
            tl = new Schema.FieldSchema("timeline", timeline, DataType.BAG);
        } catch (FrontendException ex) {
           
        }
        
        endpoint.add(tl);
        endpoint.add(groupname);
        

        try {
            return new Schema(new Schema.FieldSchema("endpoint", endpoint, DataType.TUPLE));
        } catch (FrontendException ex) {
           
        }
	    
	    return null;
        
        
    }
	
	
	
}
