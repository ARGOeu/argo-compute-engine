package ar;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import sync.AvailabilityProfiles;
import sync.EndpointGroups;


public class AddGroupInfo extends EvalFunc<Tuple>{
	
	private static final Logger LOG = Logger.getLogger(AddGroupInfo.class.getName());
	
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
		else if (this.fsUsed.equalsIgnoreCase("local")) {
			this.endpointMgr.loadAvro(new File(this.fnEndpointGroups));
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
	public Tuple exec(Tuple input)   {
		
		// Check if cache files have been opened 
		if (this.initialized==false)
        {
        	try {
				this.init();
			} catch (IOException e) {
				LOG.error("Could not initialize sync structures");
				throw new RuntimeException("pig Eval Init Error");
			} 
        }
		
		if (input == null || input.size() == 0) return null;

		String service;
		String hostname; 
		try {
			service = (String) input.get(0);
			hostname = (String) input.get(1);
		} catch (ExecException e) {
			LOG.error("Could not parse eval input data");
			LOG.error("Bad tuple input:" + input.toString());
			throw new RuntimeException("pig Eval bad input");
		}
		
		
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
