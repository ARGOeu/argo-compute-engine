package myudf;



import java.io.IOException;
import java.net.UnknownHostException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import utils.MetricProfileManager;
import utils.Aggregator;
import utils.Slot;



public class ServiceHostStatus extends EvalFunc<Tuple> {
	
	private static String mongo_host;
	private static int mongo_port;
	private static int target_date;
	
	private static MetricProfileManager prof_mgr;
	
	TupleFactory tupFactory = TupleFactory.getInstance();
    BagFactory bagFactory = BagFactory.getInstance();
	
	
	public ServiceHostStatus(String inp_mongo_host, String inp_mongo_port, String inp_target_date) throws UnknownHostException{
		mongo_host = inp_mongo_host;
		mongo_port = Integer.parseInt(inp_mongo_port);
		target_date = Integer.parseInt(inp_target_date);
		// Initialize Metric Profile Manager
		prof_mgr = new MetricProfileManager();
		prof_mgr.loadFromMongo(mongo_host,mongo_port);
	}
	
	// THIS EVAL FUNCTION ACCEPTS 
	// (site,service,hostname{(metric,timestamp,status,prevstate,date_int,time_int)
	
	
	@Override
	public Tuple exec(Tuple input) throws IOException {
		
		

	    MetricProfileManager mymgr = new MetricProfileManager();
		mymgr.loadFromMongo("localhost",27017);
		
		String site = (String) input.get(0);
		String service = (String) input.get(1);
		String host = (String) input.get(2);
		DefaultDataBag bag =  (DefaultDataBag) input.get(3);
		Iterator<Tuple> it_bag = bag.iterator();
		

		
		Map<String,Aggregator> all = new HashMap<String,Aggregator>();
		
		// Set the default profiles
	    for (String profile : mymgr.getProfiles())
	    {
	    	all.put(profile, new Aggregator());
	    }
		
	  
	    while (it_bag.hasNext()){
	    	Tuple cur_item = it_bag.next();
	    	if ((Integer)cur_item.get(4) != target_date) continue;
	    	
	    	for (String prof: all.keySet()){
	    		//exists for this profile?
	    		
	    		if (mymgr.checkProfileServiceMetric(prof, service, (String)cur_item.get(0)) == true)
	    		{
	    			   		
	    			all.get(prof).insert(prof, (Integer)cur_item.get(5), (Integer)cur_item.get(4),(String) cur_item.get(1), (String)cur_item.get(2), (String)cur_item.get(3));
	    		}
	    	}
	    }
	    
	    //Create output Tuple
	    Tuple output = tupFactory.newTuple();
	    DataBag outBag = bagFactory.newDefaultBag();
	    
	    output.append(site);
	    output.append(service);
	    output.append(host);
	    
	    
	    
	    
	    for (String prof: all.keySet())
		   {
			  
			
			   
			   all.get(prof).optimize();
			   all.get(prof).project();
			   all.get(prof).aggregateAND();
			   all.get(prof).aggrPrevState();
			   
			  
			   
			   for (Entry<Integer, Slot> item: all.get(prof).aggr_tline.entrySet())
			   {
				   Slot item_value = item.getValue();
				   Tuple cur_tupl = tupFactory.newTuple();
				   cur_tupl.append(prof); 
				   cur_tupl.append(item.getValue().timestamp);
				   cur_tupl.append(item.getValue().status);
				   cur_tupl.append(item.getValue().prev_status);
				   cur_tupl.append(item_value.date_int);
				   cur_tupl.append(item_value.time_int);
				   outBag.add(cur_tupl);
			   }
			
		 }
	    
	    output.append(outBag);
		
	    if (outBag.size()==0) return null;
	    
		return output;
		
	}
	
	
	
	@Override
    public Schema outputSchema(Schema input) {
        
		Schema.FieldSchema site = new Schema.FieldSchema("site", DataType.CHARARRAY);
		Schema.FieldSchema service = new Schema.FieldSchema("service", DataType.CHARARRAY);
		Schema.FieldSchema hostname = new Schema.FieldSchema("hostname", DataType.CHARARRAY); 
        
        Schema.FieldSchema profile = new Schema.FieldSchema("profile", DataType.CHARARRAY);
        Schema.FieldSchema timestamp = new Schema.FieldSchema("timestamp", DataType.CHARARRAY);
        Schema.FieldSchema status = new Schema.FieldSchema("status", DataType.CHARARRAY);
        Schema.FieldSchema prev_state = new Schema.FieldSchema("prev_state", DataType.CHARARRAY);
        Schema.FieldSchema date_int = new Schema.FieldSchema("date_int", DataType.INTEGER);
        Schema.FieldSchema time_int = new Schema.FieldSchema("time_int", DataType.INTEGER);
        
        Schema service_host = new Schema();
        Schema timeline = new Schema();
        
        service_host.add(site);
        service_host.add(service);
        service_host.add(hostname);
        
        timeline.add(profile);
        timeline.add(timestamp);
        timeline.add(status);
        timeline.add(prev_state);
        timeline.add(date_int);
        timeline.add(time_int);
        
        Schema.FieldSchema timelines = null;
        try {
            timelines = new Schema.FieldSchema("timelines", timeline, DataType.BAG);
        } catch (FrontendException ex) {
            Logger.getLogger(AddTopology.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        service_host.add(timelines);
        
        
        try {
            return new Schema(new Schema.FieldSchema("service_host", service_host, DataType.TUPLE));
        } catch (FrontendException ex) {
           
        }
        
        return null;
    }
	
	
	

}