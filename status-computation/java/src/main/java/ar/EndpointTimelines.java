package ar;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import myudf.AddTopology;
import ops.DAggregator;
import ops.DTimeline;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import sync.EndpointGroups;
import sync.MetricProfiles;
import utils.Slot;

public class EndpointTimelines extends EvalFunc<Tuple> {

    public DAggregator endpointAggr;
	
	private TupleFactory tupFactory; 
    private BagFactory bagFactory;
	
    private DTimeline mt;
    
    
	public String fnMetricProfiles;
	public String fnOps;
	
	public String targetDate;
	
	public boolean initialized;
	
	public EndpointTimelines( String fnOps, String targetDate) throws IOException{
		// set first the filenames
		this.fnOps = fnOps;
		
		// set the targetDate var
		this.targetDate = targetDate;
	
		// set the Structures
		this.endpointAggr = new DAggregator();
		
		// set up factories
		this.tupFactory = TupleFactory.getInstance();
		this.bagFactory = BagFactory.getInstance();
		
		// this is not yet initialized because we need files from distributed cache
		this.initialized = false;
	}
	
	public void init() throws IOException
	{
		// Open Files from distributed cache
		this.endpointAggr.opsMgr.openFile(new File("./ops"));
		this.initialized=true;
		System.out.println("Initialized!");
	}
	
	public List<String> getCacheFiles() { 
        List<String> list = new ArrayList<String>(); 
        list.add(this.fnOps.concat("#ops"));
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
		
		///Grab endpoint info
		String service = (String)input.get(0);
		String hostname = (String)input.get(1);
		// Get timeline info
		DefaultDataBag bag =  (DefaultDataBag) input.get(2);
		// Iterate the whole timeline
		Iterator<Tuple> it_bag = bag.iterator();
		
		while (it_bag.hasNext()){
	    	Tuple cur_item = it_bag.next();
	    	//Get timeline item info
	    	String metric = (String) cur_item.get(0);
	    	String ts = (String) cur_item.get(1);
	    	String status = (String) cur_item.get(2);
	    	if (! ( ts.substring(0, ts.indexOf("T")).equals(this.targetDate)) ) {
	    		this.endpointAggr.setStartState(metric, status);
	    		continue;
	    	}
	    	
	    	try {
			
	    		this.endpointAggr.insert(metric, ts, status);
			
	    	} catch (ParseException e) {
				e.printStackTrace();
			}
	    	
		}
		
		this.endpointAggr.finalizeAll();
		this.endpointAggr.aggregate("AND"); // should be supplied on outside file
		
		//Create output Tuple
	    Tuple output = tupFactory.newTuple();
	    DataBag outBag = bagFactory.newDefaultBag();
	    
	    output.append(service);
	    output.append(hostname);
	    
		//Append the timeline
	    for (int i=0;i<this.endpointAggr.aggregation.samples.length;i++)  {
	    	Tuple cur_tupl = tupFactory.newTuple();
	    	cur_tupl.append(i);
			cur_tupl.append(this.endpointAggr.aggregation.samples[i]);
			outBag.add(cur_tupl);
		}
	    
	    output.append(outBag);
	    
	    if (outBag.size()==0) return null;
	   
		return output;
	    
		
		
		
		
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        
		Schema.FieldSchema service = new Schema.FieldSchema("service", DataType.CHARARRAY);
		Schema.FieldSchema hostname = new Schema.FieldSchema("hostname", DataType.CHARARRAY);
		
		Schema.FieldSchema slot = new Schema.FieldSchema("slot", DataType.INTEGER);
		Schema.FieldSchema statusInt = new Schema.FieldSchema("status", DataType.INTEGER);
        
        Schema endpoint = new Schema();
        Schema timeline = new Schema();
       
        endpoint.add(service);
        endpoint.add(hostname);
        
        timeline.add(slot);
        timeline.add(statusInt);

        Schema.FieldSchema tl = null;
        try {
            tl = new Schema.FieldSchema("timeline", timeline, DataType.BAG);
        } catch (FrontendException ex) {
           
        }
        
        endpoint.add(tl);
        
        try {
            return new Schema(new Schema.FieldSchema("endpoint", endpoint, DataType.TUPLE));
        } catch (FrontendException ex) {
           
        }
        
        return null;
    }
	
}
