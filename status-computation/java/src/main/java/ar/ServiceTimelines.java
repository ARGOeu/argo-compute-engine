package ar;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import ops.DAggregator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;



public class ServiceTimelines extends EvalFunc<Tuple> {

    public DAggregator serviceAggr;
	
	private TupleFactory tupFactory; 
    private BagFactory bagFactory;
    
	private String fnMetricProfiles;
	private String fnOps;
	
	private String targetDate;
	
	private String fsUsed;  // local,hdfs,cache (distrubuted_cache)
	
	private boolean initialized;
	
	public ServiceTimelines( String fnOps, String targetDate, String fsUsed) throws IOException{
		// set first the filenames
		this.fnOps = fnOps;
		
		// set distribute cache flag
		this.fsUsed = fsUsed;
		
		// set the targetDate var
		this.targetDate = targetDate;
	
		// set the Structures
		this.serviceAggr = new DAggregator();
		
		// set up factories
		this.tupFactory = TupleFactory.getInstance();
		this.bagFactory = BagFactory.getInstance();
		
		// this is not yet initialized because we need files from distributed cache
		this.initialized = false;
	}
	
	public void init() throws IOException
	{
		if (this.fsUsed.equalsIgnoreCase("cache")){
			this.serviceAggr.opsMgr.openFile(new File("./ops"));
		}
		
		this.initialized=true;
		
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

		this.serviceAggr.clear();
		
		///Grab endpoint info
		String groupname = (String)input.get(0);
		String service = (String)input.get(1);
		// Get timeline info
		DefaultDataBag bag =  (DefaultDataBag) input.get(2);
		// Iterate the whole timeline
		Iterator<Tuple> it_bag = bag.iterator();
		
		while (it_bag.hasNext()){
	    	Tuple cur_item = it_bag.next();
	    	//Get timeline item info
	    	String hostname = (String) cur_item.get(0);
	    	DefaultDataBag bag2 = (DefaultDataBag) cur_item.get(1);
	    	
	    	Iterator<Tuple> it_bag2 = bag2.iterator();
	    	
	    	int j=0;
	   
	    	while (it_bag2.hasNext()){
	    		
	    		Tuple cur_subitem = it_bag2.next(); 
	    		this.serviceAggr.insertSlot(hostname, j, cur_subitem.getType(0));
	    		j++;
	    		
	    	}
	    	
		}
		
		
		this.serviceAggr.aggregate("OR"); // should be supplied on outside file
		
		//Create output Tuple
	    Tuple output = tupFactory.newTuple();
	    DataBag outBag = bagFactory.newDefaultBag();
	    
	    output.append(groupname);
	    output.append(service);
	    
		//Append the timeline
	    for (int i=0;i<this.serviceAggr.aggregation.samples.length;i++)  {
	    	Tuple cur_tupl = tupFactory.newTuple();
	    	//cur_tupl.append(i);
			cur_tupl.append(this.serviceAggr.aggregation.samples[i]);
			outBag.add(cur_tupl);
		}
	    
	    output.append(outBag);
	    
	    if (outBag.size()==0) return null;
	   
		return output;
	    
		
		
		
		
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        
		Schema.FieldSchema groupname = new Schema.FieldSchema("groupname", DataType.CHARARRAY);
		Schema.FieldSchema service = new Schema.FieldSchema("service", DataType.CHARARRAY);
		
		//Schema.FieldSchema slot = new Schema.FieldSchema("slot", DataType.INTEGER);
		Schema.FieldSchema statusInt = new Schema.FieldSchema("status", DataType.INTEGER);
        
        Schema endpoint = new Schema();
        Schema timeline = new Schema();
       
        endpoint.add(groupname);
        endpoint.add(service);
        
        //timeline.add(slot);
        timeline.add(statusInt);

        Schema.FieldSchema tl = null;
        try {
            tl = new Schema.FieldSchema("timeline", timeline, DataType.BAG);
        } catch (FrontendException ex) {
           
        }
        
        endpoint.add(tl);
        
        try {
            return new Schema(new Schema.FieldSchema("serviceTl", endpoint, DataType.TUPLE));
        } catch (FrontendException ex) {
           
        }
        
        return null;
    }
	
}