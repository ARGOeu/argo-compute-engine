package ar;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import ops.DAggregator;
import ops.DTimeline;
import ops.OpsManager;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class MetricTimelines extends EvalFunc<Tuple> {

	private String fnOps; 
	private String targetDate;
	
	private String fsUsed; //local,hdfs,cache (distrubuted_cache)
	
	public DTimeline dtl;
	public OpsManager opsMgr;
	
	private TupleFactory tupFactory; 
    private BagFactory bagFactory;
	
	private boolean initialized;
	
	public MetricTimelines ( String fnOps, String targetDate, String fsUsed ) throws IOException{
		// set first the filenames
		this.fnOps = fnOps;
		
		this.fsUsed = fsUsed;
		
		// set the targetDate var
		this.targetDate = targetDate;
	
		// set the Structures
		this.dtl = new DTimeline();
		this.opsMgr = new OpsManager();
		// set up factories
		this.tupFactory = TupleFactory.getInstance();
		this.bagFactory = BagFactory.getInstance();
		// this is not yet initialized because we need files from distributed cache
		this.initialized = false;
	}
	
	public void init() throws IOException
	{
		// Open Files from distributed cache
		if (this.fsUsed.equalsIgnoreCase("cache")){
			
			this.opsMgr.openFile(new File("./ops"));
		}
		this.initialized=true;
		System.out.println("Initialized!");
	}
	
	public List<String> getCacheFiles() { 
        List<String> list = new ArrayList<String>(); 
        list.add(this.fnOps.concat("#ops"));
        return list; 
	} 
	
	
	public Tuple exec(Tuple input) throws IOException {
		
		
		
		// Check if cache files have been opened 
		if (this.initialized==false)
        {
        	
			this.init(); // If not open them
			this.initialized = true;
        }
		// Clear timeline
		this.dtl.clear();
		
		if (input == null || input.size() == 0) return null;
		
		///Grab endpoint info
		String service = (String)input.get(0);
		String hostname = (String)input.get(1);
		String metric = (String)input.get(2);
		// Get timeline info
		DefaultDataBag bag =  (DefaultDataBag) input.get(3);
		// Iterate the whole timeline
		Iterator<Tuple> itBag = bag.iterator();
		
		while (itBag.hasNext()){
	    	Tuple curItem = itBag.next();
	    	//Get timeline item info
	    	
	    	String ts = (String) curItem.get(0);
	    	String status = (String) curItem.get(1);
	    	if (! ( ts.substring(0, ts.indexOf("T")).equals(this.targetDate)) ) {
	    		this.dtl.setStartState(this.opsMgr.getIntStatus(status));
	    		continue;
	    	}
	    	
	    	try {
			
	    		this.dtl.insert(ts, opsMgr.getIntStatus(status));
			
	    	} catch (ParseException e) {
				e.printStackTrace();
			}
	    	
		}
		
		
	
	    
	    
		
		this.dtl.finalize();
		
		//Create output Tuple
	    Tuple output = tupFactory.newTuple();
	    DataBag outBag = bagFactory.newDefaultBag();
	    
	    output.append(service);
	    output.append(hostname);
	    output.append(metric);
	    
	    
	    
		//Append the timeline
	    for (int i=0;i<this.dtl.samples.length;i++)  {
	    	Tuple cur_tupl = tupFactory.newTuple();
	    	
	    	//cur_tupl.append(i);
			cur_tupl.append(this.dtl.samples[i]);
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
		Schema.FieldSchema metric = new Schema.FieldSchema("metric", DataType.CHARARRAY);
		
		//Schema.FieldSchema slot = new Schema.FieldSchema("slot", DataType.INTEGER);
		Schema.FieldSchema statusInt = new Schema.FieldSchema("status", DataType.CHARARRAY);
        
        Schema metricTl = new Schema();
        Schema timeline = new Schema();
       
        metricTl.add(service);
        metricTl.add(hostname);
        metricTl.add(metric);
        
        //timeline.add(slot);
        timeline.add(statusInt);

        Schema.FieldSchema tl = null;
        try {
            tl = new Schema.FieldSchema("timeline", timeline, DataType.BAG);
        } catch (FrontendException ex) {
           
        }
        
        metricTl.add(tl);
        
        try {
            return new Schema(new Schema.FieldSchema("endpoint", metricTl, DataType.TUPLE));
        } catch (FrontendException ex) {
           
        }
        
        return null;
    }

	
	
}
