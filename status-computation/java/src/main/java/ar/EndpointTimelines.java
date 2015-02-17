package ar;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import ops.DAggregator;
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

import sync.AvailabilityProfiles;
import sync.Downtimes;


public class EndpointTimelines extends EvalFunc<Tuple> {

    public DAggregator endpointAggr;
	public OpsManager opsMgr;
    public Downtimes downMgr;
    public AvailabilityProfiles avMgr;
	
	private TupleFactory tupFactory; 
    private BagFactory bagFactory;
    
	private String fnAvProfiles;
	private String fnOps;
	private String fnDown;
	private String targetDate;
	
	// Sampling config
	private int sPeriod;
	private int sInterval;
	
	private String fsUsed;  // local,hdfs,cache (distrubuted_cache)
	
	private boolean initialized;
	
	public EndpointTimelines( String fnOps, String fnDown, String fnAvProfiles, String targetDate, String fsUsed, String sPeriod, String sInterval) throws IOException{
		// set first the filenames
		this.fnOps = fnOps;
		this.fnDown = fnDown;
		this.fnAvProfiles = fnAvProfiles;
		// set distribute cache flag
		this.fsUsed = fsUsed;
		
		// set the targetDate var
		this.targetDate = targetDate;
	
		// sampling
		this.sPeriod = Integer.parseInt(sPeriod);
		this.sInterval = Integer.parseInt(sInterval);
		
		// set the Structures
		this.endpointAggr = new DAggregator(this.sPeriod,this.sInterval); // Create Aggregator according to sampling freq.
		this.opsMgr = new OpsManager();
		this.downMgr = new Downtimes();
		this.avMgr = new AvailabilityProfiles();
		
		// set up factories
		this.tupFactory = TupleFactory.getInstance();
		this.bagFactory = BagFactory.getInstance();
		
		// this is not yet initialized because we need files from distributed cache
		this.initialized = false;
	}
	
	public void init() throws IOException
	{
		if (this.fsUsed.equalsIgnoreCase("cache")){
			this.opsMgr.loadJson(new File("./ops"));
			this.downMgr.loadAvro(new File("./down"));
			this.avMgr.loadJson(new File("./aps"));
		}
		else if (this.fsUsed.equalsIgnoreCase("local")) {
			this.opsMgr.loadJson(new File(this.fnOps));
			this.downMgr.loadAvro(new File(this.fnDown));
			this.avMgr.loadJson(new File(this.fnAvProfiles));
		}
		
		this.initialized=true;
		
	}
	
	public List<String> getCacheFiles() { 
        List<String> list = new ArrayList<String>(); 
        list.add(this.fnOps.concat("#ops"));
        list.add(this.fnDown.concat("#down"));
        list.add(this.fnAvProfiles.concat("#aps"));
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

		this.endpointAggr.clear();
		
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
	    		this.endpointAggr.setStartState(metric, this.opsMgr.getIntStatus(status));
	    		continue;
	    	}
	    	try {
			
	    		this.endpointAggr.insert(metric, ts, this.opsMgr.getIntStatus(status));
			
	    	} catch (ParseException e) {
				e.printStackTrace();
			}
	    	
		}
		
		this.endpointAggr.finalizeAll(this.opsMgr.getDefaultMissingInt());
		
		String aprofile = this.avMgr.getAvProfiles().get(0);
		
		this.endpointAggr.aggregate(this.avMgr.getMetricOp(aprofile),this.opsMgr); // should be supplied on outside file
		
		//Apply Downtimes if hostname is on downtime list
		ArrayList<String> downPeriod = this.downMgr.getPeriod(hostname, service);
		
		if (downPeriod!=null){
			//We have downtime declared
			try {
				this.endpointAggr.aggregation.fill(this.opsMgr.getDefaultDownInt(), downPeriod.get(0), downPeriod.get(1), this.targetDate);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		//Create output Tuple
	    Tuple output = tupFactory.newTuple();
	    DataBag outBag = bagFactory.newDefaultBag();
	    
	    output.append(service);
	    output.append(hostname);
	    
		//Append the timeline
	    for (int i=0;i<this.endpointAggr.aggregation.samples.length;i++)  {
	    	Tuple cur_tupl = tupFactory.newTuple();
	    	//cur_tupl.append(i);
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
        
        try {
            return new Schema(new Schema.FieldSchema("endpoint", endpoint, DataType.TUPLE));
        } catch (FrontendException ex) {
           
        }
        
        return null;
    }
	
}
