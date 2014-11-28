package myudf;

import java.io.IOException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;


public class ServiceHostStatus extends EvalFunc<Tuple> {
	
	private static String mongo_host;
	private static int mongo_port;
	
	private class Slot{
		
		String timestamp ="";
		String status ="";
		String prevState = "";
		int date_int = 0;
		int time_int = 0;
		
	}
	
	
	private Map<String , HashMap<String,ArrayList<String>>> poem_details = null;
	
	
	public ServiceHostStatus(String inp_mongo_host, String inp_mongo_port){
		mongo_host = inp_mongo_host;
		mongo_port = Integer.parseInt(inp_mongo_port);
	}
	
	public ServiceHostStatus() {
    }
	//private final TupleFactory mTupleFactory = TupleFactory.getInstance();
	
	// THIS EVAL FUNCTION ACCEPTS 
	// (site,service,hostname{(metric,timestamp,status,prevstate,date_int,time_int)
	
	private void initPoemDetails() throws UnknownHostException{
		// Initialize hashmap structure
		poem_details = new HashMap<String, HashMap<String,ArrayList<String>>> ();
		// Connect to the database and get site information
		MongoClient mongoClient = new MongoClient(mongo_host, mongo_port);
		DB db = mongoClient.getDB( "AR" );
		DBCollection coll = db.getCollection("poem_details");
		DBCursor cursor = coll.find();
		DBObject item;
		
		while(cursor.hasNext()) {
		   item = cursor.next();
			   
		   //unmarshal profile
		   String profile = (String)item.get("p");
		   String service = (String)item.get("s");
		   String metric = (String)item.get("m");
			   
		   //Check if profile exists or else create it 
		   if (poem_details.get(profile) == null) {
			   poem_details.put(profile, new HashMap<String,ArrayList<String>>());
			   poem_details.get(profile).put(service, new ArrayList<String>());
			   poem_details.get(profile).get(service).add(metric);
				   
		   } else if (poem_details.get(profile).get(service) == null) {
			   
			   poem_details.get(profile).put(service,new ArrayList<String>());
			   poem_details.get(profile).get(service).add(metric);
			   
		   } else if (poem_details.get(profile).get(service).contains(metric) == false){
			   poem_details.get(profile).get(service).add(metric);
		   }	   
		}
		
	}
	
	@Override
	public Tuple exec(Tuple input) throws IOException {
		
		
		if (poem_details==null)
		{
			initPoemDetails();
		}
		
		// parse input
		//String site = (String)input.get(0);
		String service = (String)input.get(1);
		//String hostname = (String)input.get(2);
		//String timeline = (String)input.get(3);
			
		
		// here we grab each profile 
		
		Map<String , HashMap<String,ArrayList<Slot>>> profile_metrics = null;
		profile_metrics = new HashMap<String, HashMap<String,ArrayList<Slot>>>();
		// init the structure for all available poems
		// where _it means iterator like poem_it = poem iterator
		Iterator<String> poem_it = poem_details.keySet().iterator();
		while (poem_it.hasNext()!=false)
		{
			String poem_name = poem_it.next();
			if (poem_details.get(poem_name).get(service) != null) { 
				profile_metrics.put(poem_it.next(), new HashMap<String,ArrayList<Slot>>());
			}
		}
		
		ArrayList<Integer> timesplits = new ArrayList<Integer>();
		
		DefaultDataBag timeline = (DefaultDataBag)input.get(3);
		Iterator<Tuple> metric_it = timeline.iterator(); //all the metrics
		while (metric_it.hasNext() != false)
		{
			// Check if each profile contains the combination of service+metric
			Tuple item = metric_it.next();
			String metric_name= (String) item.get(0);
			
			Slot temp = new Slot();
			temp.timestamp = (String)item.get(1);
			temp.status = (String)item.get(2);
			temp.prevState = (String)item.get(3);
			temp.date_int = (Integer)item.get(4);
			temp.time_int = (Integer)item.get(5);
			
			poem_it = profile_metrics.keySet().iterator();
			while (poem_it.hasNext()!=false)
			{
				String poem_name = poem_it.next();
				// service does not belong to this profile
				if (poem_details.get(poem_name).get(service) == null) continue;
				// metric does not belong to this service of this profile 
				if (poem_details.get(poem_name).get(service).contains(metric_name) == false) continue;
				// metric belongs here but not initiated yet
				if (profile_metrics.get(poem_name).get(metric_name) == null) {
					
					profile_metrics.get(poem_name).put(metric_name, new ArrayList<Slot>());
					profile_metrics.get(poem_name).get(metric_name).add(temp);
					//Introduce timesplit;
					//if (timesplits.contains(temp.time_int) == false){
					//	timesplits.add(temp.time_int);
					//}
					
				}
				// metric already initiated
				profile_metrics.get(poem_name).get(metric_name).add(temp);
				
				//if (timesplits.contains(temp.time_int) == false){
				//	timesplits.add(temp.time_int);
				//}
				
			}			
			
		}// End of iteration
		
		// Clean up and optimize metric timelines
		
		// Begin Iteration
		poem_it = profile_metrics.keySet().iterator();
		Iterator<String> prof_metric_it;
		while (poem_it.hasNext()!=false){
			// Get iterator of metric names
			String poem_name = poem_it.next();
			prof_metric_it = profile_metrics.get(poem_name).keySet().iterator();
			
			ArrayList<Slot> cur_list;
			while (prof_metric_it.hasNext()!=false)
			{
				//Get the list
				cur_list = profile_metrics.get(poem_name).get(prof_metric_it.next());
				
				//Set an iterator
				Iterator<Slot> list_it = cur_list.iterator();
				//Skip first item
				list_it.next();
				while (list_it.hasNext()){
					if (cur_list.size()>2)
					{
						Slot temp = list_it.next();
						if (temp.prevState == temp.status){
							list_it.remove();
						}
					}
				}	
			}
		}// end of metric iteration
		
		// Reiterate 
	
		return input;
		
	}
	
	
	
	@Override
    public Schema outputSchema(Schema input) {
        
		Schema.FieldSchema site = new Schema.FieldSchema("site", DataType.CHARARRAY);
		Schema.FieldSchema service = new Schema.FieldSchema("service", DataType.CHARARRAY);
		Schema.FieldSchema hostname = new Schema.FieldSchema("hostname", DataType.CHARARRAY); 
        
        Schema.FieldSchema profile = new Schema.FieldSchema("profile", DataType.CHARARRAY);
        Schema.FieldSchema timestamp = new Schema.FieldSchema("timestamp", DataType.CHARARRAY);
        Schema.FieldSchema status = new Schema.FieldSchema("", DataType.CHARARRAY);
        Schema.FieldSchema prev_state = new Schema.FieldSchema("message", DataType.CHARARRAY);
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