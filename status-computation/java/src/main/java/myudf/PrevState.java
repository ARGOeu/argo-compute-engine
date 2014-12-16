package myudf;

import java.io.IOException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;


public class PrevState extends EvalFunc<Tuple> {
	
	private static String mongo_host;
	private static int mongo_port;
	private Map<String , String> sites = null;
	
	
	public PrevState(String inp_mongo_host, String inp_mongo_port){
		mongo_host = inp_mongo_host;
		mongo_port = Integer.parseInt(inp_mongo_port);
	}
	
	public PrevState() {
    }
	//private final TupleFactory mTupleFactory = TupleFactory.getInstance();
	
	
	// THIS EVAL FUNCTION ACCEPTS 
	// (vo,vo_fqan,monitoring_box,roc,service_type,hostname,metric{(timestamp,status,summary,message)...})
	
	private void initSites() throws UnknownHostException{
		// Initialize hashmap structure
		sites = new HashMap<String, String>();
		// Connect to the database and get site information
		MongoClient mongoClient = new MongoClient(mongo_host, mongo_port);
		DB db = mongoClient.getDB( "AR" );
		DBCollection coll = db.getCollection("info_sites");
		DBCursor cursor = coll.find();
		DBObject item;
		while(cursor.hasNext()) {
		   item = cursor.next();
		   sites.put((String)item.get("h"),(String)item.get("st"));
		}
		
		
	}
	@Override
	public Tuple exec(Tuple input) throws IOException {
		
		
		if (sites==null)
		{
			initSites();
		}
		
		// parse input
		//String vo = (String)input.get(0);
		//String vo_fqan = (String)input.get(1);
		//String monitoring_box = (String)input.get(2);
		//String roc = (String)input.get(3);
		//String service_type = (String)input.get(4);
		String hostname = (String)input.get(5);
		//String metric = (String)input.get(6);
		
		DefaultDataBag timeline = (DefaultDataBag)input.get(7);
		Iterator<Tuple> myit = timeline.iterator();
		Tuple item;
		
		String prevState = "UNKNOWN";
		
		for (int i=0;i<timeline.size();i++)
		{
			 item = myit.next();
			 item.append(prevState);
			 
			 // Calculate integer of date and time
			 SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
			 Date parsedDate = null;
			 
			 try {
				 parsedDate = dateFormat.parse((String) item.get(0));
			 } catch (ParseException e) {
					e.printStackTrace();
			 }
			 Calendar cal = Calendar.getInstance();
			 cal.setTime(parsedDate);
			
					 
			 
			 	    
			 int date_int = (cal.get(Calendar.YEAR) * 10000 ) + ((cal.get(Calendar.MONTH) + 1)*100) + (cal.get(Calendar.DAY_OF_MONTH));
			 int time_int = (cal.get(Calendar.HOUR_OF_DAY) * 10000) + ((cal.get(Calendar.MINUTE)*100)) + (cal.get(Calendar.SECOND));
			    
			 item.append(date_int);
			 item.append(time_int);
			 
			 prevState = (String) item.get(1);
		}
		input.append((String)sites.get(hostname));
		return input;
		
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        
		Schema.FieldSchema vo = new Schema.FieldSchema("vo", DataType.CHARARRAY);
		Schema.FieldSchema vo_fqan = new Schema.FieldSchema("vo_fqan", DataType.CHARARRAY);
		Schema.FieldSchema monitoring_box = new Schema.FieldSchema("monitoring_box", DataType.CHARARRAY);
		Schema.FieldSchema roc = new Schema.FieldSchema("roc", DataType.CHARARRAY);
		Schema.FieldSchema hostname = new Schema.FieldSchema("hostname", DataType.CHARARRAY);
		Schema.FieldSchema service_type = new Schema.FieldSchema("service_type", DataType.CHARARRAY);
		Schema.FieldSchema metric = new Schema.FieldSchema("metric", DataType.CHARARRAY);
		Schema.FieldSchema site = new Schema.FieldSchema("site", DataType.CHARARRAY);
        
      
        
        
        Schema.FieldSchema timestamp = new Schema.FieldSchema("timestamp", DataType.CHARARRAY);
        Schema.FieldSchema status = new Schema.FieldSchema("status", DataType.CHARARRAY);
        Schema.FieldSchema summary = new Schema.FieldSchema("summary", DataType.CHARARRAY);
        Schema.FieldSchema message = new Schema.FieldSchema("message", DataType.CHARARRAY);
        Schema.FieldSchema prev_state = new Schema.FieldSchema("prev_state", DataType.CHARARRAY);
        Schema.FieldSchema date_int = new Schema.FieldSchema("date_int", DataType.INTEGER);
        Schema.FieldSchema time_int = new Schema.FieldSchema("time_int", DataType.INTEGER);
        
        Schema status_metric = new Schema();
        Schema timeline = new Schema();
        
        status_metric.add(vo);
        status_metric.add(vo_fqan);
        status_metric.add(monitoring_box);
        status_metric.add(roc);
        status_metric.add(hostname);
        status_metric.add(service_type);
        status_metric.add(metric);
        
        timeline.add(timestamp);
        timeline.add(status);
        timeline.add(summary);
        timeline.add(message);
        timeline.add(prev_state);
        timeline.add(date_int);
        timeline.add(time_int);
        
        Schema.FieldSchema timelines = null;
        try {
            timelines = new Schema.FieldSchema("timelines", timeline, DataType.BAG);
        } catch (FrontendException ex) {
            Logger.getLogger(PrevState.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        status_metric.add(timelines);
        status_metric.add(site);
        
        try {
            return new Schema(new Schema.FieldSchema("status_metric", status_metric, DataType.TUPLE));
        } catch (FrontendException ex) {
           
        }
        
        return null;
    }
	
	
	

}