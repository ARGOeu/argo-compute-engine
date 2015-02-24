package status;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import ops.ConfigManager;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import sync.AvailabilityProfiles;
import sync.EndpointGroups;
import sync.GroupsOfGroups;


public class PrepStatusDetails extends EvalFunc<Tuple> {
	
	private String fnGgrp;
	private String fnEgrp;
	private String fnAps;
	private String fnCfg;
	
	private String targetDate;
	
	private String fsUsed;

	public AvailabilityProfiles apsMgr;
	public EndpointGroups egrpMgr;
	public GroupsOfGroups ggrpMgr;
	public ConfigManager cfgMgr;
	
	private TupleFactory tupFactory;
	
	private boolean initialized;
	
	public PrepStatusDetails (String fnGgrp, String fnEgrp, String fnAps, String fnCfg, String targetDate, String fsUsed){
		
		this.targetDate = targetDate;
		
		this.fnGgrp = fnGgrp;
		this.fnEgrp= fnEgrp;
		this.fnAps = fnAps;
		this.fnCfg = fnCfg;
		this.fsUsed = fsUsed;
		
		this.apsMgr = new AvailabilityProfiles();
		this.egrpMgr = new EndpointGroups();
		this.ggrpMgr = new GroupsOfGroups();
		this.cfgMgr = new ConfigManager();
	
		this.tupFactory= TupleFactory.getInstance();
		
		this.initialized = false;
		
		
		
	}
	
	public void init() throws IOException
	{
		if (this.fsUsed.equalsIgnoreCase("cache")){
			this.egrpMgr.loadAvro(new File("./egrp"));
			this.apsMgr.loadJson(new File("./aps"));
			this.cfgMgr.loadJson(new File("./cfg"));
			this.ggrpMgr.loadAvro(new File("./ggrp"));
		}
		else if (this.fsUsed.equalsIgnoreCase("local")) {
			this.egrpMgr.loadAvro(new File(this.fnEgrp));
			this.apsMgr.loadJson(new File(this.fnAps));
			this.cfgMgr.loadJson(new File(this.fnCfg));
			this.ggrpMgr.loadAvro(new File(this.fnGgrp));
		}
		
		this.initialized=true;
	}
	
	public List<String> getCacheFiles() { 
        List<String> list = new ArrayList<String>(); 
        list.add(this.fnGgrp.concat("#ggrp"));
        list.add(this.fnEgrp.concat("#egrp"));
        list.add(this.fnAps.concat("#aps"));
        list.add(this.fnCfg.concat("#cfg"));
        
        
        return list; 
	} 
	
	
	@Override
	public Tuple exec(Tuple input) throws IOException {
		
		// Check if cache files have been opened
		if (this.initialized == false) {
			this.init(); // If not open them
		}

		if (input == null || input.size() == 0)
			return null;
		
		// parse input
		String monitoring_host = (String)input.get(0);
		String service = (String)input.get(1);
		String hostname = (String)input.get(2);
		String metric = (String)input.get(3);
		
		DefaultDataBag timeline = (DefaultDataBag)input.get(4);
		Iterator<Tuple> myit = timeline.iterator();
		Tuple item;
		
		Tuple output = this.tupFactory.newTuple();
		DefaultDataBag outBag = new DefaultDataBag();
		
		String prevState = "MISSING";
		String prevTs = this.targetDate.concat("T00:00:00Z");
		
		
		for (int i=0;i<timeline.size();i++)
		{
			 item = myit.next();
			 
			 String curTsDate = (String)item.get(0);
			 String curTsDay = curTsDate.substring(0, curTsDate.indexOf("T"));
			 
			 
			 item.append(prevState);
			 item.append(prevTs);
			 
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
			 prevTs = (String) item.get(0);
			 
			 if (curTsDay.equals(this.targetDate))
			 {
				 outBag.add(item);
			 }
		}
			 
		// Find egroup and gggroup
		
		String egroupType = this.cfgMgr.egroup;
		String egroupName = this.egrpMgr.getGroup(egroupType, hostname, service);
		String ggroupType = this.cfgMgr.ggroup;
		String ggroupName = this.ggrpMgr.getGroup(ggroupType, egroupName);

		//add stuff to the output
		
		output.append(monitoring_host);
		output.append(service);
		output.append(hostname);
		output.append(metric);
		output.append(ggroupName);
		output.append(egroupName);
		output.append(this.cfgMgr.agroup);
		output.append(this.cfgMgr.agroup);
		output.append(outBag);
		
		return output;
		
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        
		Schema.FieldSchema monitoring_box = new Schema.FieldSchema("monitoring_host", DataType.CHARARRAY);
		Schema.FieldSchema hostname = new Schema.FieldSchema("hostname", DataType.CHARARRAY);
		Schema.FieldSchema service_type = new Schema.FieldSchema("service", DataType.CHARARRAY);
		Schema.FieldSchema metric = new Schema.FieldSchema("metric", DataType.CHARARRAY);
		Schema.FieldSchema ggroup = new Schema.FieldSchema("ggroup",DataType.CHARARRAY);
		Schema.FieldSchema egroup = new Schema.FieldSchema("egroup",DataType.CHARARRAY);
		Schema.FieldSchema altgroup = new Schema.FieldSchema("altg",DataType.CHARARRAY);
		Schema.FieldSchema altgroupf = new Schema.FieldSchema("altgf",DataType.CHARARRAY);
		
		
		//Schema.FieldSchema egroup = new Schema.FieldSchema("site",DataType.CHARARRAY);
		//Schema.FieldSchema ggroup = new Schema.FieldSchema("roc",DataType.CHARARRAY);
		
		//Schema.FieldSchema altgroup = new Schema.FieldSchema("vo",DataType.CHARARRAY);
		//Schema.FieldSchema altgroupf = new Schema.FieldSchema("vo_f",DataType.CHARARRAY);
      
        
        
        Schema.FieldSchema timestamp = new Schema.FieldSchema("timestamp", DataType.CHARARRAY);
        Schema.FieldSchema status = new Schema.FieldSchema("status", DataType.CHARARRAY);
        Schema.FieldSchema summary = new Schema.FieldSchema("summary", DataType.CHARARRAY);
        Schema.FieldSchema message = new Schema.FieldSchema("message", DataType.CHARARRAY);
        Schema.FieldSchema prev_state = new Schema.FieldSchema("prev_state", DataType.CHARARRAY);
        Schema.FieldSchema prev_ts = new Schema.FieldSchema("prev_ts",DataType.CHARARRAY);
        Schema.FieldSchema date_int = new Schema.FieldSchema("date_int", DataType.INTEGER);
        Schema.FieldSchema time_int = new Schema.FieldSchema("time_int", DataType.INTEGER);
        
        Schema status_metric = new Schema();
        Schema timeline = new Schema();
        
     
        status_metric.add(monitoring_box);
        status_metric.add(service_type);
        status_metric.add(hostname);
        status_metric.add(metric);
        status_metric.add(egroup);
        status_metric.add(ggroup);
        
        status_metric.add(altgroup);
        status_metric.add(altgroupf);
        
        timeline.add(timestamp);
        timeline.add(status);
        timeline.add(summary);
        timeline.add(message);
        timeline.add(prev_state);
        timeline.add(prev_ts);
        timeline.add(date_int);
        timeline.add(time_int);
        
        Schema.FieldSchema timelines = null;
        try {
            timelines = new Schema.FieldSchema("timelines", timeline, DataType.BAG);
        } catch (FrontendException ex) {
            Logger.getLogger(PrepStatusDetails.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        status_metric.add(timelines);
       
        
        try {
            return new Schema(new Schema.FieldSchema("status_metric", status_metric, DataType.TUPLE));
        } catch (FrontendException ex) {
           
        }
        
        return null;
    }
	
	
	

}