package utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;


public class Aggregator {
	
	// timelines to be aggregated
	public Map<String,CTimeline> tlines;
	// aggregation result
	public TreeMap<Integer,Slot> aggr_tline;
	
	public Aggregator(){
		aggr_tline = new TreeMap<Integer,Slot>();
		tlines = new HashMap<String,CTimeline>();
	}
	
	public int insert(String _name, int _time_int, int _date_int, String _timestamp, String _status, String _prev_status ){
		// check if metric timeline exists
		if (tlines.containsKey(_name) == false) {
			tlines.put(_name, new CTimeline(_time_int,_date_int,_timestamp,_status,_prev_status));
		}
		else {
			tlines.get(_name).insert(_time_int, _date_int, _timestamp, _status, _prev_status);
		}
		return 0;		
	}
	
	public int optimize(){
		for ( Entry<String,CTimeline> item : tlines.entrySet()){
			item.getValue().optimize();
		}
		return 0;
	}
	
	public int project(){
		for ( Entry<String,CTimeline> item : tlines.entrySet()){
			for (Integer timeslice : item.getValue().getTimeline().keySet()){
				// if time-slice isn't contained put it
				if (aggr_tline.containsKey(timeslice) == false) {
					aggr_tline.put(timeslice, new Slot(timeslice,0,null,null,null));
				}
			}
		} 
		return 0;
	}
	
	public int aggregateAND()
	{	
		
		for ( Entry<Integer, Slot> item : aggr_tline.entrySet()){
			int cur_time_int = item.getKey();
			int cur_date_int = 0;
			String cur_timestamp="";
			String cur_status="";
			String cur_prev_status="";
			int first_pick = 1;
			for (Entry<String,CTimeline> citem: tlines.entrySet()) {
				
				if (first_pick == 1) {
					cur_date_int = citem.getValue().getDate(cur_time_int);
					cur_timestamp = citem.getValue().getTimeStamp(cur_time_int);
					cur_status = citem.getValue().getStatus(cur_time_int);
					cur_prev_status = citem.getValue().getPrevStatus(cur_time_int);
					first_pick = 0; //turn of first pick flag
					continue;
				}
				
				cur_status = get_AND_status(cur_status, citem.getValue().getStatus(cur_time_int));
				
				
			}
			
			item.setValue(new Slot(cur_time_int,cur_date_int,cur_timestamp,cur_status,cur_prev_status));
			
			
		} 
		return 0;
	}
	
	public int aggregateOR()
	{	
		
		for ( Entry<Integer, Slot> item : aggr_tline.entrySet()){
			int cur_time_int = item.getKey();
			int cur_date_int = 0;
			String cur_timestamp="";
			String cur_status="";
			String cur_prev_status="";
			int first_pick = 1;
			for (Entry<String,CTimeline> citem: tlines.entrySet()) {
				
				if (first_pick == 1) {
					cur_date_int = citem.getValue().getDate(cur_time_int);
					cur_timestamp = citem.getValue().getTimeStamp(cur_time_int);
					cur_status = citem.getValue().getStatus(cur_time_int);
					cur_prev_status = citem.getValue().getPrevStatus(cur_time_int);
					first_pick = 0; //turn of first pick flag
					continue;
				}
				
				cur_status = get_OR_status(cur_status, citem.getValue().getStatus(cur_time_int));
				
				
			}
			
			item.setValue(new Slot(cur_time_int,cur_date_int,cur_timestamp,cur_status,cur_prev_status));
			
			
		} 
		return 0;
	}
	
	public int get_AND_weights(String status) {
		
		if (status.equals("OK")) return 1;
		if (status.equals("WARNING")) return 2;
		if (status.equals("UKNOWN")) return 3;
		if (status.equals("MISSING")) return 4;
		if (status.equals("CRITICAL")) return 5;
		
		return -1;
	}
	
	public int get_OR_WEIGHTS(String status) {
		
		if (status.equals("MISSING")) return 1;
		if (status.equals("UKNOWN")) return 2;
		if (status.equals("CRITICAL")) return 3;
		if (status.equals("WARNING")) return 4;
		if (status.equals("OK")) return 5;
		
		return -1;
	}
	
	
	
		
	public int aggrPrevState(){
		String prevState = "";
		for (Entry<Integer,Slot> item : aggr_tline.entrySet()){
			if (item.equals(aggr_tline.firstEntry())) // first item
			{
				prevState = item.getValue().status;
				continue;
			}
			item.getValue().prev_status = prevState;
			prevState = item.getValue().status;
		}
		
		return 0;
	}
	
	public boolean checkPrevStatus(){
		for (Entry<String,CTimeline> item : tlines.entrySet()){
			if (item.getValue().checkPrevStatus() == false)
			{
				return false;
			}
		}
		
		return true; //all good
	}
	
	public String get_AND_status(String status_a, String status_b) {
		//Assign weights
		
		int weight_a = get_AND_weights(status_a);
		int weight_b = get_AND_weights(status_b);
		int weight_res;
		
		if (weight_a> weight_b) weight_res = weight_a;
		else weight_res = weight_b;
		
		//Return Status string from number
		if (weight_res == 1) return "OK";
		if (weight_res == 2) return "WARNING";
		if (weight_res == 3) return "UNKNOWN";
		if (weight_res == 4) return "MISSING";
		if (weight_res == 5) return "CRITICAL";
		
		return null;
	}
	
	public String get_OR_status(String status_a, String status_b) {
		//Assign weights
		
		int weight_a = get_AND_weights(status_a);
		int weight_b = get_AND_weights(status_b);
		int weight_res;
		
		if (weight_a> weight_b) weight_res = weight_a;
		else weight_res = weight_b;
		
		//Return Status string from number
		if (weight_res == 1) return "MISSING";
		if (weight_res == 2) return "UNKNOWN";
		if (weight_res == 3) return "CRITICAL";
		if (weight_res == 4) return "WARNING";
		if (weight_res == 5) return "OK";
		
		return null;
	}
	
	
	
	
	
	
	
	
}
