package utils;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map.Entry;
import java.util.TreeMap;

public class CTimeline {

	
	
	private TreeMap<Integer,Slot> tline;
	
	public CTimeline(){
		tline = new TreeMap<Integer,Slot>();
	}
	
	public CTimeline(int _time_int, int _date_int, String _timestamp, String _status, String _prev_status){
		tline = new TreeMap<Integer,Slot>();
		tline.put(_time_int, new Slot(_time_int,_date_int,_timestamp,_status,_prev_status));
	}
	
	public int insert(int _time_int, int _date_int, String _timestamp, String _status, String _prev_status){
		if (tline.containsKey(_time_int)){	
			tline.remove(_time_int);
		}
		tline.put(_time_int, new Slot(_time_int,_date_int,_timestamp,_status,_prev_status));
		
		return 0;
	}
	
	public int remove(int _time_int){
		if (tline.containsKey(_time_int))
		{
			tline.remove(_time_int);
			return 0;
		}
		return -1;
	}
	
	public TreeMap<Integer,Slot> getTimeline(){
		
		TreeMap<Integer,Slot> deliverable = new TreeMap<Integer,Slot>();
		deliverable.putAll(tline);
		return deliverable;
		
	}
	
	
	public boolean hasAtLeast(int _time){
		return (!(tline.floorEntry(_time)==null));		
	}
	
	public String getStatus(int _time){
		// Approximate if not specified exactly
		if	(hasAtLeast(_time)){
			return  tline.floorEntry(_time).getValue().status;
		}
		else{
			return tline.ceilingEntry(_time).getValue().prev_status;
		}
	}
	
	public String getTimeStamp(int _time){
		// Approximate if not specified exactly
		if	(hasAtLeast(_time)){
			return  tline.floorEntry(_time).getValue().timestamp;
		}
		else{
			return tline.ceilingEntry(_time).getValue().timestamp;
		}
	}
	
	public int getDate(int _time){
		// Approximate if not specified exactly
		if	(hasAtLeast(_time)){
			return  tline.floorEntry(_time).getValue().date_int;
		}
		else{
			return tline.ceilingEntry(_time).getValue().date_int;
		}
	}
	
	public int getTime(int _time){
		// Approximate if not specified exactly
		if	(hasAtLeast(_time)){
			return  tline.floorEntry(_time).getValue().time_int;
		}
		else{
			return tline.ceilingEntry(_time).getValue().time_int;
		}
	}
	
	public String int_to_timestamp(int _time, int _date)
	{
		String timestamp = null;
		// take care time part
		int hours = _time/ 10000;
		int mins  =(_time - (hours*10000))/100;
		int secs = (_time - (hours*10000) - (mins*100));
		// take care date part
		int year = _date/10000;
		int month = (_date - year*10000)/100;
		int day = (_date - (year*10000) - (month*100));
		
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(0);
		cal.set(year, month, day, hours, mins, secs);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		
		timestamp = sdf.format(cal.getTime());
		
		return timestamp;
	}
	
	
	
	public String getPrevStatus(int _time){
	
		if	(hasAtLeast(_time)){
			return  tline.floorEntry(_time).getValue().prev_status;
		}
		else{
			return tline.ceilingEntry(_time).getValue().prev_status;
		}
	}
	
	
	public boolean checkPrevStatus(){
		String prevState = "";
		for (Entry<Integer,Slot> item: tline.entrySet()){
			if (item.equals(tline.firstEntry())){
				prevState = item.getValue().status;
				continue;
			}
			
			if (prevState.equals(item.getValue().prev_status) == false)
			{
				return false; // error in status change chain
			}
			
			prevState = item.getValue().status;
		
		}
		
		return true; //all good
	}
	
	public int setPrevStatus(){
		String prevState = "";
		for (Entry<Integer,Slot> item : tline.entrySet()){
			if (item.equals(tline.firstEntry())) // first item
			{
				prevState = item.getValue().status;
				continue;
			}
			item.getValue().prev_status = prevState;
			prevState = item.getValue().status;
		}
		
		return 0;
		
	}
	
	public int optimize(){
		// if timeline contains only two elements (beginning and end) no need for optimization
		if (tline.size() <= 2) return -1;
		// if not use a new treemap bucket
		TreeMap<Integer,Slot> new_tline = new TreeMap<Integer,Slot>();
		
		for (Entry<Integer,Slot> item: tline.entrySet()){
			if (item.equals(tline.firstEntry()) || item.equals(tline.lastEntry()) ){
				// First and last element should always be present 
				new_tline.put(item.getKey(), item.getValue());
				continue;
			}
			
			// if previous state is not equal to current state then we have a state
			// transition and we should capture it
			if (item.getValue().status.equals(item.getValue().prev_status) == false){
				new_tline.put(item.getKey(), item.getValue());
			}
			
		}
		
		// clear tline
		tline.clear();
		tline.putAll(new_tline);
		
		return 0;
	}
	
	
	
}
