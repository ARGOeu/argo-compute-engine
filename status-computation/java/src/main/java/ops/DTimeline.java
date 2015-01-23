package ops;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.TreeMap;

public class DTimeline {
	
	
	
	private int start_state;	// state to define the beginning of the timeline  
	private TreeMap<Integer,Integer> input_states; // input states with the timestamp converted to slots
	
	
	private int s_period;       // sampling period measured in minutes
	private int s_interval;   	// sampling interval measured in minutes;
	
	int[] samples;				// array of samples based on sampling frequency		
	
	DTimeline()	{
		start_state = -1;
		s_period = 1440;			// 1 day = 24 hours = 24 * 60 minutes = 1440 minutes
		s_interval = 5;				// every 5 minutes;
		samples = new int[1440/5]; //288 samples;
		input_states = new TreeMap<Integer,Integer>();
		Arrays.fill(samples, -1);
	}
	

	public void setSampling(int _period, int _interval) {
		s_period = _period;
		s_interval = _interval;
		samples = new int[this.s_period/this.s_interval];
	}
	
	public void clearSamples(){
		samples = new int[this.s_period/this.s_interval];
	}
	
	
	public void clearTimestamps(){
		start_state = -1;
		input_states.clear();
	}
	
	public void firstState(int _state)
	{
		this.start_state = _state;
	}
	
	
	public int timestampToSlot(String _timestamp) throws ParseException{
	
		SimpleDateFormat w3c_date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		Date parsedDate = w3c_date.parse(_timestamp);
		Calendar cal = Calendar.getInstance();
		cal.setTime(parsedDate);
		
		int total_minutes = (cal.get(Calendar.HOUR_OF_DAY) * 60) + cal.get(Calendar.MINUTE);
		
		return  (total_minutes/this.s_interval) -1; //Normalize for array indexing
	}
	
	public void insertState(String _timestamp, int _state ) throws ParseException{
		int slot = this.timestampToSlot(_timestamp);
		this.input_states.put(slot,_state);
	}
	
	public void applyStates()
	{
		int prev_state = this.start_state;
		int prev_slot = 0;
		for (int item : this.input_states.keySet())
		{
			this.samples[item] = this.input_states.get(item);
			// fill previous states
			for (int i=prev_slot;i<item;i++)
			{
				this.samples[i] = prev_state;
			}
			// set the prev_state and prev_slot
			prev_state = this.input_states.get(item);
			prev_slot = item + 1;
		}
		
	}
	

	
	
	
	
	
	
}
