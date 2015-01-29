package ops;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.TreeMap;

public class DTimeline {
	

	private int startState;	// state to define the beginning of the timeline  
	private TreeMap<Integer,Integer> inputStates; // input states with the timestamp converted to slots
	
	private int sPeriod;       // sampling period measured in minutes
	private int sInterval;   	// sampling interval measured in minutes;
	
	public int[] samples;				// array of samples based on sampling frequency		
	
	DTimeline()	{
		this.startState = -1;
		this.sPeriod = 1440;			// 1 day = 24 hours = 24 * 60 minutes = 1440 minutes
		this.sInterval = 5;				// every 5 minutes;
		this.samples = new int[1440/5]; //288 samples;
		this.inputStates = new TreeMap<Integer,Integer>();
		Arrays.fill(samples, -1);
	}
	
	public void setSampling(int period, int interval) {
		this.sPeriod = period;
		this.sInterval = interval;
		samples = new int[this.sPeriod/this.sInterval];
	}
	
	public void clearSamples(){
		samples = new int[this.sPeriod/this.sInterval];
		Arrays.fill(samples, -1);
	}
	
	public void clearTimestamps(){
		startState = -1;
		inputStates.clear();
	}
	
	public void setStartState(int state)
	{
		this.startState = state;
	}
	
	public int getStartState()
	{
		return this.startState;
	}
	
	public int tsInt(String timestamp) throws ParseException{
	
		SimpleDateFormat w3c_date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		Date parsedDate = w3c_date.parse(timestamp);
		Calendar cal = Calendar.getInstance();
		cal.setTime(parsedDate);
		
		int total_seconds = (cal.get(Calendar.HOUR_OF_DAY) * 3600) + (cal.get(Calendar.MINUTE) * 60) + cal.get(Calendar.SECOND);
		
		double total_minutes = Math.round(total_seconds/60.0);
		double result = Math.round(total_minutes/this.sInterval);
		return (int)result;
	}
	
	public void insert(String timestamp, int state ) throws ParseException{
		int slot = this.tsInt(timestamp);
		this.inputStates.put(slot,state);
	}
	
	public void finalize()
	{
		int prev_state = this.startState;
		int prev_slot = 0;
		for (int item : this.inputStates.keySet())
		{
			
			this.samples[item] = this.inputStates.get(item);
			// fill previous states
			for (int i=prev_slot;i<item-1;i++)
			{
				this.samples[i] = prev_state;
			}
			// set the prev_state and prev_slot
			prev_state = this.inputStates.get(item);
			prev_slot = item-1;
		}
		
		// Fill the rest of the array with the last state
		for (int i=prev_slot;i<this.samples.length;i++)
		{
			this.samples[i] = prev_state;
		}
		
	}
	

	
	
	
	
	
	
}
