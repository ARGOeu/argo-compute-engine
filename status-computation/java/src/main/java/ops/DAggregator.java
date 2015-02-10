package ops;

import java.io.File;
import java.io.FileNotFoundException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map.Entry;


public class DAggregator {
	
	public HashMap<String,DTimeline> timelines;
	public DTimeline aggregation;
	
	//public OpsManager opsMgr;
	
	public DAggregator(){
		
		this.timelines = new HashMap<String,DTimeline>();
		this.aggregation = new DTimeline();
		//this.opsMgr = new OpsManager();
	}
	
	public void loadOpsFile(File opsFile) throws FileNotFoundException{
		//this.opsMgr.openFile(opsFile);
	}
	
	
	public void insertSlot(String name, int slot, int statusInt)
	{
		if (timelines.containsKey(name) == false) {
			DTimeline tempTimeline = new DTimeline();
			tempTimeline.samples[slot] = statusInt;
			timelines.put(name, tempTimeline);
		}
		else {
			timelines.get(name).samples[slot] = statusInt;
		}
		
	}
	
	public void insert(String name, String timestamp, int statusInt) throws ParseException {
		// Get the integer value of the specified status string
		
		
		// Check if time-line exists or else create it
		if (timelines.containsKey(name) == false) {
			DTimeline tempTimeline = new DTimeline();
			tempTimeline.insert(timestamp, statusInt);
			timelines.put(name, tempTimeline);
		}
		else {
			timelines.get(name).insert(timestamp, statusInt);
		}
	}
	
	
	
	public void setStartState(String name, int statusInt)
	{
		// Get the integer value of the specified status string
		
		// Check if time-line exists or else create it
		if (timelines.containsKey(name) == false) {
			DTimeline tempTimeline = new DTimeline();
			tempTimeline.setStartState(statusInt);
			timelines.put(name, tempTimeline);
		}
		else {
			timelines.get(name).setStartState(statusInt);
		}
	}

	public void clear()
	{
		this.timelines.clear();
		this.aggregation.clear();
	}
	
	public void finalizeAll(int missingStart)
	{
		for (Entry<String, DTimeline> item : timelines.entrySet())
		{
			item.getValue().finalize(missingStart);
		}
	}
	
	public void aggregate(String opType, OpsManager opsMgr) {
		
		int opTypeInt = opsMgr.getIntOperation(opType);
		
		for (int i=0;i<this.aggregation.samples.length;i++) {
			
			boolean firstItem = true;
			
			for (Entry<String, DTimeline> item : timelines.entrySet()) {
				
				if (firstItem) {
					this.aggregation.samples[i] = item.getValue().samples[i];
					firstItem = false;
				}
				else {
					int a = this.aggregation.samples[i];
					int b = item.getValue().samples[i];
					this.aggregation.samples[i] = opsMgr.opInt(opTypeInt,a, b);
				}		
			}
		}
	}
	
		
}
