package ops;

import java.io.File;
import java.io.FileNotFoundException;
import java.text.ParseException;
import java.util.HashMap;

import utils.CTimeline;

public class DAggregator {
	
	private HashMap<String,DTimeline> timelines;
	private DTimeline aggregation;
	
	private OpsManager opsMgr;
	
	DAggregator(){
		
		this.timelines = new HashMap<String,DTimeline>();
		this.aggregation = new DTimeline();
		this.opsMgr = new OpsManager();
	}
	
	public void loadOpsFile(File opsFile) throws FileNotFoundException{
		this.opsMgr.openFile(opsFile);
	}
	
	public void insert(String name, String timestamp, String status) throws ParseException
	{
		// Get the integer value of the specified status string
		int statusInt = opsMgr.getStatus(status);
		
		// Check if time-line exists or else create it
		if (timelines.containsKey(name) == false) {
			DTimeline tempTimeline = new DTimeline();
			tempTimeline.insert(timestamp, statusInt);
		}
		else {
			timelines.get(name).insert(timestamp, statusInt);
		}
		
	}
	
	
	
}
