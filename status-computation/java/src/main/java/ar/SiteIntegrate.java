package ar;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import sync.AvailabilityProfiles;
import sync.GroupsOfGroups;

public class SiteIntegrate extends EvalFunc<Tuple> {

	public AvailabilityProfiles apMgr;
	public GroupsOfGroups ggMgr;
	
	private String fnAps;
	public String fnGroups;
	
	private String targetDate;
	
	private String fsUsed;
	
	private boolean initialized = false;
	
	public SiteIntegrate(String fnAps, String targetDate, String fsUsed) {
		this.fnAps = fnAps;
		this.targetDate = targetDate;
		this.fsUsed = fsUsed;
		
		this.apMgr = new AvailabilityProfiles();
		this.ggMgr = new GroupsOfGroups();
	}
	
	public void init() throws IOException
	{
		if (this.fsUsed.equalsIgnoreCase("cache")){
			this.apMgr.loadProfileJson(new File("./aps"));
			this.ggMgr.loadAvro(new File("./groups"));
		}
		
		this.initialized=true;
		
	}
	
	public List<String> getCacheFiles() { 
        List<String> list = new ArrayList<String>(); 
        list.add(this.fnAps.concat("#aps"));
        list.add(this.fnGroups.concat("#groups"));
        return list; 
	} 
	
	@Override
	public Tuple exec(Tuple input) throws IOException {
		
	
		String hostname = (String) input.get(0);
		
		
		
		
		return input;
	
	
	
	}
		
		
}
