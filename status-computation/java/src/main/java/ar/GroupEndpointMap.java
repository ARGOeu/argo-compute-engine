package ar;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import ops.ConfigManager;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import sync.AvailabilityProfiles;
import sync.EndpointGroups;
import sync.GroupsOfGroups;
import sync.WeightGroups;

public class GroupEndpointMap extends EvalFunc<Tuple> {
	
	private String fnConfig;
	private String fnAps;
	private String fnWeights;
	private String fnGgroups;
	private String fnEgroups;
	private String targetDate;
	
	private String fsUsed;
	
	public ConfigManager cfgMgr;
	public AvailabilityProfiles apsMgr;
	public WeightGroups weightMgr;
	public GroupsOfGroups ggMgr;
	public EndpointGroups egMgr;
	
	private boolean initialized;
	
	public GroupEndpointMap(String fnConfig, String fnAps, String fnWeights, String fnGgroups, 
							String fnEgroups, String targetDate, String fsUsed) {
		
		this.fsUsed = fsUsed;
		this.fnConfig = fnConfig;
		this.fnAps = fnAps;
		this.fnWeights = fnWeights;
		this.fnGgroups = fnGgroups;
		this.fnEgroups = fnEgroups;
		this.targetDate = targetDate;
		this.fsUsed = fsUsed;
		
		// Initialize Managers
		this.cfgMgr = new ConfigManager();
		this.apsMgr = new AvailabilityProfiles();
		this.weightMgr = new WeightGroups();
		this.ggMgr = new GroupsOfGroups();
		this.egMgr = new EndpointGroups();
		
		this.initialized = false;
	}
	
	
	public void init() throws IOException {
		if (this.fsUsed.equalsIgnoreCase("cache")) {
			this.cfgMgr.loadJson(new File("./cfg"));
			this.apsMgr.loadJson(new File("./aps"));
			this.weightMgr.loadAvro(new File("./weights"));
			this.ggMgr.loadAvro(new File("./ggroups"));
			this.egMgr.loadAvro(new File("./egroups"));
		}
		else if (this.fsUsed.equalsIgnoreCase("local")) {
		
			this.cfgMgr.loadJson(new File(this.fnConfig));
			this.apsMgr.loadJson(new File(this.fnAps));
			this.weightMgr.loadAvro(new File(this.fnWeights));
			this.ggMgr.loadAvro(new File(this.fnGgroups));
			this.egMgr.loadAvro(new File(this.fnEgroups));
		}

		this.initialized = true;

	}
	
	public List<String> getCacheFiles() {
		List<String> list = new ArrayList<String>();
		list.add(this.fnConfig.concat("#config"));
		list.add(this.fnAps.concat("#aps"));
		list.add(this.fnWeights.concat("#weights"));
		list.add(this.fnGgroups.concat("#ggroups"));
		list.add(this.fnEgroups.concat("#egroups"));
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
		
		// Get input fields
		String groupname = (String)input.get(0);
		double av = (Float) (input.get(1));
		double rel = (Float) (input.get(2));
		double upFraction = (Float) (input.get(3));
		double unknownFraction = (Float) (input.get(4));
		double downFraction = (Float) (input.get(5));
		
		// Supplement info for datastore
		
		
		return input;
	}
}
