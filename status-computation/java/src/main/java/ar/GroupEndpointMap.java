package ar;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import ops.ConfigManager;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

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
	
	private TupleFactory tupFactory;
	
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
		
		this.tupFactory = TupleFactory.getInstance();
		
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
		
		Tuple output = tupFactory.newTuple();
		
		// Get input fields
		String egroupName = (String)input.get(0);
		double av = (Float) (input.get(1));
		double rel = (Float) (input.get(2));
		double upFraction = (Float) (input.get(3));
		double unknownFraction = (Float) (input.get(4));
		double downFraction = (Float) (input.get(5));
		
		// Supplement info for datastore
		int dateInt = Integer.parseInt(this.targetDate.replace("-", ""));
		String egroupType = this.cfgMgr.egroup;
		String ggroupType = this.cfgMgr.ggroup;
		String weightType = this.cfgMgr.weight;
		int weightVal = this.weightMgr.getWeight(weightType, egroupName);
		
		String ggroupName = this.ggMgr.getGroup(ggroupType, egroupName);
		String avProfile = this.apsMgr.getAvProfiles().get(0);
		String metricProfile = this.apsMgr.getProfileMetricProfile(avProfile);
		
		// Add the previous info before adding the tags
		output.append(dateInt); 		// 0 
		output.append(avProfile);		// 1
		output.append(metricProfile); 	// 2
		output.append(egroupName); 		// 3
		output.append(ggroupName);		// 4
		output.append(weightVal);		// 5
		
		// Add the a/r info 
		output.append(av);				// 6
		output.append(rel);				// 7
		output.append(upFraction);		// 8
		output.append(unknownFraction);	// 9 
		output.append(downFraction);	// 10
		
		// Get egroup config tags
		for ( Entry<String,String> item : this.cfgMgr.egroupTags.entrySet()){
			output.append(item.getValue());
		}
		
		HashMap<String,String> ggTags = this.ggMgr.getGroupTags(ggroupType, egroupName); 
		
		// Get ggroup config tags
		for ( Entry<String,String> item : this.cfgMgr.ggroupTags.entrySet()){
			String curValue = ggTags.get(item.getKey());
			output.append(curValue);
		}
		
		
		return input;
	}
	
	@Override
	public Schema outputSchema(Schema input) {
		
		
		Schema groupEndpointData = new Schema();
		// Define first fields
		Schema.FieldSchema sDateInt  = new Schema.FieldSchema(this.cfgMgr.getMapped("ar","date"), DataType.INTEGER);
		Schema.FieldSchema sAvProfile  = new Schema.FieldSchema(this.cfgMgr.getMapped("ar","av_profile"), DataType.CHARARRAY);
		Schema.FieldSchema sMetricProfile   = new Schema.FieldSchema(this.cfgMgr.getMapped("ar","metric_profile"),  DataType.CHARARRAY);
		Schema.FieldSchema sGroup   = new Schema.FieldSchema(this.cfgMgr.getMapped("ar","group"),  DataType.CHARARRAY);
		Schema.FieldSchema sSuperGroup   = new Schema.FieldSchema(this.cfgMgr.getMapped("ar","supergroup"),  DataType.CHARARRAY);
		Schema.FieldSchema sWeight   = new Schema.FieldSchema(this.cfgMgr.getMapped("ar","weight"),  DataType.CHARARRAY);
        // Define the ar results fields
		Schema.FieldSchema sAv  = new Schema.FieldSchema(this.cfgMgr.getMapped("ar","availability"), DataType.DOUBLE);
		Schema.FieldSchema sRel  = new Schema.FieldSchema(this.cfgMgr.getMapped("ar","reliability"), DataType.DOUBLE);
		Schema.FieldSchema sUp   = new Schema.FieldSchema(this.cfgMgr.getMapped("ar","up_f"),  DataType.DOUBLE);
		Schema.FieldSchema sUnknown   = new Schema.FieldSchema(this.cfgMgr.getMapped("ar","unkown_f"),  DataType.DOUBLE);
		Schema.FieldSchema sDown   = new Schema.FieldSchema(this.cfgMgr.getMapped("ar","down_f"),  DataType.DOUBLE);
		
		// Create a field schema list for egroup tags
		ArrayList<Schema.FieldSchema> egroupFields = new ArrayList<Schema.FieldSchema>();
		// Get egroup config tags
		for ( Entry<String,String> item : this.cfgMgr.egroupTags.entrySet()){
			egroupFields.add(new Schema.FieldSchema(this.cfgMgr.getMapped("egroup", item.getKey()),DataType.CHARARRAY));
		}
		
		ArrayList<Schema.FieldSchema> ggroupFields = new ArrayList<Schema.FieldSchema>();
		// Get ggroup config tags
		for ( Entry<String,String> item : this.cfgMgr.ggroupTags.entrySet()){
			ggroupFields.add(new Schema.FieldSchema(this.cfgMgr.getMapped("ggroup", item.getKey()),DataType.CHARARRAY));
		}
		
		// Add fields to schema
		groupEndpointData.add(sDateInt);
		groupEndpointData.add(sAvProfile);
		groupEndpointData.add(sMetricProfile);
		groupEndpointData.add(sGroup);
		groupEndpointData.add(sSuperGroup);
		groupEndpointData.add(sWeight);
		
		groupEndpointData.add(sAv);
		groupEndpointData.add(sRel);
		groupEndpointData.add(sUp);
		groupEndpointData.add(sUnknown);
		groupEndpointData.add(sDown);
		
		for (Schema.FieldSchema item : egroupFields)
		{
			groupEndpointData.add(item);
		}
		
		for (Schema.FieldSchema item : ggroupFields)
		{
			groupEndpointData.add(item);
		}
		
        return groupEndpointData;
         
	}
	
}
