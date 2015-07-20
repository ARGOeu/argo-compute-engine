package ar;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import ops.ConfigManager;

import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import sync.AvailabilityProfiles;
import sync.EndpointGroups;
import sync.GroupsOfGroups;
import sync.WeightGroups;

public class ServiceMap extends EvalFunc<Tuple> {

	private static final Logger LOG = Logger.getLogger(ServiceMap.class.getName());
	private String fnConfig;
	private String fnAps;
	private String fnGgroups;
	private String fnEgroups;
	private String targetDate;

	private String fsUsed;

	private String localCfg;

	public ConfigManager cfgMgr;
	public AvailabilityProfiles apsMgr;
	public WeightGroups weightMgr;
	public GroupsOfGroups ggMgr;
	public EndpointGroups egMgr;
	public ConfigManager localCfgMgr; // called in front-end for establishing
										// the correct output schema

	private TupleFactory tupFactory;

	private boolean initialized;
	private boolean frontInit;

	public ServiceMap(String fnConfig, String fnAps, String fnGgroups, String fnEgroups, String targetDate,
			String fsUsed, String localCfg) {

		this.fsUsed = fsUsed;
		this.fnConfig = fnConfig;
		this.fnAps = fnAps;
		this.fnGgroups = fnGgroups;
		this.fnEgroups = fnEgroups;
		this.targetDate = targetDate;
		this.fsUsed = fsUsed;

		this.localCfg = localCfg;

		// Initialize Managers
		this.cfgMgr = new ConfigManager();
		this.apsMgr = new AvailabilityProfiles();
		this.weightMgr = new WeightGroups();
		this.ggMgr = new GroupsOfGroups();
		this.egMgr = new EndpointGroups();
		this.localCfgMgr = new ConfigManager();

		this.tupFactory = TupleFactory.getInstance();

		this.initialized = false;
		this.frontInit = false;
	}

	public void init() throws IOException {
		if (this.fsUsed.equalsIgnoreCase("cache")) {
			this.cfgMgr.loadJson(new File("./cfg"));
			this.apsMgr.loadJson(new File("./aps"));
			this.ggMgr.loadAvro(new File("./ggroups"));
			this.egMgr.loadAvro(new File("./egroups"));
		} else if (this.fsUsed.equalsIgnoreCase("local")) {

			this.cfgMgr.loadJson(new File(this.fnConfig));
			this.apsMgr.loadJson(new File(this.fnAps));
			this.ggMgr.loadAvro(new File(this.fnGgroups));
			this.egMgr.loadAvro(new File(this.fnEgroups));
		}

		this.initialized = true;

	}

	public void initFrontend() throws IOException {
		this.localCfgMgr.loadJson(new File(this.localCfg));
		this.frontInit = true;
	}

	public List<String> getCacheFiles() {
		List<String> list = new ArrayList<String>();
		list.add(this.fnConfig.concat("#cfg"));
		list.add(this.fnAps.concat("#aps"));
		list.add(this.fnGgroups.concat("#ggroups"));
		list.add(this.fnEgroups.concat("#egroups"));
		return list;
	}

	@Override
	public Tuple exec(Tuple input) {

		// Check if cache files have been opened
		if (this.initialized == false) {
			try {
				this.init(); // If not open them
			} catch (IOException e) {
				LOG.error("Could not initialize sync structures");
				LOG.error(e);
				throw new IllegalStateException();
			}
		}

		if (input == null || input.size() == 0)
			return null;

		Tuple output = tupFactory.newTuple();

		String service;
		String egroupName;
		double av;
		double rel;
		double upFraction;
		double unknownFraction;
		double downFraction;

		try {
			// Get input fields
			service = (String) input.get(0);
			egroupName = (String) input.get(1);
			av = (Double) input.get(2);
			rel = (Double) input.get(3);
			upFraction = (Double) input.get(4);
			unknownFraction = (Double) input.get(5);
			downFraction = (Double) input.get(6);
		} catch (ClassCastException e) {
			LOG.error("Failed to cast input to approriate type");
			LOG.error("Bad tuple input:" + input.toString());
			LOG.error(e);
			throw new IllegalArgumentException();
		} catch (IndexOutOfBoundsException e) {
			LOG.error("Malformed tuple schema");
			LOG.error("Bad tuple input:" + input.toString());
			LOG.error(e);
			throw new IllegalArgumentException();
		} catch (ExecException e) {
			LOG.error("Execution error");
			LOG.error(e);
			throw new IllegalArgumentException();
		}

		// Supplement info for datastore
		int dateInt = Integer.parseInt(this.targetDate.replace("-", ""));
		String ggroupType = this.cfgMgr.ggroup;

		String ggroupName = this.ggMgr.getGroup(ggroupType, egroupName);
		String avProfile = this.apsMgr.getAvProfiles().get(0);
		String avNamespace = this.apsMgr.getProfileNamespace(avProfile);
		String metricProfile = this.apsMgr.getProfileMetricProfile(avProfile);
		String fullAvProfile = avNamespace + "-" + avProfile;
		// Add the previous info before adding the tags
		output.append(dateInt); // 0
		output.append(fullAvProfile); // 1
		output.append(metricProfile); // 2
		output.append(egroupName); // 3
		output.append(ggroupName); // 4
		output.append(service); // 5

		// Add the a/r info
		output.append(av); // 6
		output.append(rel); // 7
		output.append(upFraction); // 8
		output.append(unknownFraction); // 9
		output.append(downFraction); // 10

		// Get egroup config tags
		for (Entry<String, String> item : this.cfgMgr.egroupTags.entrySet()) {
			output.append(item.getValue());
		}

		HashMap<String, String> ggTags = this.ggMgr.getGroupTags(ggroupType, egroupName);

		// Get ggroup config tags
		for (Entry<String, String> item : this.cfgMgr.ggroupTags.entrySet()) {
			String curValue = ggTags.get(item.getKey());
			output.append(curValue);
		}

		return output;
	}

	@Override
	public Schema outputSchema(Schema input) {

		if (this.frontInit == false) {
			try {
				this.initFrontend();
			} catch (FileNotFoundException e) {
				LOG.error(e);
			} catch (IOException e) {
				LOG.error(e);
			}
		}

		Schema serviceData = new Schema();

		// Define first fields
		Schema.FieldSchema sDateInt = new Schema.FieldSchema(this.localCfgMgr.getMapped("ar", "date"),
				DataType.INTEGER);
		Schema.FieldSchema sAvProfile = new Schema.FieldSchema(this.localCfgMgr.getMapped("ar", "av_profile"),
				DataType.CHARARRAY);
		Schema.FieldSchema sMetricProfile = new Schema.FieldSchema(this.localCfgMgr.getMapped("ar", "metric_profile"),
				DataType.CHARARRAY);
		Schema.FieldSchema sGroup = new Schema.FieldSchema(this.localCfgMgr.getMapped("ar", "group"),
				DataType.CHARARRAY);
		Schema.FieldSchema sSuperGroup = new Schema.FieldSchema(this.localCfgMgr.getMapped("ar", "supergroup"),
				DataType.CHARARRAY);
		Schema.FieldSchema sService = new Schema.FieldSchema(this.localCfgMgr.getMapped("ar", "service"),
				DataType.CHARARRAY);
		// Define the ar results fields
		Schema.FieldSchema sAv = new Schema.FieldSchema(this.localCfgMgr.getMapped("ar", "availability"),
				DataType.DOUBLE);
		Schema.FieldSchema sRel = new Schema.FieldSchema(this.localCfgMgr.getMapped("ar", "reliability"),
				DataType.DOUBLE);
		Schema.FieldSchema sUp = new Schema.FieldSchema(this.localCfgMgr.getMapped("ar", "up_f"), DataType.DOUBLE);
		Schema.FieldSchema sUnknown = new Schema.FieldSchema(this.localCfgMgr.getMapped("ar", "unknown_f"),
				DataType.DOUBLE);
		Schema.FieldSchema sDown = new Schema.FieldSchema(this.localCfgMgr.getMapped("ar", "down_f"), DataType.DOUBLE);

		// Create a field schema list for egroup tags
		ArrayList<Schema.FieldSchema> egroupFields = new ArrayList<Schema.FieldSchema>();
		// Get egroup config tags
		for (Entry<String, String> item : this.localCfgMgr.egroupTags.entrySet()) {
			egroupFields.add(
					new Schema.FieldSchema(this.localCfgMgr.getMapped("egroups", item.getKey()), DataType.CHARARRAY));
		}

		ArrayList<Schema.FieldSchema> ggroupFields = new ArrayList<Schema.FieldSchema>();
		// Get ggroup config tags
		for (Entry<String, String> item : this.localCfgMgr.ggroupTags.entrySet()) {
			ggroupFields.add(
					new Schema.FieldSchema(this.localCfgMgr.getMapped("ggroups", item.getKey()), DataType.CHARARRAY));
		}

		// Add fields to schema
		serviceData.add(sDateInt);
		serviceData.add(sAvProfile);
		serviceData.add(sMetricProfile);
		serviceData.add(sGroup);
		serviceData.add(sSuperGroup);
		serviceData.add(sService);

		serviceData.add(sAv);
		serviceData.add(sRel);
		serviceData.add(sUp);
		serviceData.add(sUnknown);
		serviceData.add(sDown);

		for (Schema.FieldSchema item : egroupFields) {
			serviceData.add(item);
		}

		for (Schema.FieldSchema item : ggroupFields) {
			serviceData.add(item);
		}

		return serviceData;

	}

}
