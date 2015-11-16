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
import org.apache.pig.impl.logicalLayer.FrontendException;
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
		
		output.append(cfgMgr.id);       // 0 - report id
		output.append(dateInt); 			// 1 - date
		output.append(service); 			// 2 - name
		output.append(egroupName); 			// 3 - supergroup 

		// Add the a/r info
		output.append(av); 					// 4 - availability 
		output.append(rel); 				// 5 - reliability 
		output.append(upFraction); 			// 6 - up fraction
		output.append(downFraction); 		// 7 - down fraction
		output.append(unknownFraction); 	// 8 - unknown fraction 

		// NOTE: tags will be handled properly in a later PR

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
		Schema.FieldSchema sReport = new Schema.FieldSchema("report",
				DataType.CHARARRAY);
		Schema.FieldSchema sDateInt = new Schema.FieldSchema("date",
				DataType.INTEGER);
		Schema.FieldSchema sName = new Schema.FieldSchema("name",
				DataType.CHARARRAY);
		Schema.FieldSchema sSuperGroup = new Schema.FieldSchema("supergroup",
				DataType.CHARARRAY);
		
		// Define the ar results fields
		Schema.FieldSchema sAvailability = new Schema.FieldSchema("availability",
				DataType.DOUBLE);
		Schema.FieldSchema sReliability = new Schema.FieldSchema("reliability",
				DataType.DOUBLE);
		Schema.FieldSchema sUp = new Schema.FieldSchema("up", DataType.DOUBLE);
		Schema.FieldSchema sDown = new Schema.FieldSchema("down", DataType.DOUBLE);
		Schema.FieldSchema sUnknown = new Schema.FieldSchema("unknown",
				DataType.DOUBLE);
		
		// NOTE: tags and tag schema will be handled properly in a later PR

		// Add fields to schema
		serviceData.add(sReport);
		serviceData.add(sDateInt); 
		serviceData.add(sName); 
		serviceData.add(sSuperGroup); 

		serviceData.add(sAvailability);
		serviceData.add(sReliability);
		serviceData.add(sUp);
		serviceData.add(sUnknown);
		serviceData.add(sDown);


		try {
			return new Schema(new Schema.FieldSchema("service_data", serviceData, DataType.TUPLE));
		} catch (FrontendException ex) {
			LOG.error(ex);

		}
		
		return serviceData;

	}

}
