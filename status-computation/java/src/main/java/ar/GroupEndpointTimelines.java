package ar;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import ops.ConfigManager;
import ops.DAggregator;
import ops.DTimeline;
import ops.OpsManager;

import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import sync.AvailabilityProfiles;
import sync.GroupsOfGroups;
import sync.Recomputations;

public class GroupEndpointTimelines extends EvalFunc<Tuple> {

	private static final Logger LOG = Logger.getLogger(GroupEndpointTimelines.class.getName());
	public HashMap<String, DAggregator> groupEndpointAggr;

	public OpsManager opsMgr;
	public AvailabilityProfiles apMgr;
    public Recomputations recMgr;
	public GroupsOfGroups ggMgr;
	public ConfigManager cfgMgr;
	
	private TupleFactory tupFactory;
	private BagFactory bagFactory;

	private String fnOps;
	private String fnAps;
	private String fnRec;
	private String fnGG;
	private String fnCfg;
	
	private String targetDate;
	
	private int sPeriod;
	private int sInterval;

	private String fsUsed; // local,hdfs,cache (distrubuted_cache)

	private boolean initialized;

	public GroupEndpointTimelines(String fnOps, String fnAps, String fnRec, String fnGG,  
			String fnCfg, String targetDate, String fsUsed, String sPeriod, String sInterval) {

		// set first the filenames
		this.fnOps = fnOps;
		this.fnAps = fnAps;
		this.fnRec = fnRec;
		this.fnGG = fnGG;
		this.fnCfg = fnCfg;
		// set distribute cache flag
		this.fsUsed = fsUsed;

		this.sPeriod = Integer.parseInt(sPeriod);
		this.sInterval = Integer.parseInt(sInterval);
		
		// set the Structures
		this.groupEndpointAggr = new HashMap<String, DAggregator>();
		this.opsMgr = new OpsManager();
		this.apMgr = new AvailabilityProfiles();
		this.recMgr = new Recomputations();
		this.ggMgr = new GroupsOfGroups();
		this.cfgMgr = new ConfigManager();
		
		this.targetDate = targetDate;
		
		// set up factories
		this.tupFactory = TupleFactory.getInstance();
		this.bagFactory = BagFactory.getInstance();

		// this is not yet initialized because we need files from distributed
		// cache
		this.initialized = false;

	}

	public void init() throws IOException {
		if (this.fsUsed.equalsIgnoreCase("cache")) {
			this.opsMgr.loadJson(new File("./ops"));
			this.apMgr.loadJson(new File("./aps"));
			this.recMgr.loadJson(new File("./rec"));
			this.ggMgr.loadAvro(new File("./ggroups"));
			this.cfgMgr.loadJson(new File("./cfg"));
		}
		else if (this.fsUsed.equalsIgnoreCase("local")) {
			this.apMgr.loadJson(new File(this.fnAps));
			this.opsMgr.loadJson(new File(this.fnOps));
			this.recMgr.loadJson(new File(this.fnRec));
			this.ggMgr.loadAvro(new File(this.fnGG));
			this.cfgMgr.loadJson(new File(this.fnCfg));
		
		}

		this.initialized = true;

	}

	public List<String> getCacheFiles() {
		List<String> list = new ArrayList<String>();
		list.add(this.fnOps.concat("#ops"));
		list.add(this.fnAps.concat("#aps"));
		list.add(this.fnRec.concat("#rec"));
		list.add(this.fnGG.concat("#ggroups"));
		list.add(this.fnCfg.concat("#cfg"));
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
				throw new RuntimeException("pig Eval Init Error");
			}
		}

		if (input == null || input.size() == 0)
			return null;

		this.groupEndpointAggr.clear();

		String aprofile = this.apMgr.getAvProfiles().get(0); // One Availability Profile

		String groupname;
		DefaultDataBag bag;
		
		try {
			groupname = (String)input.get(0);
			bag = (DefaultDataBag) input.get(1);
		} catch (ClassCastException e) {
			LOG.error("Failed to cast input to approriate type");
			LOG.error("Bad tuple input:" + input.toString());
			throw new RuntimeException("pig Eval bad input");
		} catch (IndexOutOfBoundsException e) {
			LOG.error("Malformed tuple schema");
			LOG.error("Bad tuple input:" + input.toString());
			throw new RuntimeException("pig Eval bad input");
		} catch (ExecException e) {
			LOG.error("Execution error");
			throw new RuntimeException("pig Eval bad input");
		}

		// Iterate the whole timeline
		Iterator<Tuple> it_bag = bag.iterator();

		while (it_bag.hasNext()) {
			Tuple cur_item = it_bag.next();

			String service;
			DefaultDataBag bag2;

			try {
				// Get timeline item info
				service = (String)cur_item.get(0);
				bag2 = (DefaultDataBag) cur_item.get(1);	
			} catch (ClassCastException e) {
				LOG.error("Failed to cast input to approriate type");
				LOG.error("Bad tuple input:" + cur_item.toString());
				throw new RuntimeException("pig Eval bad input");
			} catch (IndexOutOfBoundsException e) {
				LOG.error("Malformed tuple schema");
				LOG.error("Bad tuple input:" + cur_item.toString());
				throw new RuntimeException("pig Eval bad input");
			} catch (ExecException e) {
	    		LOG.error ("Execution error");
	    		throw new RuntimeException("bad bag item input");
			}
			// Get the availability group
			String group = apMgr.getGroupByService(aprofile, service);

			// if group doesn't exist yet create it
			if (this.groupEndpointAggr.containsKey(group) == false) {
				this.groupEndpointAggr.put(group, new DAggregator(this.sPeriod,this.sInterval));
			}

			// Group will be present now
			Iterator<Tuple> it_bag2 = bag2.iterator();
			int j = 0;
			while (it_bag2.hasNext()) {

				Tuple cur_subitem = it_bag2.next();
				try {
					this.groupEndpointAggr.get(group).insertSlot(service, j,
							Integer.parseInt(cur_subitem.get(0).toString()));
				} catch (NumberFormatException e) {
		    		LOG.error ("Failed to cast input to approriate type");
		    		LOG.error ("Bad subitem:" + cur_subitem.toString());
		    		throw new RuntimeException("bad bag item input");
				} catch (ExecException e) {
		    		LOG.error ("Execution error");
		    		throw new RuntimeException("bad bag item input");
				}

				j++;

			}

		}

		// Aggregate each group
		for (String group : this.groupEndpointAggr.keySet()) {
			// Get group Operation
			
			String gop = this.apMgr.getProfileGroupOp(aprofile, group);
			
			this.groupEndpointAggr.get(group).aggregate(gop, this.opsMgr);

		}

		// Aggregate all sites
		DAggregator totalSite = new DAggregator();

		// Aggregate each group
		for (String group : this.groupEndpointAggr.keySet()) {
			DTimeline curTimeline = this.groupEndpointAggr.get(group).aggregation;
			for (int i = 0; i < curTimeline.samples.length; i++) {
				totalSite.insertSlot(group, i, curTimeline.samples[i]);

			}

		}

		// Final site aggregate
		// Get appropriate operation from availability profile
		totalSite.aggregate(this.apMgr.getTotalOp(aprofile), this.opsMgr);

		// Check for Recalculations
		String supergroupType = this.cfgMgr.ggroup;
		String groupType = this.cfgMgr.egroup;
		String supergroup = this.ggMgr.getGroup(supergroupType, groupname);
		
		try {
			
			if (this.recMgr.shouldRecompute(supergroup, groupname,this.targetDate)){
				
				String startRec = this.recMgr.getStart(supergroup);
				String endRec = this.recMgr.getEnd(supergroup);
				
				totalSite.aggregation.fill(this.opsMgr.getDefaultUnknownInt(), startRec, endRec, this.targetDate);
			}
			
		} catch (ParseException e) {
			e.printStackTrace();
		}
			
		
		
		// Create output Tuple
		Tuple output = tupFactory.newTuple();
		DataBag outBag = bagFactory.newDefaultBag();

		output.append(groupname);

		// Append the timeline
		for (int i = 0; i < totalSite.aggregation.samples.length; i++) {
			Tuple cur_tupl = tupFactory.newTuple();
			// cur_tupl.append(i);
			cur_tupl.append(totalSite.aggregation.samples[i]);
			outBag.add(cur_tupl);
		}

		output.append(outBag);

		return output;

	}

	@Override
	public Schema outputSchema(Schema input) {

		Schema.FieldSchema groupname = new Schema.FieldSchema("groupname",
				DataType.CHARARRAY);

		// Schema.FieldSchema slot = new Schema.FieldSchema("slot",
		// DataType.INTEGER);
		Schema.FieldSchema statusInt = new Schema.FieldSchema("status",
				DataType.INTEGER);

		Schema endpoint = new Schema();
		Schema timeline = new Schema();

		endpoint.add(groupname);

		// timeline.add(slot);
		timeline.add(statusInt);

		Schema.FieldSchema tl = null;
		try {
			tl = new Schema.FieldSchema("timeline", timeline, DataType.BAG);
		} catch (FrontendException ex) {

		}

		endpoint.add(tl);

		try {
			return new Schema(new Schema.FieldSchema("serviceTl", endpoint,
					DataType.TUPLE));
		} catch (FrontendException ex) {

		}

		return null;
	}

}
