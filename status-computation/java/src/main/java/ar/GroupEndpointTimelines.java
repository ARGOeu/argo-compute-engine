package ar;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import ops.DAggregator;
import ops.DTimeline;
import ops.OpsManager;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import sync.AvailabilityProfiles;

public class GroupEndpointTimelines extends EvalFunc<Tuple> {

	public HashMap<String, DAggregator> groupEndpointAggr;

	public OpsManager opsMgr = new OpsManager();
	public AvailabilityProfiles apMgr = new AvailabilityProfiles();

	private TupleFactory tupFactory;
	private BagFactory bagFactory;

	private String fnOps;
	private String fnAps;
	
	private int sPeriod;
	private int sInterval;

	private String fsUsed; // local,hdfs,cache (distrubuted_cache)

	private boolean initialized;

	public GroupEndpointTimelines(String fnOps, String fnAps, String fsUsed, String sPeriod, String sInterval) {

		// set first the filenames
		this.fnOps = fnOps;
		this.fnAps = fnAps;
		// set distribute cache flag
		this.fsUsed = fsUsed;

		this.sPeriod = Integer.parseInt(sPeriod);
		this.sInterval = Integer.parseInt(sInterval);
		
		// set the Structures
		this.groupEndpointAggr = new HashMap<String, DAggregator>();
		this.opsMgr = new OpsManager();
		this.apMgr = new AvailabilityProfiles();
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
		}

		this.initialized = true;

	}

	public List<String> getCacheFiles() {
		List<String> list = new ArrayList<String>();
		list.add(this.fnOps.concat("#ops"));
		list.add(this.fnAps.concat("#aps"));
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

		this.groupEndpointAggr.clear();

		String aprofile = this.apMgr.getAvProfiles().get(0); // One Availability
																// Profile

		String sitename = (String) input.get(0);
		DefaultDataBag bag = (DefaultDataBag) input.get(1);

		// Iterate the whole timeline
		Iterator<Tuple> it_bag = bag.iterator();

		while (it_bag.hasNext()) {
			Tuple cur_item = it_bag.next();
			// Get timeline item info
			String service = (String) cur_item.get(0);
			DefaultDataBag bag2 = (DefaultDataBag) cur_item.get(1);

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
				this.groupEndpointAggr.get(group).insertSlot(service, j,
						Integer.parseInt(cur_subitem.get(0).toString()));
				j++;

			}

		}

		// Aggregate each group
		for (String group : this.groupEndpointAggr.keySet()) {
			// Get group Operation
			System.out.println(group);
			System.out.println(aprofile);
			String gop = this.apMgr.getProfileGroupOp(aprofile, group);
			System.out.println(gop);
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

		// Create output Tuple
		Tuple output = tupFactory.newTuple();
		DataBag outBag = bagFactory.newDefaultBag();

		output.append(sitename);

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
