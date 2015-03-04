package ar;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import ops.DAggregator;
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

public class ServiceTimelines extends EvalFunc<Tuple> {

	public DAggregator serviceAggr;
	public OpsManager opsMgr;
	public AvailabilityProfiles apsMgr;

	private TupleFactory tupFactory;
	private BagFactory bagFactory;

	private String fnOps;
	private String fnAps;
	
	private int sPeriod;
	private int sInterval;

	private String fsUsed; // local,hdfs,cache (distrubuted_cache)

	private boolean initialized;

	public ServiceTimelines(String fnAps, String fnOps, String fsUsed, String sPeriod, String sInterval)
			throws IOException {
		// set first the filenames
		this.fnOps = fnOps;
		this.fnAps = fnAps;
		// set distribute cache flag
		this.fsUsed = fsUsed;
		
		// set frequency config
		this.sPeriod = Integer.parseInt(sPeriod);
		this.sInterval = Integer.parseInt(sInterval);

		// set the Structures
		this.serviceAggr = new DAggregator(this.sPeriod,this.sInterval);
		this.opsMgr = new OpsManager();
		this.apsMgr = new AvailabilityProfiles();
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
			this.apsMgr.loadJson(new File("./aps"));

		}
		else if (this.fsUsed.equalsIgnoreCase("local")) {
			this.apsMgr.loadJson(new File(this.fnAps));
			this.opsMgr.loadJson(new File(this.fnOps));
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

		this.serviceAggr.clear();

		// /Grab endpoint info
		String groupname = (String) input.get(0);
		String service = (String) input.get(1);
		// Get timeline info
		DefaultDataBag bag = (DefaultDataBag) input.get(2);
		// Iterate the whole timeline
		Iterator<Tuple> it_bag = bag.iterator();

		while (it_bag.hasNext()) {
			Tuple cur_item = it_bag.next();
			// Get timeline item info
			String hostname = (String) cur_item.get(0);
			DefaultDataBag bag2 = (DefaultDataBag) cur_item.get(1);

			Iterator<Tuple> it_bag2 = bag2.iterator();

			int j = 0;

			while (it_bag2.hasNext()) {

				Tuple cur_subitem = it_bag2.next();

				this.serviceAggr.insertSlot(hostname, j,
						Integer.parseInt(cur_subitem.get(0).toString()));

				j++;

			}

		}

		// Get the availability profile - One per job
		String avProfile = this.apsMgr.getAvProfiles().get(0);
		// Get the availability Group in which this service belongs
		String avGroup = this.apsMgr.getGroupByService(avProfile, service);
		// Get the availability operation on this service instances
		String avOp = this.apsMgr.getProfileGroupServiceOp(avProfile, avGroup,
				service);

		this.serviceAggr.aggregate(avOp, this.opsMgr); // now the operation is
														// read from
														// availability file

		// Create output Tuple
		Tuple output = tupFactory.newTuple();
		DataBag outBag = bagFactory.newDefaultBag();

		output.append(groupname);
		output.append(service);

		// Append the timeline
		for (int i = 0; i < this.serviceAggr.aggregation.samples.length; i++) {
			Tuple cur_tupl = tupFactory.newTuple();
			// cur_tupl.append(i);
			cur_tupl.append(this.serviceAggr.aggregation.samples[i]);
			outBag.add(cur_tupl);
		}

		output.append(outBag);

		if (outBag.size() == 0)
			return null;

		return output;

	}

	@Override
	public Schema outputSchema(Schema input) {

		Schema.FieldSchema groupname = new Schema.FieldSchema("groupname",
				DataType.CHARARRAY);
		Schema.FieldSchema service = new Schema.FieldSchema("service",
				DataType.CHARARRAY);

		// Schema.FieldSchema slot = new Schema.FieldSchema("slot",
		// DataType.INTEGER);
		Schema.FieldSchema statusInt = new Schema.FieldSchema("status",
				DataType.INTEGER);

		Schema endpoint = new Schema();
		Schema timeline = new Schema();

		endpoint.add(groupname);
		endpoint.add(service);

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