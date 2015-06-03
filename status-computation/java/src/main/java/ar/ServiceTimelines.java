package ar;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import ops.DAggregator;
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

public class ServiceTimelines extends EvalFunc<Tuple> {

	private static final Logger LOG = Logger.getLogger(ServiceTimelines.class.getName());
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

		this.serviceAggr.clear();

		String groupname;
		String service;
		DefaultDataBag bag;
		try {
			// /Grab endpoint info
			groupname = (String)input.get(0);
			service = (String)input.get(1);
			// Get timeline info
			bag = (DefaultDataBag) input.get(2);
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

			String hostname;
			DefaultDataBag bag2;
			try {
				// Get timeline item info
				hostname = (String)cur_item.get(0);
				bag2 = (DefaultDataBag)cur_item.get(1);
			} catch (ClassCastException e) {
				LOG.error("Failed to cast input to approriate type");
				LOG.error("Bad tuple input:" + cur_item.toString());
				throw new RuntimeException("pig Eval bad input");
			} catch (IndexOutOfBoundsException e) {
				LOG.error("Malformed tuple schema");
				LOG.error("Bad tuple input:" + cur_item.toString());
				throw new RuntimeException("pig Eval bad input");
			} catch (ExecException e) {
				LOG.error("Execution error");
				throw new RuntimeException("pig Eval bad input");
			}

			Iterator<Tuple> it_bag2 = bag2.iterator();

			int j = 0;

			while (it_bag2.hasNext()) {

				Tuple cur_subitem = it_bag2.next();
				try {
					this.serviceAggr.insertSlot(hostname, j,
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