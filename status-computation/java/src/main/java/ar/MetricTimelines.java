package ar;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

public class MetricTimelines extends EvalFunc<Tuple> {

	private static final Logger LOG = Logger.getLogger(MetricTimelines.class.getName());
	private String fnOps;
	private String targetDate;

	private String fsUsed; // local,hdfs,cache (distrubuted_cache)

	public DTimeline dtl;
	public OpsManager opsMgr;

	private int sPeriod;
	private int sInterval;

	private TupleFactory tupFactory;
	private BagFactory bagFactory;

	private boolean initialized;

	public MetricTimelines(String fnOps, String targetDate, String fsUsed, String sPeriod, String sInterval)
			throws IOException {
		// set first the filenames
		this.fnOps = fnOps;
		this.fsUsed = fsUsed;

		// set the targetDate var
		this.targetDate = targetDate;

		// set the sampling configuration
		this.sPeriod = Integer.parseInt(sPeriod);
		this.sInterval = Integer.parseInt(sInterval);

		// set the Structures
		this.dtl = new DTimeline(this.sPeriod, this.sInterval);
		this.opsMgr = new OpsManager();

		// set up factories
		this.tupFactory = TupleFactory.getInstance();
		this.bagFactory = BagFactory.getInstance();
		// this is not yet initialized because we need files from distributed
		// cache
		this.initialized = false;
	}

	public void init() throws IOException {
		// Open Files from distributed cache
		if (this.fsUsed.equalsIgnoreCase("cache")) {

			this.opsMgr.loadJson(new File("./ops"));
		} else if (this.fsUsed.equalsIgnoreCase("local")) {

			this.opsMgr.loadJson(new File(this.fnOps));
		}

		this.initialized = true;

	}

	public List<String> getCacheFiles() {
		List<String> list = new ArrayList<String>();
		list.add(this.fnOps.concat("#ops"));
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
		// Clear timeline
		this.dtl.clear();

		if (input == null || input.size() == 0)
			return null;

		String service;
		String hostname;
		String metric;
		DefaultDataBag bag;
		try {
			// /Grab endpoint info
			service = (String) input.get(0);
			hostname = (String) input.get(1);
			metric = (String) input.get(2);
			// Get timeline info
			bag = (DefaultDataBag) input.get(3);
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

		// Iterate the whole timeline
		Iterator<Tuple> itBag = bag.iterator();

		while (itBag.hasNext()) {
			Tuple curItem = itBag.next();

			String ts;
			String status;
			try {
				// Get timeline item info
				ts = (String) curItem.get(0);
				status = (String) curItem.get(1);
			} catch (ClassCastException e) {
				LOG.error("Failed to cast input to approriate type");
				LOG.error("Bad tuple input:" + curItem.toString());
				LOG.error(e);
				throw new IllegalArgumentException();
			} catch (IndexOutOfBoundsException e) {
				LOG.error("Malformed tuple schema");
				LOG.error("Bad tuple input:" + curItem.toString());
				LOG.error(e);
				throw new IllegalArgumentException();
			} catch (ExecException e) {
				LOG.error("Execution error");
				LOG.error(e);
				throw new IllegalArgumentException();
			}
			if (!(ts.substring(0, ts.indexOf("T")).equals(this.targetDate))) {
				this.dtl.setStartState(this.opsMgr.getIntStatus(status));
				continue;
			}

			try {

				this.dtl.insert(ts, opsMgr.getIntStatus(status));

			} catch (ParseException e) {
				LOG.error(e);
			}

		}

		this.dtl.settle(this.opsMgr.getDefaultMissingInt());

		// Create output Tuple
		Tuple output = tupFactory.newTuple();
		DataBag outBag = bagFactory.newDefaultBag();

		output.append(service);
		output.append(hostname);
		output.append(metric);

		// Append the timeline
		for (int i = 0; i < this.dtl.samples.length; i++) {
			Tuple cur_tupl = tupFactory.newTuple();

			// cur_tupl.append(i);
			cur_tupl.append(this.dtl.samples[i]);
			outBag.add(cur_tupl);
		}

		output.append(outBag);

		if (outBag.size() == 0)
			return null;

		return output;

	}

	@Override
	public Schema outputSchema(Schema input) {

		Schema.FieldSchema service = new Schema.FieldSchema("service", DataType.CHARARRAY);
		Schema.FieldSchema hostname = new Schema.FieldSchema("hostname", DataType.CHARARRAY);
		Schema.FieldSchema metric = new Schema.FieldSchema("metric", DataType.CHARARRAY);

		// Schema.FieldSchema slot = new Schema.FieldSchema("slot",
		// DataType.INTEGER);
		Schema.FieldSchema statusInt = new Schema.FieldSchema("status", DataType.INTEGER);

		Schema metricTl = new Schema();
		Schema timeline = new Schema();

		metricTl.add(service);
		metricTl.add(hostname);
		metricTl.add(metric);

		// timeline.add(slot);
		timeline.add(statusInt);

		Schema.FieldSchema tl = null;
		try {
			tl = new Schema.FieldSchema("timeline", timeline, DataType.BAG);
		} catch (FrontendException ex) {
			LOG.error(ex);

		}

		metricTl.add(tl);

		try {
			return new Schema(new Schema.FieldSchema("endpoint", metricTl, DataType.TUPLE));
		} catch (FrontendException ex) {
			LOG.error(ex);
		}

		return null;
	}

}
