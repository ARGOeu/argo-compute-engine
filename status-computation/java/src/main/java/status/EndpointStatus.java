package status;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import ops.CAggregator;
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
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;

import sync.AvailabilityProfiles;
import sync.Downtimes;
import sync.MetricProfiles;

public class EndpointStatus extends EvalFunc<Tuple> {

	private static final Logger LOG = Logger.getLogger(EndpointStatus.class.getName());
	public CAggregator endpointAggr;
	public OpsManager opsMgr;
	public Downtimes downMgr;
	public AvailabilityProfiles avMgr;
	public MetricProfiles metricMgr;

	private TupleFactory tupFactory;
	private BagFactory bagFactory;

	private String fnAvProfiles;
	private String fnOps;
	private String fnMetricProfiles;
	private String targetDate;

	private String fsUsed; // local,hdfs,cache (distrubuted_cache)

	private boolean initialized;

	public EndpointStatus(String fnOps, String fnAvProfiles, String fnMetricProfiles,
			String targetDate, String fsUsed) throws IOException {
		// set first the filenames
		this.fnOps = fnOps;

		this.fnAvProfiles = fnAvProfiles;
		this.fnMetricProfiles = fnMetricProfiles;
		// set distribute cache flag
		this.fsUsed = fsUsed;

		// set the targetDate var
		this.targetDate = targetDate;


		// set the Structures
		this.endpointAggr = new CAggregator(); // Create
																			// Aggregator
																			// according
																			// to
																			// sampling
																			// freq.
		this.opsMgr = new OpsManager();
		this.downMgr = new Downtimes();
		this.avMgr = new AvailabilityProfiles();
		this.metricMgr = new MetricProfiles();

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
			this.avMgr.loadJson(new File("./aps"));
			this.metricMgr.loadAvro(new File("./mps"));

		} else if (this.fsUsed.equalsIgnoreCase("local")) {
			this.opsMgr.loadJson(new File(this.fnOps));
			this.avMgr.loadJson(new File(this.fnAvProfiles));
			this.metricMgr.loadAvro(new File(this.fnMetricProfiles));
		}

		this.initialized = true;

	}

	public List<String> getCacheFiles() {
		List<String> list = new ArrayList<String>();
		list.add(this.fnOps.concat("#ops"));
		list.add(this.fnAvProfiles.concat("#aps"));
		list.add(this.fnMetricProfiles.concat("#mps"));
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

		this.endpointAggr.clear();

		/// Grab endpoint info
		String report;
		String endpointGroup;
		String service;
		String hostname;
		DefaultDataBag bag;

		try {
			// Get Arguments
			report = (String) input.get(0);
			endpointGroup = (String) input.get(1);
			service = (String) input.get(2);
			hostname = (String) input.get(3);
			// Get timeline info
			bag = (DefaultDataBag) input.get(4);
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
		Iterator<Tuple> it_bag = bag.iterator();
		
		

		// Before reading metric messages, init expected metric timelines
		// Only 1 profile per job
		String mProfile = metricMgr.getProfiles().get(0);
		// Get default missing state
		int defMissing = this.opsMgr.getDefaultMissingInt();
		// Iterate all metric names of profile and initiate timelines
		// set deftimestamp
		LocalDate length = this.endpointAggr.getDate();
		String defTimestamp = this.endpointAggr.tsFromDate(targetDate);
		String prevMetricName = "";
		
		ArrayList<String> test = this.metricMgr.getProfileServiceMetrics(mProfile, service);
		
		this.metricMgr.getProfileServiceMetrics(mProfile, service);
		
		for (String mName : this.metricMgr.getProfileServiceMetrics(mProfile, service)) {
			this.endpointAggr.createTimeline(mName,defTimestamp, defMissing);
		}

		while (it_bag.hasNext()) {
			Tuple cur_item = it_bag.next();
			// Get timeline item info
			String metric;
			String ts;
			String status;
			String prevStatus;
			try {
				metric = (String) cur_item.get(0);
				ts = (String) cur_item.get(1);
				status = (String) cur_item.get(2);
				prevStatus = (String) cur_item.get(3);
			} catch (ClassCastException e) {
				LOG.error("Failed to cast input to approriate type");
				LOG.error("Bad tuple input:" + cur_item.toString());
				LOG.error(e);
				throw new IllegalArgumentException();
			} catch (IndexOutOfBoundsException e) {
				LOG.error("Malformed tuple schema");
				LOG.error("Bad tuple input:" + cur_item.toString());
				LOG.error(e);
				throw new IllegalArgumentException();
			} catch (ExecException e) {
				LOG.error("Execution error");
				LOG.error(e);
				throw new IllegalArgumentException();
			}
			
			// Check if we are in the switch of a new metric name
			if (prevMetricName.equals(metric) == false ) {
				this.endpointAggr.setFirst(metric, ts,  this.opsMgr.getIntStatus(prevStatus));
				prevMetricName=metric;
				continue;
			}
			

			this.endpointAggr.insert(metric, ts, this.opsMgr.getIntStatus(status));
			prevMetricName=metric;

		}


		String aprofile = this.avMgr.getAvProfiles().get(0);

		this.endpointAggr.aggregate( this.opsMgr,this.avMgr.getMetricOp(aprofile));

		
		// Create output Tuple
		Tuple output = tupFactory.newTuple();
		DataBag outBag = bagFactory.newDefaultBag();
		
		output.append(report);
		output.append(endpointGroup);
		output.append(service);
		output.append(hostname);

		// Append the timeline
		for (Entry<DateTime,Integer> item : this.endpointAggr.getSamples()) {
			Tuple cur_tupl = tupFactory.newTuple();
			// cur_tupl.append(i);
			cur_tupl.append(item.getKey().toString(DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")));
			cur_tupl.append(opsMgr.getStrStatus(item.getValue()));
			outBag.add(cur_tupl);
		}

		output.append(outBag);

		if (outBag.size() == 0)
			return null;

		return output;

	}

	@Override
	public Schema outputSchema(Schema input) {
		Schema.FieldSchema report = new Schema.FieldSchema("service", DataType.CHARARRAY);
		Schema.FieldSchema endpointGroup = new Schema.FieldSchema("endpoint_group", DataType.CHARARRAY);
		Schema.FieldSchema service = new Schema.FieldSchema("service", DataType.CHARARRAY);
		Schema.FieldSchema hostname = new Schema.FieldSchema("hostname", DataType.CHARARRAY);

		Schema.FieldSchema timestamp = new Schema.FieldSchema("timestamp", DataType.CHARARRAY);
		Schema.FieldSchema status = new Schema.FieldSchema("status", DataType.CHARARRAY);

		Schema endpoint = new Schema();
		Schema timeline = new Schema();

		endpoint.add(service);
		endpoint.add(hostname);

		timeline.add(timestamp);
		timeline.add(status);

		Schema.FieldSchema tl = null;
		try {
			tl = new Schema.FieldSchema("timeline", timeline, DataType.BAG);
		} catch (FrontendException ex) {
			LOG.error(ex);

		}

		endpoint.add(tl);

		try {
			return new Schema(new Schema.FieldSchema("endpoint", endpoint, DataType.TUPLE));
		} catch (FrontendException ex) {
			LOG.error(ex);

		}

		return null;
	}

}
