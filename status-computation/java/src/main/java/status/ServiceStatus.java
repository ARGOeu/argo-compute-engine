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

public class ServiceStatus extends EvalFunc<Tuple> {

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

	public ServiceStatus(String fnOps, String fnAvProfiles, String fnMetricProfiles,
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

		/// Grab service info
		String report;
		int dateInteger;
		String endpointGroup;
		String service;
		DefaultDataBag hostBag;

		try {
			// Get Arguments
			report = (String) input.get(0);
			dateInteger = (Integer) input.get(1);
			endpointGroup = (String) input.get(2);
			service = (String) input.get(3);
			// Get timeline info
			hostBag = (DefaultDataBag) input.get(4);
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
		Iterator<Tuple> iterHost = hostBag.iterator();
		

		while (iterHost.hasNext()) {
			Tuple curHost = iterHost.next();
			// Get host and timeline
			String host;
			DefaultDataBag timeline;
			try {
				host = (String) curHost.get(0);
				timeline = (DefaultDataBag) curHost.get(1);
			} catch (ClassCastException e) {
				LOG.error("Failed to cast input to approriate type");
				LOG.error("Bad tuple input:" + curHost.toString());
				LOG.error(e);
				throw new IllegalArgumentException();
			} catch (IndexOutOfBoundsException e) {
				LOG.error("Malformed tuple schema");
				LOG.error("Bad tuple input:" + curHost.toString());
				LOG.error(e);
				throw new IllegalArgumentException();
			} catch (ExecException e) {
				LOG.error("Execution error");
				LOG.error(e);
				throw new IllegalArgumentException();
			}
			
			Iterator<Tuple> iterStatus = timeline.iterator();
			
			while (iterStatus.hasNext()){
				String timestamp;
				String status;
				Tuple curStatus = iterStatus.next();
				try {
					timestamp = (String) curStatus.get(0);
					status = (String) curStatus.get(1);
				} catch (ClassCastException e) {
					LOG.error("Failed to cast input to approriate type");
					LOG.error("Bad tuple input:" + curStatus.toString());
					LOG.error(e);
					throw new IllegalArgumentException();
				} catch (IndexOutOfBoundsException e) {
					LOG.error("Malformed tuple schema");
					LOG.error("Bad tuple input:" + curStatus.toString());
					LOG.error(e);
					throw new IllegalArgumentException();
				} catch (ExecException e) {
					LOG.error("Execution error");
					LOG.error(e);
					throw new IllegalArgumentException();
				}
				
				this.endpointAggr.insert(host, timestamp, this.opsMgr.getIntStatus(status));
				
				
			}

			

		}


		// Get the availability profile - One per job
		String avProfile = this.avMgr.getAvProfiles().get(0);
		// Get the availability Group in which this service belongs
		String avGroup = this.avMgr.getGroupByService(avProfile, service);
		// Get the availability operation on this service instances
		String avOp = this.avMgr.getProfileGroupServiceOp(avProfile, avGroup, service);

		this.endpointAggr.aggregate( this.opsMgr,avOp);

		
		// Create output Tuple
		Tuple output = tupFactory.newTuple();
		DataBag outBag = bagFactory.newDefaultBag();
		
		output.append(report);
		output.append(dateInteger);
		output.append(endpointGroup);
		output.append(service);
		

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
		Schema.FieldSchema report = new Schema.FieldSchema("report", DataType.CHARARRAY);
		Schema.FieldSchema dateInteger = new Schema.FieldSchema("date_integer", DataType.INTEGER);
		Schema.FieldSchema endpointGroup = new Schema.FieldSchema("endpoint_group", DataType.CHARARRAY);
		Schema.FieldSchema service = new Schema.FieldSchema("service", DataType.CHARARRAY);

		Schema.FieldSchema timestamp = new Schema.FieldSchema("timestamp", DataType.CHARARRAY);
		Schema.FieldSchema status = new Schema.FieldSchema("status", DataType.CHARARRAY);

		Schema endpoint = new Schema();
		Schema timeline = new Schema();

		endpoint.add(report);
		endpoint.add(dateInteger);
		endpoint.add(endpointGroup);
		endpoint.add(service);

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

