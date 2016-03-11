package ar;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import ops.ConfigManager;
import sync.EndpointGroups;
import sync.MetricProfiles;

public class FillMissing extends EvalFunc<Tuple> {

	private static final Logger LOG = Logger.getLogger(AddGroupInfo.class.getName());
	private String fnCfg;
	private String fnMps;
	private String targetDate;

	private TupleFactory tupFactory;
	private BagFactory bagFactory;
	
	public ConfigManager cfgMgr;
	public MetricProfiles mpsMgr;
	
	private String fsUsed; // local,hdfs,cache (distrubuted_cache)
	
	private boolean initialized;

	public FillMissing(String targetDate, String fnConfig, String fsUsed) {
		

		this.fsUsed = fsUsed;

		this.fnCfg = fnConfig;
		
		this.targetDate = targetDate;
		
		this.cfgMgr = new ConfigManager();
		
		
		
		// set up factories
		this.tupFactory = TupleFactory.getInstance();
		this.bagFactory = BagFactory.getInstance();
	}

	public void init() throws IOException {
		if (this.fsUsed.equalsIgnoreCase("cache")) {
			this.cfgMgr.loadJson(new File("./cfg"));
			
		} else if (this.fsUsed.equalsIgnoreCase("local")) {
			
			this.cfgMgr.loadJson(new File(this.fnCfg));
			
		}

		this.initialized = true;
	}

	public List<String> getCacheFiles() {
		List<String> list = new ArrayList<String>();
		list.add(this.fnCfg.concat("#cfg"));
		return list;
	}

	@Override
	public Tuple exec(Tuple input) {

		// Check if cache files have been opened
		if (this.initialized == false) {
			try {
				this.init();
			} catch (IOException e) {
				LOG.error("Could not initialize sync structures");
				LOG.error(e);
				throw new IllegalStateException(e);
			}
		}

		if (input == null || input.size() == 0)
			return null;
		
		String service;
		String hostname;
		String metric;
		try {
			service = (String) input.get(0);
			hostname = (String) input.get(1);
			metric = (String) input.get(2);
			
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
		
		
		
		
		Tuple output = tupFactory.newTuple();
		
		output.append(cfgMgr.id);
		output.append("none");
		output.append(service);
		output.append(hostname);
		output.append(metric);
		output.append(targetDate+"T00:00:00Z");
		output.append("MISSING");
		output.append("missing");
		output.append("missing");
		
		return output;
	}

	@Override
	public Schema outputSchema(Schema input) {

		Schema.FieldSchema report = new Schema.FieldSchema("report", DataType.CHARARRAY);
		Schema.FieldSchema monBox = new Schema.FieldSchema("monitoring_box", DataType.CHARARRAY);
	
		Schema.FieldSchema service = new Schema.FieldSchema("service", DataType.CHARARRAY);
		Schema.FieldSchema hostname = new Schema.FieldSchema("hostname", DataType.CHARARRAY);
		Schema.FieldSchema metric = new Schema.FieldSchema("metric", DataType.CHARARRAY);
		
		Schema.FieldSchema ts = new Schema.FieldSchema("timestamp", DataType.CHARARRAY);
		Schema.FieldSchema status = new Schema.FieldSchema("status", DataType.CHARARRAY);
		
		Schema.FieldSchema msg = new Schema.FieldSchema("message", DataType.CHARARRAY);
		Schema.FieldSchema sum = new Schema.FieldSchema("summary", DataType.CHARARRAY);
		
		Schema endpoint = new Schema();
	
		
		endpoint.add(report);
		endpoint.add(monBox);
		endpoint.add(service);
		endpoint.add(hostname);
		endpoint.add(metric);
		endpoint.add(ts);
		endpoint.add(status);
		endpoint.add(msg);
		endpoint.add(sum);
		

		try {
			return new Schema(new Schema.FieldSchema("endpoint", endpoint, DataType.TUPLE));
		} catch (FrontendException ex) {
			LOG.error(ex);
		}

		return null;

	}

}
