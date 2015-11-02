package status;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import org.apache.log4j.Logger;

import ops.ConfigManager;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.eclipse.jdt.core.dom.ThisExpression;

import ar.AddGroupInfo;
import sync.AvailabilityProfiles;
import sync.EndpointGroups;
import sync.GroupsOfGroups;

public class PrepStatusDetails extends EvalFunc<Tuple> {

	private String fnGgrp;
	private String fnEgrp;
	private String fnCfg;

	private String targetDate;

	private String fsUsed;

	// public AvailabilityProfiles apsMgr;
	public EndpointGroups egrpMgr;
	public GroupsOfGroups ggrpMgr;
	public ConfigManager cfgMgr;

	private TupleFactory tupFactory;

	private boolean initialized;

	private static final Logger LOG = Logger.getLogger(PrepStatusDetails.class.getName());

	public PrepStatusDetails(String fnGgrp, String fnEgrp, String fnCfg, String targetDate, String fsUsed) {

		this.targetDate = targetDate;

		this.fnGgrp = fnGgrp;
		this.fnEgrp = fnEgrp;
		this.fnCfg = fnCfg;
		this.fsUsed = fsUsed;

		this.egrpMgr = new EndpointGroups();
		this.ggrpMgr = new GroupsOfGroups();
		this.cfgMgr = new ConfigManager();

		this.tupFactory = TupleFactory.getInstance();

		this.initialized = false;

	}

	public void init() throws IOException {
		if (this.fsUsed.equalsIgnoreCase("cache")) {
			this.egrpMgr.loadAvro(new File("./egrp"));
			this.ggrpMgr.loadAvro(new File("./ggrp"));
			this.cfgMgr.loadJson(new File("./cfg"));
		} else if (this.fsUsed.equalsIgnoreCase("local")) {
			this.egrpMgr.loadAvro(new File(this.fnEgrp));
			this.ggrpMgr.loadAvro(new File(this.fnGgrp));
			this.cfgMgr.loadJson(new File(this.fnCfg));
		}

		this.initialized = true;
	}

	public List<String> getCacheFiles() {
		List<String> list = new ArrayList<String>();
		list.add(this.fnGgrp.concat("#ggrp"));
		list.add(this.fnEgrp.concat("#egrp"));
		list.add(this.fnCfg.concat("#cfg"));

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

		// parse input
		String monitoringHost = (String) input.get(0);
		String service = (String) input.get(1);
		String hostname = (String) input.get(2);
		String metric = (String) input.get(3);

		DefaultDataBag timeline = (DefaultDataBag) input.get(4);
		Iterator<Tuple> myit = timeline.iterator();
		Tuple item;

		Tuple output = this.tupFactory.newTuple();
		DefaultDataBag outBag = new DefaultDataBag();

		String prevState = "MISSING";
		String prevTs = this.targetDate.concat("T00:00:00Z");

		for (int i = 0; i < timeline.size(); i++) {
			item = myit.next();

			String curTsDate = (String) item.get(0);
			String curTsDay = curTsDate.substring(0, curTsDate.indexOf("T"));

			item.append(prevState);
			item.append(prevTs);

			// Calculate integer of date and time
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
			Date parsedDate = null;

			try {
				parsedDate = dateFormat.parse((String) item.get(0));
			} catch (ParseException e) {
				LOG.error(e);
			}
			Calendar cal = Calendar.getInstance();
			cal.setTime(parsedDate);

			int date_int = (cal.get(Calendar.YEAR) * 10000) + ((cal.get(Calendar.MONTH) + 1) * 100)
					+ (cal.get(Calendar.DAY_OF_MONTH));
			int time_int = (cal.get(Calendar.HOUR_OF_DAY) * 10000) + ((cal.get(Calendar.MINUTE) * 100))
					+ (cal.get(Calendar.SECOND));

			item.append(date_int);
			item.append(time_int);

			prevState = (String) item.get(1);
			prevTs = (String) item.get(0);

			if (curTsDay.equals(this.targetDate)) {
				outBag.add(item);
			}
		}
		
		// Find endpoint group
		String egroupType = this.cfgMgr.egroup;
		String egroupName = this.egrpMgr.getGroup(egroupType, hostname, service);

	
		// add stuff to the output
		output.append(this.cfgMgr.id); // Add report id
		output.append(egroupName);
		output.append(monitoringHost);
		output.append(service);		   
		output.append(hostname);
		output.append(metric);
		output.append(outBag);

		return output;

	}

	@Override
	public Schema outputSchema(Schema input) {
		Schema.FieldSchema report = new Schema.FieldSchema("report",DataType.CHARARRAY);
		Schema.FieldSchema endpointGroup = new Schema.FieldSchema("endpoint_group", DataType.CHARARRAY); 
		Schema.FieldSchema monitoringBox = new Schema.FieldSchema("monitoring_box", DataType.CHARARRAY);
		Schema.FieldSchema hostname = new Schema.FieldSchema("hostname", DataType.CHARARRAY);
		Schema.FieldSchema serviceType = new Schema.FieldSchema("service", DataType.CHARARRAY);
		Schema.FieldSchema metric = new Schema.FieldSchema("metric", DataType.CHARARRAY);
		
		Schema.FieldSchema timestamp = new Schema.FieldSchema("timestamp", DataType.CHARARRAY);
		Schema.FieldSchema status = new Schema.FieldSchema("status", DataType.CHARARRAY);
		Schema.FieldSchema summary = new Schema.FieldSchema("summary", DataType.CHARARRAY);
		Schema.FieldSchema message = new Schema.FieldSchema("message", DataType.CHARARRAY);
		Schema.FieldSchema prevState = new Schema.FieldSchema("previous_state", DataType.CHARARRAY);
		Schema.FieldSchema prevTs = new Schema.FieldSchema("previous_timestamp", DataType.CHARARRAY);
		Schema.FieldSchema dateInt = new Schema.FieldSchema("date_integer", DataType.INTEGER);
		Schema.FieldSchema timeInt = new Schema.FieldSchema("time_integer", DataType.INTEGER);

		Schema statusMetric = new Schema();
		Schema timeline = new Schema();

		statusMetric.add(report);
		statusMetric.add(endpointGroup);
		statusMetric.add(monitoringBox);
		statusMetric.add(serviceType);
		statusMetric.add(hostname);
		statusMetric.add(metric);

		timeline.add(timestamp);
		timeline.add(status);
		timeline.add(summary);
		timeline.add(message);
		timeline.add(prevState);
		timeline.add(prevTs);
		timeline.add(dateInt);
		timeline.add(timeInt);

		Schema.FieldSchema timelines = null;
		try {
			timelines = new Schema.FieldSchema("timelines", timeline, DataType.BAG);
		} catch (FrontendException ex) {
			LOG.error(ex);
		}

		statusMetric.add(timelines);

		try {
			return new Schema(new Schema.FieldSchema("status_metric", statusMetric, DataType.TUPLE));
		} catch (FrontendException ex) {
			LOG.error(ex);
		}

		return null;
	}

}