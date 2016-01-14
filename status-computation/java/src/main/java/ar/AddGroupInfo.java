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
import sync.EndpointGroups;

public class AddGroupInfo extends EvalFunc<Tuple> {

	private static final Logger LOG = Logger.getLogger(AddGroupInfo.class.getName());
	public String fnEndpointGroups;
	public String fnGroupGroups;

	private TupleFactory tupFactory;
	private BagFactory bagFactory;
	
	public String type;

	public EndpointGroups endpointMgr;
	// public GroupsOfGroups groupMgr;

	private String fsUsed; // local,hdfs,cache (distrubuted_cache)

	private boolean initialized;

	public AddGroupInfo(String fnEndpointGroups, String fnGroupGroups, String type, String fsUsed) {
		this.fnEndpointGroups = fnEndpointGroups;
		this.fnGroupGroups = fnGroupGroups;

		this.fsUsed = fsUsed;

		this.type = type; // type of group

		this.endpointMgr = new EndpointGroups();

		// set up factories
		this.tupFactory = TupleFactory.getInstance();
		this.bagFactory = BagFactory.getInstance();
	}

	public void init() throws IOException {
		if (this.fsUsed.equalsIgnoreCase("cache")) {
			this.endpointMgr.loadAvro(new File("./endpoint_groups"));
			// this.groupMgr.loadAvro(new File("./group_groups"));
		} else if (this.fsUsed.equalsIgnoreCase("local")) {
			this.endpointMgr.loadAvro(new File(this.fnEndpointGroups));
		}
		this.initialized = true;
	}

	public List<String> getCacheFiles() {
		List<String> list = new ArrayList<String>();
		list.add(this.fnEndpointGroups.concat("#endpoint_groups"));
		// list.add(this.fnGroupGroups.concat("#group_groups"));
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
				throw new IllegalStateException();
			}
		}

		if (input == null || input.size() == 0)
			return null;

		String service;
		String hostname;
		try {
			service = (String) input.get(0);
			hostname = (String) input.get(1);
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
		
		// Create Databag with available groups that this endopoint belongs to
		ArrayList<String> groups = endpointMgr.getGroup(this.type, hostname, service);
		// Create output Tuple
		DataBag groupBag = bagFactory.newDefaultBag();
		
		for (String group : groups)
		{
			Tuple cur_tupl = tupFactory.newTuple();
			cur_tupl.append(group);
			groupBag.add(cur_tupl);
		}
		input.append(groupBag);

		return input;

	}

	@Override
	public Schema outputSchema(Schema input) {

		Schema.FieldSchema service = new Schema.FieldSchema("service", DataType.CHARARRAY);
		Schema.FieldSchema hostname = new Schema.FieldSchema("hostname", DataType.CHARARRAY);
		// Schema.FieldSchema slot = new Schema.FieldSchema("slot",
		// DataType.INTEGER);
		Schema.FieldSchema statusInt = new Schema.FieldSchema("status", DataType.INTEGER);

		Schema endpoint = new Schema();
		Schema groups = new Schema();
		Schema timeline = new Schema();

		endpoint.add(service);
		endpoint.add(hostname);

		// timeline.add(slot);
		timeline.add(statusInt);
		
		Schema.FieldSchema tl = null;
		try {
			tl = new Schema.FieldSchema("timeline", timeline, DataType.BAG);
		} catch (FrontendException ex) {
			LOG.error(ex);
		}

		endpoint.add(tl);
		
		Schema.FieldSchema grp = null;
		try {
			grp = new Schema.FieldSchema("groups", groups, DataType.BAG);
		} catch (FrontendException ex) {
			LOG.error(ex);
		}
		
		endpoint.add(grp);

		try {
			return new Schema(new Schema.FieldSchema("endpoint", endpoint, DataType.TUPLE));
		} catch (FrontendException ex) {
			LOG.error(ex);
		}

		return null;

	}

}
