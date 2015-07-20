package ar;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import ops.DIntegrator;
import ops.DTimeline;
import ops.OpsManager;

import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import sync.AvailabilityProfiles;

public class GroupEndpointIntegrate extends EvalFunc<Tuple> {

	private static final Logger LOG = Logger.getLogger(GroupEndpointIntegrate.class.getName());
	public AvailabilityProfiles apMgr;
	public OpsManager opsMgr;

	private String fnAps;
	public String fnOps;

	private String fsUsed;

	public DIntegrator arMgr;

	private boolean initialized = false;

	private TupleFactory tupFactory;


	public GroupEndpointIntegrate(String fnOps, String fnAps, String fsUsed) {
		this.fnAps = fnAps;
		this.fsUsed = fsUsed;
		this.fnOps = fnOps;

		this.apMgr = new AvailabilityProfiles();
		this.opsMgr = new OpsManager();
		this.arMgr = new DIntegrator();

		this.tupFactory = TupleFactory.getInstance();
	}

	public void init() throws IOException {

		if (this.fsUsed.equalsIgnoreCase("cache")) {
			this.apMgr.loadJson(new File("./aps"));
			this.opsMgr.loadJson(new File("./ops"));
		} else if (this.fsUsed.equalsIgnoreCase("local")) {
			this.apMgr.loadJson(new File(this.fnAps));
			this.opsMgr.loadJson(new File(this.fnOps));
		}

		this.initialized = true;

	}

	public List<String> getCacheFiles() {
		List<String> list = new ArrayList<String>();
		list.add(this.fnAps.concat("#aps"));
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

		if (input == null || input.size() == 0)
			return null;

		String groupname;
		DefaultDataBag bag;
		try {
			groupname = (String)input.get(0);
			bag = (DefaultDataBag) input.get(1);
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

		// Get the Timeline
		DTimeline siteTl = new DTimeline();
		
		Iterator<Tuple> itBag = bag.iterator();
		int j = 0;
		while (itBag.hasNext()) {
			Tuple curItem = itBag.next();
			try {
				siteTl.samples[j] = Integer.parseInt(curItem.get(0).toString());
			} catch (NumberFormatException e) {
	    		LOG.error ("Failed to cast input to approriate type");
	    		LOG.error ("Bad subitem:" + curItem.toString());
	    		LOG.error(e);
	    		throw new IllegalArgumentException();
			} catch (ExecException e) {
	    		LOG.error ("Execution error");
	    		LOG.error(e);
	    		throw new IllegalArgumentException();
			}
			j++;
		}

		this.arMgr.calculateAR(siteTl.samples, this.opsMgr);

		Tuple output = tupFactory.newTuple();

		output.append(groupname);
		output.append(this.arMgr.availability);
		output.append(this.arMgr.reliability);
		output.append(this.arMgr.up_f);
		output.append(this.arMgr.unknown_f);
		output.append(this.arMgr.down_f);

		return output;
	}

	@Override
	public Schema outputSchema(Schema input) {

		Schema groupEndpointAR = new Schema();

		Schema.FieldSchema groupname = new Schema.FieldSchema("groupname",
				DataType.DOUBLE);
		Schema.FieldSchema av = new Schema.FieldSchema("availability",
				DataType.DOUBLE);
		Schema.FieldSchema rel = new Schema.FieldSchema("reliability",
				DataType.DOUBLE);
		Schema.FieldSchema upFraction = new Schema.FieldSchema("up_f",
				DataType.DOUBLE);
		Schema.FieldSchema unknownFraction = new Schema.FieldSchema(
				"unknown_f", DataType.DOUBLE);
		Schema.FieldSchema downFraction = new Schema.FieldSchema("down_f",
				DataType.DOUBLE);

		groupEndpointAR.add(groupname);
		groupEndpointAR.add(av);
		groupEndpointAR.add(rel);
		groupEndpointAR.add(upFraction);
		groupEndpointAR.add(unknownFraction);
		groupEndpointAR.add(downFraction);

		return groupEndpointAR;

	}

}
