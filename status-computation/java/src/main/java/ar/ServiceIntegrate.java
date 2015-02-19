package ar;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import ops.DIntegrator;
import ops.DTimeline;
import ops.OpsManager;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import sync.AvailabilityProfiles;
import sync.GroupsOfGroups;

public class ServiceIntegrate extends EvalFunc<Tuple> {

	public AvailabilityProfiles apMgr;
	public GroupsOfGroups ggMgr;
	public OpsManager opsMgr;

	private String fnAps;
	public String fnGroups;

	public String fnOps;

	private String fsUsed;
	public DIntegrator arMgr;
	
	private boolean initialized;

	private TupleFactory tupFactory;
	private BagFactory bagFactory;

	public ServiceIntegrate(String fnOps, String fnAps, String fsUsed) {
		this.fnAps = fnAps;

		this.fsUsed = fsUsed;
		this.fnOps = fnOps;

		this.apMgr = new AvailabilityProfiles();
		this.opsMgr = new OpsManager();
		this.arMgr = new DIntegrator();
		

		this.tupFactory = TupleFactory.getInstance();
		this.bagFactory = BagFactory.getInstance();

		this.initialized = false;
	}

	public void init() throws IOException {
		if (this.fsUsed.equalsIgnoreCase("cache")) {
			this.apMgr.loadJson(new File("./aps"));
			this.opsMgr.loadJson(new File("./ops"));
		}
		else if (this.fsUsed.equalsIgnoreCase("local")) {
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
	public Tuple exec(Tuple input) throws IOException {
		
		// Check if cache files have been opened
		if (this.initialized == false) {
			this.init(); // If not open them
		}
		
		if (input == null || input.size() == 0)
			return null;

		String groupname = (String) input.get(0);
		String service = (String) input.get(1);
		// Get the Timeline
		DTimeline serviceTl = new DTimeline();

		DefaultDataBag bag = (DefaultDataBag) input.get(2);
		Iterator<Tuple> itBag = bag.iterator();
		int j = 0;
		while (itBag.hasNext()) {
			Tuple curItem = itBag.next();
			serviceTl.samples[j] = Integer.parseInt(curItem.get(0).toString());
			j++;
		}

		this.arMgr.calculateAR(serviceTl.samples, this.opsMgr);

		Tuple output = tupFactory.newTuple();
		output.append(service);
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
		
		 Schema serviceAR = new Schema();
		 Schema.FieldSchema service  = new Schema.FieldSchema("service", DataType.DOUBLE);
		 Schema.FieldSchema groupname  = new Schema.FieldSchema("groupname", DataType.DOUBLE);
		 Schema.FieldSchema av  = new Schema.FieldSchema("availability", DataType.DOUBLE);
         Schema.FieldSchema rel   = new Schema.FieldSchema("reliability",  DataType.DOUBLE);
         Schema.FieldSchema upFraction       = new Schema.FieldSchema("up_f",       DataType.DOUBLE);
         Schema.FieldSchema unknownFraction  = new Schema.FieldSchema("unknown_f",  DataType.DOUBLE);
         Schema.FieldSchema downFraction     = new Schema.FieldSchema("down_f",     DataType.DOUBLE);
         
         serviceAR.add(service);
         serviceAR.add(groupname);
         serviceAR.add(av);
         serviceAR.add(rel);
         serviceAR.add(upFraction);
         serviceAR.add(unknownFraction);
         serviceAR.add(downFraction);
         
         return serviceAR;
         
	}


}
