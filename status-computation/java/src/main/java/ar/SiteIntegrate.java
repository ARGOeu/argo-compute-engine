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
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.eclipse.jdt.core.dom.ThisExpression;

import sync.AvailabilityProfiles;
import sync.GroupsOfGroups;

public class SiteIntegrate extends EvalFunc<Tuple> {

	public AvailabilityProfiles apMgr;
	public GroupsOfGroups ggMgr;
	public OpsManager opsMgr;
	
	
	private String fnAps;
	public String fnGroups;
	
	public String fnOps;
	
	
	private String fsUsed;
	public DIntegrator arMgr;
	
	private boolean initialized = false;
	
	private TupleFactory tupFactory; 
    private BagFactory bagFactory;
	
	public SiteIntegrate(String fnOps, String fnAps, String fnGroups, String fsUsed) {
		this.fnAps = fnAps;
		this.fsUsed = fsUsed;
		this.fnOps = fnOps;
		this.fnGroups = fnGroups;
		this.apMgr = new AvailabilityProfiles();
		this.ggMgr = new GroupsOfGroups();
		this.opsMgr = new OpsManager();
		this.arMgr = new DIntegrator();
	
		this.tupFactory = TupleFactory.getInstance();
		this.bagFactory = BagFactory.getInstance();
	}
	
	public void init() throws IOException
	{
		if (this.fsUsed.equalsIgnoreCase("cache")){
			this.apMgr.loadProfileJson(new File("./aps"));
			this.ggMgr.loadAvro(new File("./groups"));
			this.opsMgr.openFile(new File("./ops"));
		}
		
		this.initialized=true;
		
	}
	
	public List<String> getCacheFiles() { 
        List<String> list = new ArrayList<String>(); 
        list.add(this.fnAps.concat("#aps"));
        list.add(this.fnGroups.concat("#groups"));
        list.add(this.fnOps.concat("#ops"));
        return list; 
	} 
	
	@Override
	public Tuple exec(Tuple input) throws IOException {
		
		// Check if cache files have been opened 
		if (this.initialized==false)
        {
        	this.init(); // If not open them 
        }
		
		if (input == null || input.size() == 0) return null;
		
		String sitename = (String) input.get(0);
		
		//Get the Timeline
		DTimeline siteTl = new DTimeline();
		
		DefaultDataBag bag =  (DefaultDataBag) input.get(1);
		Iterator<Tuple> itBag = bag.iterator();
		int j=0;
		while (itBag.hasNext()){
	    	Tuple curItem = itBag.next();
	    	siteTl.samples[j] = Integer.parseInt(curItem.get(0).toString());
	    	j++;
		}
		
		this.arMgr.calculateAR(siteTl.samples, this.opsMgr);
		
		String aprofile = this.apMgr.getAvProfiles().get(0);
		
		Tuple output = tupFactory.newTuple();
		output.append(this.apMgr.getProfileMetricProfile(aprofile));
		output.append(aprofile);
		output.append(sitename);
		output.append(this.ggMgr.getGroup("NGI", sitename)); // NGI
		output.append(this.arMgr.availability);
		output.append(this.arMgr.reliability);
		output.append(this.arMgr.up_f);
		output.append(this.arMgr.unknown_f);
		output.append(this.arMgr.down_f);
		
		return output;
	}
		
		
}
