package ar;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import ops.OpsManagerTest;

import org.apache.commons.io.IOUtils;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.BeforeClass;
import org.junit.Test;

import TestIO.JsonToPig;

public class GroupEndpointTimelinesTest {
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", GroupEndpointTimelinesTest.class.getResource("/ar/site_timeline.json"));
	}
	

	@Test
	public void test() throws IOException, URISyntaxException {
		
		//Prepare Resource File
		URL opsFile = GroupEndpointTimelines.class.getResource("/ops/EGI-algorithm.json");
		File opsJson = new File(opsFile.toURI());
		
		URL apsFile = GroupEndpointTimelines.class.getResource("/ops/ap1.json");
		File apsJson = new File(apsFile.toURI());
		
		// Instatiate class
		GroupEndpointTimelines st = new GroupEndpointTimelines("","","test","1440","5");
		st.apMgr.loadJson(apsJson);
		st.opsMgr.loadJson(opsJson);
		
		String jsonStr = IOUtils.toString(this.getClass().getResourceAsStream("/ar/site_timeline.json"),"UTF-8");
		TupleFactory tf = TupleFactory.getInstance();
		
		Tuple cur = tf.newTuple();
		Tuple inpTuple = JsonToPig.jsonToTuple(jsonStr);
		
	    cur = st.exec(inpTuple);
	    
	    System.out.println(cur);
		
		
		
		
	}

}
