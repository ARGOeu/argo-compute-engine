package ar;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import ops.OpsManager;
import ops.OpsManagerTest;

import org.apache.commons.io.IOUtils;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.BeforeClass;
import org.junit.Test;

import TestIO.JsonToPig;

public class EndpointTimelinesTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", EndpointTimelinesTest.class.getResource("/ar/endpoint_timeline.json"));
	}
	
	@Test
	public void test() throws URISyntaxException, IOException {
		//Prepare Resource File
		URL resJsonFile = OpsManagerTest.class.getResource("/ops/EGI-algorithm.json");
		File jsonFile = new File(resJsonFile.toURI());
		// Instatiate class
		
		String jsonStr = IOUtils.toString(this.getClass().getResourceAsStream("/ar/endpoint_timeline.json"),"UTF-8");
		TupleFactory tf = TupleFactory.getInstance();
		
		Tuple cur = tf.newTuple();
		Tuple inpTuple = JsonToPig.jsonToTuple(jsonStr);
		EndpointTimelines et = new EndpointTimelines("","","local");
	   
		
		
		et.opsMgr.openFile(jsonFile);
		cur = et.exec(inpTuple);
		
		
		
		Tuple expTuple = tf.newTuple();
		
		BagFactory bf = BagFactory.getInstance();
		DataBag expBag = bf.newDefaultBag();
		
		for (int i=0;i<288;i++) {
			Tuple subTuple = tf.newTuple();
			subTuple.append(4);
			expBag.add(subTuple);
		}
		
		expTuple.append("unicore6.TargetSystemFactory");
		expTuple.append("unicore-ui.reef.man.poznan.pl");
		expTuple.append(expBag);
		
		assertTrue(expTuple.toString().equals(cur.toString()));
	}

}
