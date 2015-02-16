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

public class ServiceTimelinesTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", ServiceTimelinesTest.class.getResource("/ar/service_timeline.json"));
	}
	
	@Test
	public void test() throws IOException, URISyntaxException {
		
		//Prepare Resource File
		URL resJsonFile = OpsManagerTest.class.getResource("/ops/EGI-algorithm.json");
		File jsonFile = new File(resJsonFile.toURI());
		
		//Prepare Resource File
		URL avpFilePath = OpsManagerTest.class.getResource("/ops/ap1.json");
		File avpJson = new File(avpFilePath.toURI());
		// Instatiate class
		
		String jsonStr = IOUtils.toString(this.getClass().getResourceAsStream("/ar/service_timeline.json"),"UTF-8");
		TupleFactory tf = TupleFactory.getInstance();
		
		Tuple cur = tf.newTuple();
		Tuple inpTuple = JsonToPig.jsonToTuple(jsonStr);
		ServiceTimelines st = new ServiceTimelines("","","local","1440","5");
	   
		st.apsMgr.loadJson(avpJson);
		st.opsMgr.loadJson(jsonFile);
		cur = st.exec(inpTuple);
		
		Tuple expTuple = tf.newTuple();
		expTuple.append("CA-VICTORIA-WESTGRID-T2");
		expTuple.append("CREAM-CE");
		
		
		
		BagFactory bf = BagFactory.getInstance();
		DataBag expBag = bf.newDefaultBag();
		for (int i=0;i<288;i++) {
			Tuple subTuple = tf.newTuple();
			subTuple.append(0);
			expBag.add(subTuple);
		}
		
		expTuple.append(expBag);
		
		//assertTrue(expTuple.toString().equals(cur.toString()));
		
		
		
	}


}
