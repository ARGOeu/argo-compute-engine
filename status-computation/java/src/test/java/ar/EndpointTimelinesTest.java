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

import sync.EndpointGroupsTest;
import sync.GroupsOfGroupsTest;
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
		
		String jsonStr = IOUtils.toString(this.getClass().getResourceAsStream("/ar/cream-ce-timeline.json"),"UTF-8");
		TupleFactory tf = TupleFactory.getInstance();
		
		Tuple cur = tf.newTuple();
		Tuple inpTuple = JsonToPig.jsonToTuple(jsonStr);
		EndpointTimelines et = new EndpointTimelines("","","","","2015-02-06","test","1440","5");
	   
		URL downRes = EndpointTimelinesTest.class.getResource("/avro/downtimes_test.avro");
		File downAvro = new File(downRes.toURI());
		
		URL avpJsonFile = EndpointTimelines.class.getResource("/ops/ap1.json");
		File avpFile = new File(avpJsonFile.toURI());
		
		URL metricRes = EndpointTimelines.class.getResource("/avro/poem_sync_test.avro");
		File metricFile = new File(metricRes.toURI());
		
		
		et.avMgr.loadJson(avpFile);
		et.downMgr.loadAvro(downAvro);
		et.metricMgr.loadAvro(metricFile);
		et.opsMgr.loadJson(jsonFile);
		cur = et.exec(inpTuple);
		
		
		
		String curToStr="(CREAM-CE,cce.ihep.ac.cn,{(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(4),(4),(4),(4),(4),(4),(4),(4),(4),(4),(4),(4),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(4),(4),(4),(4),(4),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(4),(4),(4),(4),(4),(4),(4),(4),(4),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2),(2)})";
		assertTrue(curToStr.equals(cur.toString()));
		
		
		//assertTrue(expTuple.toString().equals(cur.toString()));
	}

}
