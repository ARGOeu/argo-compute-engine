package status;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import TestIO.JsonToPig;
import ar.EndpointTimelines;
import ar.EndpointTimelinesTest;

public class ServiceStatusTest {

	@Test
	public void test() throws URISyntaxException, IOException {
		// Prepare Resource File
		URL resJsonFile = this.getClass().getResource("/ops/EGI-algorithm.json");
		File jsonFile = new File(resJsonFile.toURI());
		
		// Prepare Resource File
		String jsonStr = IOUtils.toString(this.getClass().getResourceAsStream("/status/endpoint.json"), "UTF-8");
		TupleFactory tf = TupleFactory.getInstance();
		

		Tuple inpTuple = JsonToPig.jsonToTuple(jsonStr);
		Tuple cur = tf.newTuple();
		
		ServiceStatus et = new ServiceStatus("", "", "", "2015-02-06", "test");

		URL downRes = this.getClass().getResource("/avro/downtimes_v2.avro");
		File downAvro = new File(downRes.toURI());

		URL avpJsonFile = this.getClass().getResource("/ops/ap1.json");
		File avpFile = new File(avpJsonFile.toURI());

		URL metricRes = this.getClass().getResource("/avro/poem_sync_v2.avro");
		File metricFile = new File(metricRes.toURI());

		et.avMgr.loadJson(avpFile);
		et.downMgr.loadAvro(downAvro);
		et.metricMgr.loadAvro(metricFile);
		et.opsMgr.loadJson(jsonFile);
		cur = et.exec(inpTuple);
		
		String expected = "(Critical,20150602,GRIF,SRMv2,{(2015-06-02T00:00:00Z,UNKNOWN),(2015-06-02T15:18:31Z,CRITICAL),(2015-06-02T15:19:01Z,WARNING),(2015-06-02T15:19:12Z,OK)})";
		
		assertEquals(expected,cur.toString());
	}

}
