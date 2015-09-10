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


public class StatusMetricTest {

	@Test
	public void test() throws URISyntaxException, IOException {
		// Prepare Resource File
		URL resJsonFile = StatusMetricTest.class.getResource("/ops/EGI-algorithm.json");
		File jsonFile = new File(resJsonFile.toURI());
		
		// Prepare Resource File
		String jsonStr = IOUtils.toString(this.getClass().getResourceAsStream("/status/metric.json"), "UTF-8");
		TupleFactory tf = TupleFactory.getInstance();

		Tuple inpTuple = JsonToPig.jsonToTuple(jsonStr);
		Tuple cur = tf.newTuple();
		EndpointStatus et = new EndpointStatus("", "", "", "2015-02-06", "test");

		URL downRes = EndpointTimelinesTest.class.getResource("/avro/downtimes_v2.avro");
		File downAvro = new File(downRes.toURI());

		URL avpJsonFile = EndpointTimelines.class.getResource("/ops/ap1.json");
		File avpFile = new File(avpJsonFile.toURI());

		URL metricRes = EndpointTimelines.class.getResource("/avro/poem_sync_v2.avro");
		File metricFile = new File(metricRes.toURI());

		et.avMgr.loadJson(avpFile);
		et.downMgr.loadAvro(downAvro);
		et.metricMgr.loadAvro(metricFile);
		et.opsMgr.loadJson(jsonFile);
		cur = et.exec(inpTuple);
		
		
		System.out.println(cur);
		
	}

}
