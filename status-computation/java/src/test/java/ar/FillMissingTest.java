package ar;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import TestIO.JsonToPig;
import sync.EndpointGroupsTest;
import sync.GroupsOfGroupsTest;

public class FillMissingTest {

	@Test
	public void test() throws IOException, URISyntaxException {
		// Prepare Resource File
		URL metricRes = EndpointGroupsTest.class.getResource("/avro/poem_sync_v2.avro");
		File metricAvro = new File(metricRes.toURI());

		URL cfgRes = GroupsOfGroupsTest.class.getResource("/ops/config.json");
		File cfgJson = new File(cfgRes.toURI());

		FillMissing fm = new FillMissing("2016-02-10","", "", "test");

		fm.mpsMgr.loadAvro(metricAvro);
		fm.cfgMgr.loadJson(cfgJson);
		
		TupleFactory tf = TupleFactory.getInstance();
		
		Tuple inTuple = tf.newTuple();
		
		inTuple.append("SRMv2");
		inTuple.append("se01.afroditi.hellasgrid.gr");
		
		String jsonStr = IOUtils.toString(this.getClass().getResourceAsStream("/ar/missing_endpoint.json"), "UTF-8");
		Tuple expTuple = JsonToPig.jsonToTuple(jsonStr);
		Tuple outTuple = fm.exec(inTuple);
		
		assertTrue(expTuple.toString().equals(outTuple.toString()));
	
	}

}
