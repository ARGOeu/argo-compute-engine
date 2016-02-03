package ar;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.ParseException;

import ops.OpsManagerTest;

import org.apache.commons.io.IOUtils;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.BeforeClass;
import org.junit.Test;

import sync.EndpointGroupsTest;
import sync.GroupsOfGroupsTest;

public class PickEndpointsTest {

	@Test
	public void test() throws URISyntaxException, IOException, ParseException {
		// Prepare Resource File
		URL metricRes = EndpointGroupsTest.class.getResource("/avro/poem_sync_v2.avro");
		File metricAvro = new File(metricRes.toURI());

		URL groupEndpointRes = GroupsOfGroupsTest.class.getResource("/avro/group_endpoints_v2.avro");
		File groupEndpointAvro = new File(groupEndpointRes.toURI());
		
		URL groupGroupsRes = GroupsOfGroupsTest.class.getResource("/avro/group_groups_v2.avro");
		File groupGroupsAvro = new File(groupGroupsRes.toURI());

		URL apsRes = GroupsOfGroupsTest.class.getResource("/ops/ap1.json");
		File apsJson = new File(apsRes.toURI());
		
		URL recRes = GroupsOfGroupsTest.class.getResource("/ops/recomp.json");
		File recJson = new File(recRes.toURI());
		
		URL cfgRes = GroupsOfGroupsTest.class.getResource("/ops/config.json");
		File cfgJson = new File(cfgRes.toURI());

		PickEndpoints pt = new PickEndpoints("","", "", "", "", "", "1", "test");

		pt.mpsMgr.loadAvro(metricAvro);
		pt.egMgr.loadAvro(groupEndpointAvro);
		pt.ggMgr.loadAvro(groupGroupsAvro);
		pt.apsMgr.loadJson(apsJson);
		pt.recMgr.loadJson(recJson);
		pt.cfgMgr.loadJson(cfgJson);

		
		
		TupleFactory tf = TupleFactory.getInstance();
		
		// Exclude because of monitoring engine
		Tuple inp = tf.newTuple();
		inp.append("se01.afroditi.hellasgrid.gr");
		inp.append("SRMv2");
		inp.append("org.sam.SRM-Ls");
		inp.append("monA");
		inp.append("2013-12-08T13:03:44Z");

		assertEquals(false,pt.exec(inp));
		
		Tuple inp2 = tf.newTuple();
		inp2.append("se01.afroditi.hellasgrid.gr");
		inp2.append("SRMv2");
		inp2.append("org.sam.SRM-Ls");
		inp2.append("monB");
		inp2.append("2013-12-08T13:03:44Z");
		
		
		assertEquals(true,pt.exec(inp2));

	}

}
