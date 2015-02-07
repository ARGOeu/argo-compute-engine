package ar;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

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
	public void test() throws URISyntaxException, IOException {
		//Prepare Resource File
		URL metricRes = EndpointGroupsTest.class.getResource("/avro/poem_sync_test.avro");
		File metricAvro = new File(metricRes.toURI());
		
		URL groupEndpointRes = GroupsOfGroupsTest.class.getResource("/avro/group_endpoints_test.avro");
		File groupEndpointAvro = new File(groupEndpointRes.toURI());
		
		PickEndpoints pt = new PickEndpoints("","","local");
		
		pt.metricMgr.loadAvro(metricAvro);
		pt.endpointMgr.loadAvro(groupEndpointAvro);
		
		TupleFactory tf = TupleFactory.getInstance();
		Tuple inp = tf.newTuple();
		inp.append("se01.afroditi.hellasgrid.gr");
		inp.append("SRMv2");
		inp.append("org.sam.SRM-Ls");
		
		System.out.println(pt.exec(inp));
		
		
	}

}
