package pigServer;

import org.apache.pig.ExecType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;

import org.apache.pig.pigunit.Cluster;

import org.apache.pig.pigunit.pig.PigServer;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class PigServerTest {

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws ExecException {
		PigServer pigServer ;
	    PigContext pigContext ;
	    Cluster cluster ;
		pigServer = new PigServer(ExecType.LOCAL);
		pigContext = pigServer.getPigContext();
	
	    assertTrue(pigServer!=null);   
	    
	}

}
