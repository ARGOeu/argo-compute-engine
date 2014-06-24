package myudf;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import TestIO.JsonToPig;

public class SFAvailabilityTest {

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
	public void testExecTuple() throws IOException {
		String json_str = IOUtils.toString(this.getClass().getResourceAsStream("/sfa_data/sfa_input.json"),"UTF-8");
		String expectedString = IOUtils.toString(this.getClass().getResourceAsStream("/sfa_data/sfa_output.txt"),"UTF-8");
		TupleFactory tf = TupleFactory.getInstance();
		Tuple cur = tf.newTuple();
		Tuple tmp = JsonToPig.jsonToTuple(json_str);
		SFAvailability sfa = new SFAvailability();
		String resultString = sfa.exec(tmp).toString();
		assertTrue(resultString.compareTo(expectedString) == 0);
	}

}
