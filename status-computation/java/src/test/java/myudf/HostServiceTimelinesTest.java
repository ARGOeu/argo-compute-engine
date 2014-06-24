package myudf;

import static org.junit.Assert.*;

import java.io.IOException;


import org.apache.commons.io.IOUtils;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import TestIO.JsonToPig;

public class HostServiceTimelinesTest {

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
		/*String json_str = IOUtils.toString(this.getClass().getResourceAsStream("/pigData/timetables.json"),"UTF-8");
		String poems = IOUtils.toString(this.getClass().getResourceAsStream("/poems/poemsIn.txt"),"UTF-8");
		String downtimes = IOUtils.toString(this.getClass().getResourceAsStream("/downtimes/downtimesIn.txt"),"UTF-8");
		TupleFactory tf = TupleFactory.getInstance();
		Tuple cur = tf.newTuple();
		Tuple tmp = JsonToPig.jsonToTuple(json_str);
		
		System.out.println("timelines: " + tmp.get(4).toString());
		
		System.out.println("profile: " + tmp.get(2).toString());
		System.out.println("cur_date:"+"2014-02-18");
		System.out.println("hostname: " + tmp.get(0).toString());
		System.out.println("service_flavour: " + tmp.get(1).toString());
		System.out.println("prev_date:"+"2014-02-18");
		System.out.println("downtimes:"+ downtimes);
		System.out.println("poems:" + poems);
		
		// By the above 
		
		cur.append(tmp.get(4));
		cur.append(tmp.get(2));
		cur.append("20140218");
		cur.append(tmp.get(0));
		cur.append(tmp.get(1));
		cur.append("20140218");
		cur.append(downtimes);
		cur.append(poems);
		
		System.out.println(cur.toString());
		
		HostServiceTimelines hst = new HostServiceTimelines();
		System.out.println(hst.exec(cur).toString());*/
		
		
		String json_str = IOUtils.toString(this.getClass().getResourceAsStream("/hst_data/hst_input_data.json"),"UTF-8");
		String expectedString = IOUtils.toString(this.getClass().getResourceAsStream("/hst_data/hst_output.txt"),"UTF-8");
		TupleFactory tf = TupleFactory.getInstance();
		Tuple cur = tf.newTuple();
		Tuple tmp = JsonToPig.jsonToTuple(json_str);
		HostServiceTimelines hst = new HostServiceTimelines();
		String resultString = hst.exec(tmp).toString();
		assertTrue(resultString.compareTo(expectedString) == 0);
	}

}
