package status;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URISyntaxException;


import org.apache.commons.io.IOUtils;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import TestIO.JsonToPig;


public class StatusMetricTest {

	@Test
	public void test() throws URISyntaxException, IOException {
		// Prepare Resource File
		String jsonStr = IOUtils.toString(this.getClass().getResourceAsStream("/status/metric.json"), "UTF-8");
		TupleFactory tf = TupleFactory.getInstance();

		Tuple inpTuple = JsonToPig.jsonToTuple(jsonStr);
		
		System.out.println(inpTuple);
		
	}

}
