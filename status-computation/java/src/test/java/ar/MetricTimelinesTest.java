package ar;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import ops.DAggregatorTest;
import ops.OpsManagerTest;

import org.apache.commons.io.IOUtils;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.BeforeClass;
import org.junit.Test;

import TestIO.JsonToPig;

public class MetricTimelinesTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", MetricTimelinesTest.class.getResource("/ar/metric_timeline.json"));
	}
	
	
	@Test
	public void test() throws IOException, URISyntaxException {
		
		//Prepare Resource File
		URL resJsonFile = MetricTimelinesTest.class.getResource("/ops/EGI-algorithm.json");
		File jsonFile = new File(resJsonFile.toURI());
		// Instatiate class
		
		String jsonStr = IOUtils.toString(this.getClass().getResourceAsStream("/ar/metric_timeline.json"),"UTF-8");
		TupleFactory tf = TupleFactory.getInstance();
		
		Tuple cur = tf.newTuple();
		Tuple inpTuple = JsonToPig.jsonToTuple(jsonStr);
		MetricTimelines mt = new MetricTimelines("","","local");
	   
		mt.opsMgr.openFile(jsonFile);
		cur = mt.exec(inpTuple);
		
		Tuple expTuple = tf.newTuple();
		
		BagFactory bf = BagFactory.getInstance();
		DataBag expBag = bf.newDefaultBag();
		
		for (int i=0;i<288;i++) {
			Tuple subTuple = tf.newTuple();
			subTuple.append(0);
			expBag.add(subTuple);
		}
		
		expTuple.append("unicore6.TargetSystemFactory");
		expTuple.append("unicore.grid.task.gda.pl");
		expTuple.append("emi.unicore.UNICORE-Job");
		expTuple.append(expBag);
		
		assertTrue(expTuple.toString().equals(cur.toString()));
		
		
		
		
	}

}
