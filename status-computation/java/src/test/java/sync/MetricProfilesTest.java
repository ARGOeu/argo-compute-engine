package sync;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import org.junit.BeforeClass;
import org.junit.Test;



public class MetricProfilesTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", MetricProfilesTest.class.getResource("/avro/poem_sync_test.avro"));
	}
	
	@Test
	public void test() throws URISyntaxException, IOException {
		
		//Load file 
		URL resAvroFile = MetricProfilesTest.class.getResource("/avro/poem_sync_test.avro");
		File avroFile = new File(resAvroFile.toURI());
		MetricProfiles mp = new MetricProfiles();
		mp.loadAvro(avroFile);
		assertNotNull("File Loaded",mp);
		assertEquals("Only one metric profile must be loaded",mp.getProfiles().size(),1);
		assertEquals("Profile ch.cern.sam.roc_critical must be loaded",mp.getProfiles().get(0).toString(),"CH.CERN.SAM.ROC_CRITICAL");
		
	
		
		
	}

}
