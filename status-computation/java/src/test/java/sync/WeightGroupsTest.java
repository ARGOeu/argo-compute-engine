package sync;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import org.junit.BeforeClass;
import org.junit.Test;

public class WeightGroupsTest {

	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", MetricProfilesTest.class.getResource("/avro/weights_sync_test.avro"));
	}
	
	@Test
	public void test() throws IOException, URISyntaxException {
		//Prepare Resource File
		URL resAvroFile = MetricProfilesTest.class.getResource("/avro/weights_sync_test.avro");
		File avroFile = new File(resAvroFile.toURI());
		// Instatiate class
		WeightGroups wg = new WeightGroups();
		// Test loading file
		wg.loadAvro(avroFile);
		assertNotNull("File Loaded",wg);
		
		// Test factor retrieval for various sites
		
		assertEquals("Factor for GR-06-IASA",wg.getWeight("hepspec", "GR-06-IASA"),866);
		assertEquals("Factor for UNI-DORTMUND",wg.getWeight("hepspec", "UNI-DORTMUND"),16000);
		assertEquals("Factor for INFN-COSENZA",wg.getWeight("hepspec", "INFN-COSENZA"),2006);
		assertEquals("Factor for CA-ALBERTA-WESTGRID-T2",wg.getWeight("hepspec", "CA-ALBERTA-WESTGRID-T2"),9720);
		
	}

}
