package sync;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;

import org.junit.BeforeClass;
import org.junit.Test;

public class DowntimesTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", MetricProfilesTest.class.getResource("/avro/downtimes_test.avro"));
	}
	
	@Test
	public void test() throws IOException, URISyntaxException {
		//Prepare Resource File
		URL resAvroFile = MetricProfilesTest.class.getResource("/avro/downtimes_test.avro");
		File avroFile = new File(resAvroFile.toURI());
		// Instatiate class
		Downtimes dt = new Downtimes();
		// Test loading file
		dt.loadAvro(avroFile);
		assertNotNull("File Loaded",dt);
		
		// Test time period retrieval by service endpoint
		// test for lcg-se3.scinet.utoronto.ca, Site-BDII
		ArrayList<String> timePeriod = new ArrayList<String>();
		timePeriod.add("2015-01-14T11:00:00Z");
		timePeriod.add("2015-01-14T23:59:00Z");
		assertEquals("Test timeperiod #1",dt.getPeriod("lcg-se3.scinet.utoronto.ca", "Site-BDII"),timePeriod);
		// test for arc.imbg.org.ua, APEL
		timePeriod.clear();
		timePeriod.add("2015-01-14T00:00:00Z");
		timePeriod.add("2015-01-14T23:59:00Z");
		assertEquals("Test timeperiod #2",dt.getPeriod("arc.imbg.org.ua", "APEL"),timePeriod);
		// test for atlas-ui-03.roma1.infn.it, UI
		timePeriod.clear();
		timePeriod.add("2015-01-14T07:00:00Z");
		timePeriod.add("2015-01-14T21:00:00Z");
		assertEquals("Test timeperiod #2",dt.getPeriod("atlas-ui-03.roma1.infn.it", "UI"),timePeriod);
				
		   
	}

}
