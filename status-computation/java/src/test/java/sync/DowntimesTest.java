package sync;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

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
	}

}
