package sync;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import org.junit.BeforeClass;
import org.junit.Test;

public class EndpointGroupsTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", EndpointGroupsTest.class.getResource("/avro/group_endpoints_test.avro"));
	}
	
	@Test
	public void test() throws URISyntaxException, IOException {
		//Prepare Resource File
		URL resAvroFile = EndpointGroupsTest.class.getResource("/avro/group_endpoints_test.avro");
		File avroFile = new File(resAvroFile.toURI());
		// Instatiate class
		EndpointGroups ge = new EndpointGroups();
		// Test loading file
		ge.loadAvro(avroFile);
		assertNotNull("File Loaded",ge);
	}

}
