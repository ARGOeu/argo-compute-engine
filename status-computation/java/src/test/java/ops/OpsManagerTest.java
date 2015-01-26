package ops;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;

import org.junit.BeforeClass;
import org.junit.Test;

import sync.EndpointGroups;
import sync.EndpointGroupsTest;
import sync.MetricProfilesTest;

public class OpsManagerTest {
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", OpsManagerTest.class.getResource("/ops/EGI-algorithm.json"));
	}
	
	
	

	@Test
	public void test() throws URISyntaxException, FileNotFoundException {
		//Prepare Resource File
		URL resJsonFile = OpsManagerTest.class.getResource("/ops/EGI-algorithm.json");
		File JsonFile = new File(resJsonFile.toURI());
		// Instatiate class
		OpsManager opsMgr = new OpsManager();
		// Test loading file
		opsMgr.openFile(JsonFile);
	}

}
