package ops;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;

import org.junit.BeforeClass;
import org.junit.Test;

public class ConfigManagerTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", ConfigManagerTest.class.getResource("/ops/config.json"));
	}
	
	@Test
	public void test() throws URISyntaxException, FileNotFoundException {
		// Load the resource file
		URL resJsonFile = OpsManagerTest.class.getResource("/ops/config.json");
		File jsonFile = new File(resJsonFile.toURI());
		
		// Instantiate a new ConfigManager and load the test file
		ConfigManager cfgMgr = new ConfigManager();
		cfgMgr.loadJson(jsonFile);
		
		// Assert that the simple fields are loaded correctly
		assertEquals("EGI", cfgMgr.tenant);
		assertEquals("Critical", cfgMgr.job);
		assertEquals("SITES", cfgMgr.egroup);
		assertEquals("NGI", cfgMgr.ggroup);
		assertEquals("hepspec",cfgMgr.weight);
		
		// Assert compound fields
		assertEquals("Production",cfgMgr.ggroupTags.get("infrastructure"));
		assertEquals("Certified",cfgMgr.ggroupTags.get("certification"));
		assertEquals("EGI",cfgMgr.ggroupTags.get("scope"));
		
		// Assert compound fields
		assertEquals("1",cfgMgr.egroupTags.get("production"));
		assertEquals("1",cfgMgr.egroupTags.get("monitored"));
		assertEquals("EGI",cfgMgr.egroupTags.get("scope"));
	
		System.out.println(cfgMgr.getMapped("ar", "reliability"));
		System.out.println(cfgMgr.getMapped("ar","date"));
		System.out.println(cfgMgr.getMapped("ar","av_profile"));
		System.out.println(cfgMgr.getMapped("ar","metric_profile"));
		System.out.println(cfgMgr.getMapped("ar","group"));
		System.out.println(cfgMgr.getMapped("ar","supergroup"));
		System.out.println(cfgMgr.getMapped("ar","weight"));
		
	}

}
