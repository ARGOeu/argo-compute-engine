package utils;




import static org.junit.Assert.*;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import com.google.gson.Gson;

public class ExteralResourcesTest {

	
	
	
	
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
	public void testResultFilesToString() {
	   assertNotNull("Test file missing", getClass().getResource("/poems/poemsIn.txt"));
	   assertNotNull("Test file missing", getClass().getResource("/poems/poemsOut.txt"));
	   assertNotNull("Test file missing", getClass().getResource("/downtimes/downtimesIn.txt"));
	   assertNotNull("Test file missing", getClass().getResource("/downtimes/downtimesOut.txt"));
	}
	
	
	@Test
	public void testGetDowntimes() throws IOException, JSONException {
		Map<String, Map.Entry<Integer, Integer>> result = new HashMap<String, Map.Entry<Integer, Integer>>();
		String downtimesString = IOUtils.toString(this.getClass().getResourceAsStream("/downtimes/downtimesIn.txt"),"UTF-8");
		String resultString = IOUtils.toString(this.getClass().getResourceAsStream("/downtimes/downtimesOut.txt"),"UTF-8");
		int quantum = 288;
		result = ExternalResources.getDowntimes(downtimesString, quantum);
		
		Gson gson = new Gson(); 
		String r_to_json = gson.toJson(result); 
		
		JSONAssert.assertEquals(resultString, r_to_json, false);
	}
	
	
	
	

	@Test
	public void testInitPOEMs() throws IOException, JSONException {
		Map<String, ArrayList<String>> result = new HashMap<String, ArrayList<String>>();
		String poemString = IOUtils.toString(this.getClass().getResourceAsStream("/poems/poemsIn.txt"),"UTF-8");
		String resultString = IOUtils.toString(this.getClass().getResourceAsStream("/poems/poemsOut.txt"),"UTF-8");
				
		result = ExternalResources.initPOEMs(poemString);
		
		
		
		Gson gson = new Gson(); 
		String r_to_json = gson.toJson(result); 
		
		JSONAssert.assertEquals(resultString, r_to_json, false);
		
	}

	@Test
	public void testInitWeights() throws IOException, JSONException {
		Map<String, String> result = new HashMap<String, String>();
		String weightString = IOUtils.toString(this.getClass().getResourceAsStream("/weights/weightsIn.txt"),"UTF-8");
		String resultString = IOUtils.toString(this.getClass().getResourceAsStream("/weights/weightsOut.txt"),"UTF-8");
	
		result = ExternalResources.initWeights(weightString);
		
		Gson gson = new Gson(); 
		String r_to_json = gson.toJson(result); 
		
		JSONAssert.assertEquals(resultString, r_to_json, false);
	
	}

	@Ignore
	public void testInitAPs() {
		fail("Not yet implemented");
	}

	@Ignore
	public void testGetSFtoAvailabilityProfileNames() {
		fail("Not yet implemented");
	}

	@Ignore
	public void testGetRecalculationRequests() {
		fail("Not yet implemented");
	}

}
