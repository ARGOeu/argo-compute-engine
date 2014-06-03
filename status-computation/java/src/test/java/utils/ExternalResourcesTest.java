package utils;

import static org.junit.Assert.*;

import org.apache.commons.io.IOUtils;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import java.util.logging.Handler;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import org.json.JSONException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import TestIO.JsonToPig;

import com.google.gson.Gson;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.RuntimeConfigBuilder;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.config.io.ProcessOutput;
import de.flapdoodle.embed.process.runtime.Network;

public class ExternalResourcesTest {

	private static Gson gson; 
	private static MongodStarter starter = MongodStarter.getDefaultInstance();
	private static MongodExecutable mongodExe;
	private static MongodProcess mongod;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", ExternalResourcesTest.class.getResource("/mongodata/aps.cloud_monitor.json"));
		assertNotNull("Test file missing", ExternalResourcesTest.class.getResource("/mongodata/aps.roc_critical.json"));
		assertNotNull("Test file missing", ExternalResourcesTest.class.getResource("/mongodata/recalculations.json"));
		
		// Initialize global Gson object, usefull for json manipulation of input data
		gson = new Gson();
		// Handle Logging (Generated mostly from Embedded mongoDB
		Logger globalLogger = Logger.getLogger("global");
		Handler[] handlers = globalLogger.getHandlers();
		for(Handler handler : handlers) {
		    globalLogger.removeHandler(handler);
		}
		// Prepare embedded mongoDB env.
	    IRuntimeConfig runtimeConfig = new RuntimeConfigBuilder()
	        .defaultsWithLogger(Command.MongoD, globalLogger)
	        .processOutput(ProcessOutput.getDefaultInstanceSilent())
	        .build();
	  
		starter = MongodStarter.getInstance(runtimeConfig);
		// Assign port 12345 to emb. server
		mongodExe = starter.prepare(new MongodConfigBuilder()
        .version(Version.Main.PRODUCTION)
        .net(new Net(12345, Network.localhostIsIPv6()))
        .build());
		
		//Flipwitch to stop embedded mongo logging
		//LogManager.getLogManager().reset();
		
		// Start Embedded mongoDB server
		mongod = mongodExe.start();
		// Read mongo data from json files
		
		String  rocCritJson = IOUtils.toString(ExternalResourcesTest.class.getResourceAsStream("/mongodata/aps.cloud_monitor.json"),"UTF-8");
		String  cloudMonJson = IOUtils.toString(ExternalResourcesTest.class.getResourceAsStream("/mongodata/aps.roc_critical.json"),"UTF-8");
		String  recalcJson = IOUtils.toString(ExternalResourcesTest.class.getResourceAsStream("/mongodata/recalculations.json"),"UTF-8");
		
		// Use MongoClient to connect and populate data
		MongoClient mongoClient = new MongoClient("localhost", 12345);
		// Get DB
		DB db = mongoClient.getDB( "AR" );
	    // Get aps and recalculation collections
		DBCollection apsCol = db.getCollection("aps");
	    DBCollection recalcCol = db.getCollection("recalculations");
	    // Build instantly db objects from json data
	    DBObject rocCritData = (DBObject)JSON.parse(rocCritJson);
	    DBObject cloudMonData = (DBObject)JSON.parse(cloudMonJson);
	    DBObject recalcData = (DBObject)JSON.parse(recalcJson);   
	    // Insert Objects to collections
	    apsCol.insert(rocCritData);
	    apsCol.insert(cloudMonData);
	    recalcCol.insert(recalcData);
	    
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		// If started stop server
		if (mongod != null) mongod.stop();
		if (mongodExe != null) mongodExe.stop();
	}
	
	@Test
	public void testGetDowntimes() throws IOException, JSONException {
		Map<String, Map.Entry<Integer, Integer>> result = new HashMap<String, Map.Entry<Integer, Integer>>();
		String downtimesString = IOUtils.toString(this.getClass().getResourceAsStream("/sync/downtimes.in.gz.base64"),"UTF-8");
		String resultString = IOUtils.toString(this.getClass().getResourceAsStream("/sync/downtimes.out.json"),"UTF-8");
		int quantum = 288;
		
		result = ExternalResources.getDowntimes(downtimesString, quantum);
		 
		String r_to_json = gson.toJson(result); 
		JSONAssert.assertEquals(resultString, r_to_json, true);
	}

	@Test
	public void testInitPOEMs() throws IOException, JSONException {
		Map<String, ArrayList<String>> result = new HashMap<String, ArrayList<String>>();
		String poemString = IOUtils.toString(this.getClass().getResourceAsStream("/sync/poems.in.gz.base64"),"UTF-8");
		String resultString = IOUtils.toString(this.getClass().getResourceAsStream("/sync/poems.out.json"),"UTF-8");
				
		result = ExternalResources.initPOEMs(poemString);
		
		String r_to_json = gson.toJson(result); 
		JSONAssert.assertEquals(resultString, r_to_json, true);
	}

	@Test
	public void testInitWeights() throws IOException, JSONException {
		Map<String, Integer> result = new HashMap<String, Integer>();
		String weightString = IOUtils.toString(this.getClass().getResourceAsStream("/sync/weights.in.gz.base64"),"UTF-8");
		String resultString = IOUtils.toString(this.getClass().getResourceAsStream("/sync/weights.out.json"),"UTF-8");
	
		result = ExternalResources.initWeights(weightString);
		
		String r_to_json = gson.toJson(result); 
		JSONAssert.assertEquals(resultString, r_to_json, false);
	}

	@Test
	public void testInitAPs() throws FileNotFoundException, IOException, JSONException {
		Map<String, Map<String, Integer>> result = new HashMap<String, Map<String, Integer>>();
		String resultString = IOUtils.toString(this.getClass().getResourceAsStream("/mongodata/get_aps.json"),"UTF-8");
		result = ExternalResources.initAPs("localhost", 12345);
	
		String r_to_json = gson.toJson(result);
		JSONAssert.assertEquals(resultString,r_to_json,false);
	}

	@Test
	public void testGetSFtoAvailabilityProfileNames() throws IOException, JSONException  {
		Map<String, Map <String,DataBag>> result = new HashMap<String, Map <String, DataBag>>(10);
		result = ExternalResources.getSFtoAvailabilityProfileNames("localhost", 12345);
		String r_to_json = gson.toJson(result);
		String resultString = IOUtils.toString(this.getClass().getResourceAsStream("/mongodata/get_sf_to_aps.json"),"UTF-8");
	
		
		
		JSONAssert.assertEquals(resultString, r_to_json, false);
	}

	@Test
	public void testGetRecalculationRequests() throws UnknownHostException, IOException, JSONException {
		Map<String, Map<String, Object>> result = new HashMap<String, Map<String, Object>>(10);
		result = ExternalResources.getRecalculationRequests("localhost", 12345, 20131209, 288);
		String resultString = IOUtils.toString(this.getClass().getResourceAsStream("/mongodata/get_recalc_requests.json"),"UTF-8");
		
		String r_to_json = gson.toJson(result); 
		
		JSONAssert.assertEquals(resultString, r_to_json, false);
	}

}
