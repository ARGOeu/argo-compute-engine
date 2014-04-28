package utils;




import static org.junit.Assert.*;

import org.apache.commons.io.IOUtils;
import org.apache.pig.data.DataBag;



import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.ConsoleHandler;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import org.json.JSONException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

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

public class ExteralResourcesTest {

	private Gson gson; 
	private MongodStarter starter = MongodStarter.getDefaultInstance();
	private MongodExecutable mongodExe;
	private MongodProcess mongod;
	
	
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		gson = new Gson();
		
		Logger globalLogger = Logger.getLogger("global");
		Handler[] handlers = globalLogger.getHandlers();
		for(Handler handler : handlers) {
		    globalLogger.removeHandler(handler);
		}
		
		

	    IRuntimeConfig runtimeConfig = new RuntimeConfigBuilder()
	        .defaultsWithLogger(Command.MongoD, globalLogger)
	        .processOutput(ProcessOutput.getDefaultInstanceSilent())
	        .build();

	    

		starter = MongodStarter.getInstance(runtimeConfig);
		
		
		
		//Initialize Mongo Server
		mongodExe = starter.prepare(new MongodConfigBuilder()
        .version(Version.Main.PRODUCTION)
        .net(new Net(12345, Network.localhostIsIPv6()))
        .build());

		LogManager.getLogManager().reset();
		mongod = mongodExe.start();
		
		
		
		
		// Add Data
		assertNotNull("Test file missing", getClass().getResource("/mongodata/mongo.aps.json"));
		assertNotNull("Test file missing", getClass().getResource("/mongodata/mongo.recalc.json"));
		
		String  mongo_aps_data = IOUtils.toString(this.getClass().getResourceAsStream("/mongodata/mongo.aps.json"),"UTF-8");
		String  mongo_recalc_data = IOUtils.toString(this.getClass().getResourceAsStream("/mongodata/mongo.recalc.json"),"UTF-8");
		
		MongoClient mongoClient = new MongoClient("localhost", 12345);
		
		DB db = mongoClient.getDB( "AR" );
	    DBCollection aps_col = db.getCollection("aps");
	    DBCollection recalc_col = db.getCollection("recalculations");
	    DBObject aps_data = (DBObject)JSON.parse(mongo_aps_data);
	    DBObject recalc_data = (DBObject)JSON.parse(mongo_recalc_data);   
	    aps_col.insert(aps_data);
	    recalc_col.insert(recalc_data);
	    
	    
		
		
	}

	@After
	public void tearDown() throws Exception {
		if (mongod != null) mongod.stop();
		if (mongodExe != null) mongodExe.stop();
		
		
	}

	
	@Test
	public void testResultFilesToString() {
	   assertNotNull("Test file missing", getClass().getResource("/poems/poemsIn.txt"));
	   assertNotNull("Test file missing", getClass().getResource("/poems/poemsOut.txt"));
	   assertNotNull("Test file missing", getClass().getResource("/downtimes/downtimesIn.txt"));
	   assertNotNull("Test file missing", getClass().getResource("/downtimes/downtimesOut.txt"));
	   assertNotNull("Test file missing", getClass().getResource("/weights/weightsIn.txt"));
	   assertNotNull("Test file missing", getClass().getResource("/weights/weightsOut.txt"));
	   assertNotNull("Test file missing", getClass().getResource("/aps/apsOut.txt"));
	   assertNotNull("Test file missing", getClass().getResource("/recalc/recalcOut.txt"));
	   assertNotNull("Test file missing", getClass().getResource("/aps/sf2aps.txt"));
	   assertNotNull("Test file missing", getClass().getResource("/mongodata/mongo.aps.json"));
	   assertNotNull("Test file missing", getClass().getResource("/mongodata/mongo.recalc.json"));
	}
	
	
	@Test
	public void testGetDowntimes() throws IOException, JSONException {
		Map<String, Map.Entry<Integer, Integer>> result = new HashMap<String, Map.Entry<Integer, Integer>>();
		String downtimesString = IOUtils.toString(this.getClass().getResourceAsStream("/downtimes/downtimesIn.txt"),"UTF-8");
		String resultString = IOUtils.toString(this.getClass().getResourceAsStream("/downtimes/downtimesOut.txt"),"UTF-8");
		int quantum = 288;
		result = ExternalResources.getDowntimes(downtimesString, quantum);
		
		 
		String r_to_json = gson.toJson(result); 
		
		JSONAssert.assertEquals(resultString, r_to_json, false);
	}
	
	
	
	

	@Test
	public void testInitPOEMs() throws IOException, JSONException {
		Map<String, ArrayList<String>> result = new HashMap<String, ArrayList<String>>();
		String poemString = IOUtils.toString(this.getClass().getResourceAsStream("/poems/poemsIn.txt"),"UTF-8");
		String resultString = IOUtils.toString(this.getClass().getResourceAsStream("/poems/poemsOut.txt"),"UTF-8");
				
		result = ExternalResources.initPOEMs(poemString);
		
		
		
		
		String r_to_json = gson.toJson(result); 
		
		JSONAssert.assertEquals(resultString, r_to_json, false);
		
	}

	@Test
	public void testInitWeights() throws IOException, JSONException {
		Map<String, String> result = new HashMap<String, String>();
		String weightString = IOUtils.toString(this.getClass().getResourceAsStream("/weights/weightsIn.txt"),"UTF-8");
		String resultString = IOUtils.toString(this.getClass().getResourceAsStream("/weights/weightsOut.txt"),"UTF-8");
	
		result = ExternalResources.initWeights(weightString);
		
		
		String r_to_json = gson.toJson(result); 
		
		JSONAssert.assertEquals(resultString, r_to_json, false);
	
	}

	@Test
	public void testInitAPs() throws FileNotFoundException, IOException, JSONException {
		Map<String, Map<String, Integer>> result = new HashMap<String, Map<String, Integer>>();
		String resultString = IOUtils.toString(this.getClass().getResourceAsStream("/aps/apsOut.txt"),"UTF-8");
		result = ExternalResources.initAPs("localhost", 12345);
	
		String r_to_json = gson.toJson(result); 
		
		JSONAssert.assertEquals(resultString,r_to_json,false);
		
	}

	@Test
	public void testGetSFtoAvailabilityProfileNames() throws IOException, JSONException  {
		Map<String, Map <String,DataBag>> result = new HashMap<String, Map <String, DataBag>>(10);
		result = ExternalResources.getSFtoAvailabilityProfileNames("localhost", 12345);
		String r_to_json = gson.toJson(result);
		String resultString = IOUtils.toString(this.getClass().getResourceAsStream("/aps/sf2aps.txt"),"UTF-8");
		
		JSONAssert.assertEquals(resultString, r_to_json, false);
	}

	@Test
	public void testGetRecalculationRequests() throws UnknownHostException, IOException, JSONException {
		Map<String, Map<String, Object>> result = new HashMap<String, Map<String, Object>>(10);
		result = ExternalResources.getRecalculationRequests("localhost", 12345, 20131209, 288);
		String resultString = IOUtils.toString(this.getClass().getResourceAsStream("/recalc/recalcOut.txt"),"UTF-8");
		
		
		String r_to_json = gson.toJson(result); 
		
		JSONAssert.assertEquals(resultString, r_to_json, false);
	}

}
