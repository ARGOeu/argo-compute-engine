/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package utils;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.zip.GZIPInputStream;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.TupleFactory;

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class ExternalResources {
    
    /**
     *
     * @param downtimesString
     * @param quantum
     * @return 
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static Map<String, Entry<Integer, Integer>> getDowntimes(final String downtimesString, final int quantum) throws FileNotFoundException, IOException {
        Map<String, Map.Entry<Integer, Integer>> downtimes = new HashMap<String, Map.Entry<Integer, Integer>>();

        byte[] decodedBytes = javax.xml.bind.DatatypeConverter.parseBase64Binary(downtimesString);
        BufferedReader in = new BufferedReader(new InputStreamReader(new GZIPInputStream(new ByteArrayInputStream(decodedBytes))));

        String input = in.readLine();

        StringTokenizer tokenizer = new StringTokenizer(input, "\\|");

        String host, serviceFlavor;
        Map.Entry<Integer, Integer> period;
        while (tokenizer.hasMoreTokens()) {
            String[] tokens = tokenizer.nextToken().split("\u0001");
            if (tokens.length > 2) {
                host = tokens[0];
                serviceFlavor = tokens[1];

                String startTimeStamp = tokens[2].split("T", 2)[1];
                String endTimeStamp = tokens[3].split("T", 2)[1];

                int startGroup = Utils.getTimeGroup(startTimeStamp, quantum);
                int endGroup = Utils.getTimeGroup(endTimeStamp, quantum);

                period = new SimpleEntry<Integer, Integer>(startGroup, endGroup);

                downtimes.put(host + " " + serviceFlavor, period);
            }
        }
        
        return downtimes;
    }
    
    public static Map<String, ArrayList<String>> initPOEMs(final String poemsString) throws FileNotFoundException, IOException {
        Map<String, ArrayList<String>> poems = new HashMap<String, ArrayList<String>>();

        byte[] decodedBytes = javax.xml.bind.DatatypeConverter.parseBase64Binary(poemsString);
        BufferedReader in = new BufferedReader(new InputStreamReader(new GZIPInputStream(new ByteArrayInputStream(decodedBytes))));

        String input = in.readLine();

        StringTokenizer tokenizer = new StringTokenizer(input, "\\|");

        while (tokenizer.hasMoreTokens()) {
            String[] tokens = tokenizer.nextToken().split("\u0001");
            // Input:
            //  [0]:profile_name, [1]:service_flavor, [2] metric
            if (tokens.length > 2) {
                String key = tokens[0] + " " + tokens[1];
                String metric = tokens[2];

                if (poems.containsKey(key)) {
                    poems.get(key).add(metric);
                } else {
                    ArrayList<String> metrics = new ArrayList<String>();
                    metrics.add(metric);
                    poems.put(key, metrics);
                }
            }
        }
        
        return poems;
    }

    public static Map<String, Integer> initWeights(final String weightsString) throws FileNotFoundException, IOException {
        Map<String, Integer> weights = new HashMap<String, Integer>();

        byte[] decodedBytes = javax.xml.bind.DatatypeConverter.parseBase64Binary(weightsString);
        BufferedReader in = new BufferedReader(new InputStreamReader(new GZIPInputStream(new ByteArrayInputStream(decodedBytes))));

        String input = in.readLine();

        StringTokenizer tokenizer = new StringTokenizer(input, "\\|");

        String host;
        Integer w;
        while (tokenizer.hasMoreTokens()) {
            // (hostname, weight)
            String[] tokens = tokenizer.nextToken().split("\u0001", 2);
            if (tokens.length > 1) {
                host = tokens[0];
                w = Integer.parseInt(tokens[1]);

                weights.put(host, w);
            }
        }
        
        return weights;
    }

    public static Map<String, Map<String, Integer>> initAPs(final String mongoHostname, final int port) throws FileNotFoundException, IOException {
        Map<String, Map<String, Integer>> aps = new HashMap<String, Map<String, Integer>>();

        MongoClient mongoClient = new MongoClient(mongoHostname, port);
        DBCollection collection = mongoClient.getDB("AR").getCollection("aps");

        // we need to merge namespace with name
        // {$project : { name : {$concat: ["$namespace", "-", "$name"]}, groups:1}}
        BasicDBList concatArgs = new BasicDBList();
        concatArgs.add("$namespace");
        concatArgs.add("-");
        concatArgs.add("$name");
        DBObject project = new BasicDBObject("$project", new BasicDBObject("name", new BasicDBObject("$concat", concatArgs)).append("groups", 1).append("poems", 1));
        
        AggregationOutput output = collection.aggregate(project);
        
        // For each AP
        for (DBObject dbo : output.results()) {
            Map<String, Integer> sfGroup = new HashMap<String, Integer>();
            aps.put((String) dbo.get("name"), sfGroup);

            BasicDBList l = (BasicDBList) dbo.get("groups");

            int groupID = 0;
            // For each group
            for (Object o : l) {
                BasicDBList dbl = (BasicDBList) o;
                
                // For each service flavour
                for (Object sf : dbl) {
                    sfGroup.put((String) sf, groupID);
                }
                
                groupID++;
            }
        }
        
        mongoClient.close();
        return aps;
    }
    
    // We need to create a Pig DataBag full of the possible AP names in tuples.
    // The reason we do that is for better performance. We need to create the DataBag
    // only once, and then for each input row we just point out the appropriate DataBag.
    // After the step of the topology, Pig will take the DataBag and for each tuple inside
    // will create a row. Thus, each host timeline will be calculated in the apropriate group.
    public static Map<String, Map <String, DataBag>> getSFtoAvailabilityProfileNames(final String mongoHostname, final int port) throws UnknownHostException {
        Map<String, Map <String, DataBag>> poemMap = new HashMap<String, Map <String, DataBag>>(10);
        BagFactory mBagFactory = BagFactory.getInstance();
        TupleFactory mTupleFactory = TupleFactory.getInstance();
        
        MongoClient mongoClient = new MongoClient(mongoHostname, port);
        DBCollection collection = mongoClient.getDB("AR").getCollection("aps");
        
        // We need to implement this query to get the unique service flavors 
        // for each AP
        // {$project: { name : {$concat : ["$namespace", "-", "$name"]}, groups : 1, poems : 1 }}
        // { $unwind : "$poems" }, {$unwind : "$groups"}, {$unwind : "$groups"},
        // { $group : { _id : {poem : "$poems", sf : "$groups" }, aps : {$addToSet : "$name"}}},
        // { $group : { _id : {poem : "$_id.poem"}, sfs : {$addToSet: { sf : "$_id.sf", aps : "$aps" }}}}
        BasicDBList concatArgs = new BasicDBList();
        concatArgs.add("$namespace");
        concatArgs.add("-");
        concatArgs.add("$name");
        DBObject project = new BasicDBObject("$project", new BasicDBObject("name", new BasicDBObject("$concat", concatArgs)).append("groups", 1).append("poems", 1));
        DBObject unwindPoems = new BasicDBObject("$unwind", "$poems");
        DBObject unwindGroups = new BasicDBObject("$unwind", "$groups");
        DBObject group = new BasicDBObject("$group",
                new BasicDBObject("_id", new BasicDBObject("poem", "$poems").append("sf", "$groups"))
                        .append("aps", new BasicDBObject("$addToSet", "$name")));
        DBObject group2 = new BasicDBObject("$group",
                new BasicDBObject("_id", new BasicDBObject("poem", "$_id.poem"))
                        .append("sfs", new BasicDBObject("$addToSet", new BasicDBObject("sf", "$_id.sf").append("aps", "$aps"))));
        
        AggregationOutput output = collection.aggregate(project, unwindPoems, unwindGroups, unwindGroups, group, group2);
        
        // For each poem profile
        for (DBObject dbo : output.results()) {
            BasicDBList l = (BasicDBList) dbo.get("sfs");
            String poemProfile = (String) ((DBObject) dbo.get("_id")).get("poem");
            
            Map<String, DataBag> sfMap = new HashMap<String, DataBag>(10);
            // For each service flavour
            for (Object o : l) {
                DBObject sfs = (DBObject) o;
                
                String serviceFlaver = (String) sfs.get("sf");
                BasicDBList apList = (BasicDBList) sfs.get("aps");
                
                DataBag apBag = mBagFactory.newDefaultBag();
                // For each AP
                for (Object ap : apList) {
                    apBag.add(mTupleFactory.newTuple((String) ap));
                }
                
                sfMap.put(serviceFlaver, apBag);
            }
            
            poemMap.put(poemProfile, sfMap);
        }
        
        mongoClient.close();
        return poemMap;
    }
    
    public static Map<String, Map<String, Object>> getRecalculationRequests(final String mongoHostname, final int port, final int date, final int quantum) throws UnknownHostException, IOException {
        Map<String, Map<String, Object>> recalcMap = new HashMap<String, Map<String, Object>>(10);
        
        MongoClient mongoClient = new MongoClient(mongoHostname, port);
        DBCollection collection = mongoClient.getDB("AR").getCollection("recalculations");
        
        // We need to take all recalculatios that include the date we calculate.
        DBCursor cursor = collection.find(new BasicDBObject("$where", 
            String.format("'%s' <= this.et.split('T')[0].replace(/-/g,'') || '%s' >= this.st.split('T')[0].replace(/-/g,'')", date, date)));

        for (DBObject dbo : cursor) {
            String ngi = (String) dbo.get("n");
            int size = ((BasicDBList) dbo.get("es")).size();
            String[] excludedSites = ((BasicDBList) dbo.get("es")).toArray(new String[size]);
            
            int startGroup = Utils.determineTimeGroup((String) dbo.get("st"), date, quantum);
            int endGroup = Utils.determineTimeGroup((String) dbo.get("et"), date, quantum);
            
            // data object is Entry<Integer, Integer>
            // exclude object is String[]
            Map<String, Object> hmap = new HashMap<String, Object>(5);
            hmap.put("data", new SimpleEntry<Integer, Integer>(startGroup, endGroup));
            hmap.put("exclude", excludedSites);
            recalcMap.put(ngi, hmap);
        }
        
        mongoClient.close();
        return recalcMap;
    }
}
