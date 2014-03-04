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
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    public static Map<String, Map.Entry<Integer, Integer>> getDowntimes(final String downtimesString, final int quantum) throws FileNotFoundException, IOException {
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

                int hour = Integer.parseInt(tokens[2].split("T", 2)[1].split(":")[0]);
                int minutes = Integer.parseInt(tokens[2].split("T", 2)[1].split(":")[1]);
                int timeGroupStart = (hour * 60 + minutes) / (24 * 60 / quantum);

                hour = Integer.parseInt(tokens[3].split("T", 2)[1].split(":")[0]);
                minutes = Integer.parseInt(tokens[3].split("T", 2)[1].split(":")[1]);
                int timeGroupEnd = (hour * 60 + minutes) / (24 * 60 / quantum);

                period = new AbstractMap.SimpleEntry<Integer, Integer>(timeGroupStart, timeGroupEnd);

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

    
    public static Map<String, List<Integer>> initHLPs(final String hlp) throws FileNotFoundException, IOException {
        Map<String, List<Integer>> hlps = new HashMap<String, List<Integer>>();

        byte[] decodedBytes = javax.xml.bind.DatatypeConverter.parseBase64Binary(hlp);
        BufferedReader in = new BufferedReader(new InputStreamReader(new GZIPInputStream(new ByteArrayInputStream(decodedBytes))));

        String input = in.readLine();

        StringTokenizer tokenizer = new StringTokenizer(input, "\\|");

        while (tokenizer.hasMoreTokens()) {
            String[] tokens = tokenizer.nextToken().split(":");
            // Input:
            //  [0]:hlpName, [1]:serviceFlavor, [2] groupID
            if (tokens.length > 2) {
                String key = tokens[0] + " " + tokens[1];
                Integer groupid = Integer.parseInt(tokens[2]);

                if (hlps.containsKey(key)) {
                    hlps.get(key).add(groupid);
                } else {
                    ArrayList<Integer> groupids = new ArrayList<Integer>();
                    groupids.add(groupid);
                    hlps.put(key, groupids);
                }
            }
        }
        
        return hlps;
    }

    public static Map<String, String> initWeights(final String weightsString) throws FileNotFoundException, IOException {
        Map<String, String> weights = new HashMap<String, String>();

        byte[] decodedBytes = javax.xml.bind.DatatypeConverter.parseBase64Binary(weightsString);
        BufferedReader in = new BufferedReader(new InputStreamReader(new GZIPInputStream(new ByteArrayInputStream(decodedBytes))));

        String input = in.readLine();

        StringTokenizer tokenizer = new StringTokenizer(input, "\\|");

        String host, w;
        while (tokenizer.hasMoreTokens()) {
            // (hostname, weight)
            String[] tokens = tokenizer.nextToken().split("\u0001", 2);
            if (tokens.length > 1) {
                host = tokens[0];
                w = tokens[1];

                weights.put(host, w);
            }
        }
        
        return weights;
    }

    public static Map<String, Map<String, Integer>> initHLPs(final String mongoHostname, final int port) throws FileNotFoundException, IOException {
        Map<String, Map<String, Integer>> hlps = new HashMap<String, Map<String, Integer>>();

        MongoClient mongoClient = new MongoClient(mongoHostname, port);
        DBCollection collection = mongoClient.getDB("AR").getCollection("hlps");

        DBObject fields = new BasicDBObject("sf", "$sf").append("g", "$g");
        DBObject groupFields = new BasicDBObject("_id", "$n");
        groupFields.put("rules", new BasicDBObject("$push", fields));
        DBObject group = new BasicDBObject("$group", groupFields);

        AggregationOutput output = collection.aggregate(group);

        for (DBObject dbo : output.results()) {
            Map<String, Integer> hlpsRules = new HashMap<String, Integer>();
            hlps.put((String) dbo.get("_id"), hlpsRules);

            BasicDBList l = (BasicDBList) dbo.get("rules");

            for (Object o : l) {
                DBObject dbor = (BasicDBObject) o;
                String service_flavor = (String) dbor.get("sf");
                Integer groupid = ((Number) dbor.get("g")).intValue();

                hlpsRules.put(service_flavor, groupid);
            }
        }
        
        mongoClient.close();
        return hlps;
    }
    
    public static Map<String, DataBag> getSFtoAvailabilityProfileNames(final String mongoHostname, final int port) throws UnknownHostException {
        Map<String, DataBag> sf_to_apnames = new HashMap<String, DataBag>(10);
        BagFactory mBagFactory = BagFactory.getInstance();
        TupleFactory mTupleFactory = TupleFactory.getInstance();
        
        MongoClient mongoClient = new MongoClient(mongoHostname, port);
        DBCollection collection = mongoClient.getDB("AR").getCollection("hlps");

        
        DBObject groupFields = new BasicDBObject("_id", "$sf");
        groupFields.put("profs", new BasicDBObject("$push", "$n"));
        DBObject group = new BasicDBObject("$group", groupFields);

        AggregationOutput output = collection.aggregate(group);
        
        // For each service flavor
        for (DBObject dbo : output.results()) {
            DataBag db = mBagFactory.newDefaultBag();
            
            BasicDBList l = (BasicDBList) dbo.get("profs");
            
            // For each availability profile name
            for (Object o : l) {
                String apname = (String) o;
                db.add(mTupleFactory.newTuple(apname));
            }
            
            sf_to_apnames.put((String) dbo.get("_id"), db);
        }
        
        mongoClient.close();
        return sf_to_apnames;
    }
    
}
