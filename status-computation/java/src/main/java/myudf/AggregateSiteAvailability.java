package myudf;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import utils.State;
import utils.Utils;

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class AggregateSiteAvailability extends EvalFunc<Tuple> {
    
    private final double quantum = 288.0;
    private final TupleFactory mTupleFactory = TupleFactory.getInstance();
    private Map<String, String> weights = null;
    
    private State[] output_table = null;
    private Map<Integer, State[]> ultimate_kickass_table = null;
    
    private Map<String, Integer> highLevelProfiles = null;
    
    private final Integer nGroups = 3;
    private Map<String, List<Integer>> hlps = null;
    
    private void initHLPs(final String hlp) throws FileNotFoundException, IOException {
        this.hlps = new HashMap<String, List<Integer>>();

        byte[] decodedBytes = javax.xml.bind.DatatypeConverter.parseBase64Binary(hlp);
        BufferedReader in = new BufferedReader(new InputStreamReader(new GZIPInputStream(new ByteArrayInputStream(decodedBytes))));

        String input = in.readLine();

        StringTokenizer tokenizer = new StringTokenizer(input, "\\|");

        while (tokenizer.hasMoreTokens()) {
            String[] tokens = tokenizer.nextToken().split("\u0001");
            // Input:
            //  [0]:hlpName, [1]:serviceFlavor, [2] groupID
            if (tokens.length > 2) {
                String key = tokens[0] + " " + tokens[1];
                Integer groupid = Integer.parseInt(tokens[2]);

                if (this.hlps.containsKey(key)) {
                    this.hlps.get(key).add(groupid);
                } else {
                    ArrayList<Integer> groupids = new ArrayList<Integer>();
                    groupids.add(groupid);
                    this.hlps.put(key, groupids);
                }
            }
        }
    }

    private void initWeights(final String weights) throws FileNotFoundException, IOException {

        this.weights = new HashMap<String, String>();

        byte[] decodedBytes = javax.xml.bind.DatatypeConverter.parseBase64Binary(weights);
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

                this.weights.put(host, w);
            }
        }
    }

    private void getHighLevelProfiles() throws FileNotFoundException, IOException {
        this.highLevelProfiles = new HashMap<String, Integer>();
//        
//        FileReader fr = new FileReader("./highlevelprofiles.txt");
//        BufferedReader d = new BufferedReader(fr);
//        String line = d.readLine();
//        
//        String service;
//        Integer group;
//        String[] tokens;
//        while (line != null) {
//            tokens  = line.split(" ");
//            service = tokens[0];
//            group   = Integer.parseInt(tokens[1]);
//            
//            this.highLevelProfiles.put(service, group);
//        }
        
        this.highLevelProfiles.put("CREAM-CE", 1);
        this.highLevelProfiles.put("ARC-CE"  , 1);
        this.highLevelProfiles.put("GRAM5"   , 1);
        this.highLevelProfiles.put("unicore6.TargetSystemFactory", 1);
        this.highLevelProfiles.put("SRM"  , 2);
        this.highLevelProfiles.put("SRMv2", 2);
        this.highLevelProfiles.put("Site-BDII", 3);
    }
    
    @Override
    public Tuple exec(Tuple tuple) throws IOException {
        
        DataBag in = (DataBag) tuple.get(0);
        
        if (this.weights == null) {
            this.initWeights((String) tuple.get(2));
        }

        if (this.highLevelProfiles == null) {
            getHighLevelProfiles();
        }
        
        String service_flavor;
        State[] timeline, tmp;
        timeline = tmp = null;
        
        ultimate_kickass_table = new HashMap<Integer, State[]>();
        
        for (Tuple t : in) {
            service_flavor = (String) t.get(5);
            String [] tmpa = ((String) t.get(3)).substring(1, ((String)t.get(3)).length() - 1).split(", ");
            
            timeline = new State[tmpa.length];
            
            for (int i = 0; i<tmpa.length; i++) {
                timeline[i] = State.valueOf(tmpa[i]);
            }

            // Future: serialize objects
//            try {
//                byte[] data = javax.xml.bind.DatatypeConverter.parseBase64Binary((String) t.get(3));
//                ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
//                timeline = (State[]) ois.readObject();
//                ois.close();
//            } catch (Exception ex) {
//                Logger.getLogger(AggregateSiteAvailability.class.getName()).log(Level.SEVERE, null, ex);
//            }

            Integer group_id = this.highLevelProfiles.get(service_flavor);
            
            if (this.ultimate_kickass_table.containsKey(group_id)) {
                tmp = this.ultimate_kickass_table.get(group_id);
                Utils.makeOR(timeline, tmp);
            } else {
                if (group_id!=null) {
                    this.ultimate_kickass_table.put(group_id, timeline);
                } 
//                else {
//                    String msg = "Encounterd: " + service_flavor;
//                    Logger.getLogger(AggregateSiteAvailability.class.getName()).log(Level.INFO, msg);
//                }
            }
        }
        
        // We get the first table, we dont care about the first iteration
        // because we do an AND with self.
        if (this.ultimate_kickass_table.size() > this.nGroups) {
            // this.output_table = new String[24];
            // Utils.makeMiss(this.output_table);
            throw new UnsupportedOperationException("A site has more flavors than expected. Something is terribly wrong! " + this.ultimate_kickass_table.keySet());
        } else {
            if (this.ultimate_kickass_table.values().size() > 0) {
                this.output_table = this.ultimate_kickass_table.values().iterator().next();
                for (State[] tb : this.ultimate_kickass_table.values()) {
                    Utils.makeAND(tb, this.output_table);
                }
            } else {
                this.output_table = new State[(int)this.quantum];
                Utils.makeMiss(this.output_table);
            }
        }
        
        String w = this.weights.get((String) tuple.get(3));
        if (w == null) {
            w = "1";
        }
        
        Tuple t = Utils.getARReport(this.output_table, mTupleFactory.newTuple(6), this.quantum);
        t.set(5, w);
        return t;
    }
    
    @Override
    public Schema outputSchema(Schema input) {
        try {
            Schema.FieldSchema availA   = new Schema.FieldSchema("availability", DataType.DOUBLE);
            Schema.FieldSchema availR   = new Schema.FieldSchema("reliability",  DataType.DOUBLE);
            Schema.FieldSchema up       = new Schema.FieldSchema("up",           DataType.DOUBLE);
            Schema.FieldSchema unknown  = new Schema.FieldSchema("unknown",      DataType.DOUBLE);
            Schema.FieldSchema down     = new Schema.FieldSchema("downtime",     DataType.DOUBLE);
            Schema.FieldSchema weight   = new Schema.FieldSchema("weight",       DataType.CHARARRAY);

            Schema p_metricS = new Schema();
            p_metricS.add(availA);
            p_metricS.add(availR);
            p_metricS.add(up);
            p_metricS.add(unknown);
            p_metricS.add(down);
            p_metricS.add(weight);
            
            return new Schema(new Schema.FieldSchema("Availability_Report", p_metricS, DataType.TUPLE));
        } catch (FrontendException ex) {
            Logger.getLogger(AggregateSiteAvailability.class.getName()).log(Level.SEVERE, null, ex);
        }

        return null;
    }

}
