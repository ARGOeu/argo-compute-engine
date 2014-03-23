package myudf;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import utils.ExternalResources;
import utils.State;
import utils.Utils;

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class SiteAvailability extends EvalFunc<Tuple> {
    
    private final double quantum = 288.0;
    private final TupleFactory mTupleFactory = TupleFactory.getInstance();
    private Map<String, String> weights = null;
    
    private Integer nGroups = null;
    private Map<String, Map<String, Integer>> allAPs = null;
    private Map<String, Map<String, Object>> recalculationMap = null;

    @Override
    public Tuple exec(Tuple tuple) throws IOException {
        String availabilityProfile = (String) tuple.get(1);
        String weightsInfo = (String) tuple.get(2);
        String site = (String) tuple.get(3);
        String mongoInfo = (String) tuple.get(4);
        Integer date = (Integer) tuple.get(5);
        String ngi = (String) tuple.get(6);
        
        State[] output_table = null;
        Map<Integer, State[]> ultimate_kickass_table = new HashMap<Integer, State[]>();
        
        // Get a map that contains Sites as keys and the weight of each site as
        // a value.
        if (this.weights == null) {
            this.weights = ExternalResources.initWeights(weightsInfo);
        }
        
        // Connect to mongo and retrive a Map that contains Service Flavours as keys
        // and as values, a bag with the appropriate availability profiles.
        if (this.allAPs == null) {
            String mongoHostname = mongoInfo.split(":",2)[0];
            int mongoPort = Integer.parseInt(mongoInfo.split(":",2)[1]);
            
            this.allAPs = ExternalResources.initAPs(mongoHostname, mongoPort);
        }
        
        Map<String, Integer> currentAP = this.allAPs.get(availabilityProfile);
        if (currentAP == null) {
            return null;
        }
        this.nGroups = Collections.max(currentAP.values());
        
        // Get recalculation requests. Create arrays with UKNOWN states that will
        // be merged later on with the results.
        if (this.recalculationMap == null) {
            String mongoHostname = mongoInfo.split(":",2)[0];
            int mongoPort = Integer.parseInt(mongoInfo.split(":",2)[1]);

            this.recalculationMap = ExternalResources.getRecalculationRequests(mongoHostname, mongoPort, date, (int) this.quantum);
        }
        
        String service_flavor;
        State[] timeline = new State[(int)this.quantum];
                
        for (Tuple t : (DataBag) tuple.get(0)) {            
            service_flavor = (String) t.get(4);
            String [] tmpa = ((String) t.get(2)).substring(1, ((String)t.get(2)).length() - 1).split(", ");
            
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
//                Logger.getLogger(SiteAvailability.class.getName()).log(Level.SEVERE, null, ex);
//            }

            Integer group_id = currentAP.get(service_flavor);
            
            if (ultimate_kickass_table.containsKey(group_id)) {
                Utils.makeOR(timeline, ultimate_kickass_table.get(group_id));
            } else {
                if (group_id!=null) {
                    ultimate_kickass_table.put(group_id, timeline);
                } 
            }
        }
        
        // We get the first table, we dont care about the first iteration
        // because we do an AND with self.
        if (ultimate_kickass_table.size() > this.nGroups) {
            // this.output_table = new String[24];
            // Utils.makeMiss(this.output_table);
            throw new UnsupportedOperationException("A site has more flavors than expected. Something is terribly wrong! " + ultimate_kickass_table.keySet());
        } else {
            if (ultimate_kickass_table.values().size() > 0) {
                output_table = ultimate_kickass_table.values().iterator().next();
                for (State[] tb : ultimate_kickass_table.values()) {
                    Utils.makeAND(tb, output_table);
                }
            } else {
                output_table = new State[(int)this.quantum];
                Utils.makeMiss(output_table);
            }
        }
        
        // Get the weight of each site. If the weight is missing, mark it as 1.
        String w = this.weights.get(site);
        if (w == null) {
            w = "1";
        }
        
        // Add recalculation data at site levels.
        Map<String, Object> ngiRecalcRequest = this.recalculationMap.get(ngi);
        
        // Check if we have a request for this ngi.
        if (ngiRecalcRequest != null) {
            // Check if this site is excluded.
            if (!Arrays.asList((String[]) ngiRecalcRequest.get("exclude")).contains(site)) {
                Utils.putRecalculations((Entry<Integer, Integer>) ngiRecalcRequest.get("data"), output_table);
            }
        }
        
        // Count A/R for the site. Append weight in the end.
        Tuple t = Utils.getARReport(output_table, mTupleFactory.newTuple(6), this.quantum);
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
            Logger.getLogger(SiteAvailability.class.getName()).log(Level.SEVERE, null, ex);
        }

        return null;
    }

}
