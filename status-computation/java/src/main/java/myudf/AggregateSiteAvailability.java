package myudf;

import java.io.FileNotFoundException;
import static utils.State.OK;
import static utils.State.WARNING;
import static utils.State.UNKNOWN;
import static utils.State.CRITICAL;
import static utils.State.MISSING;
import static utils.State.DOWNTIME;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
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
    
    private double quantum = 288.0;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    
    private String[] output_table = null;
    private Map<Integer, String[]> ultimate_kickass_table;
    
    private Map<String, Integer> highLevelProfiles = null;
    
    private Integer nGroups = 3;
    
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
        
    private Tuple getReport() {
        int UP, UNKNOWN, DOWNTIME;
        UP = UNKNOWN = DOWNTIME = 0;
        
        for (String s : this.output_table) {
            State st = State.valueOf(s);
            
            switch (st) {
                case OK:
                case WARNING:
                    UP++;
                    break;                
                case CRITICAL:
                    break;
                case MISSING:
                case UNKNOWN:
                    UNKNOWN++;
                    break;
                case DOWNTIME:
                    DOWNTIME++;
                    break;
            }            
        }
        
        Tuple t = mTupleFactory.newTuple();
        
        // Availability = UP period / KNOWN period = UP period / (Total period – UNKNOWN period)
        t.append(Utils.round(((UP/this.quantum)/(1.0 - (UNKNOWN/this.quantum)))*100, 3, BigDecimal.ROUND_HALF_UP));
        
        // Reliability = UP period / (KNOWN period – Scheduled Downtime)
        //             = UP period / (Total period – UNKNOWN period – ScheduledDowntime)
        t.append(Utils.round(((UP/this.quantum)/(1.0 - (UNKNOWN/this.quantum) - (DOWNTIME/this.quantum)))*100, 3, BigDecimal.ROUND_HALF_UP));
        
        t.append(Utils.round(UP/this.quantum,       5, BigDecimal.ROUND_HALF_UP));
        t.append(Utils.round(UNKNOWN/this.quantum,  5, BigDecimal.ROUND_HALF_UP));
        t.append(Utils.round(DOWNTIME/this.quantum, 5, BigDecimal.ROUND_HALF_UP));
        return t;
    }
    
    @Override
    public Tuple exec(Tuple tuple) throws IOException {
        DataBag in = (DataBag) tuple.get(0);
        
        if (this.highLevelProfiles == null) {
            getHighLevelProfiles();
        }
        
        String service_flavor;
        String[] timeline, tmp;
        
        ultimate_kickass_table = new HashMap<Integer, String[]>();
        
        for (Tuple t : in) {
            service_flavor = (String) t.get(5);
            timeline       = ((String) t.get(3)).substring(1, ((String)t.get(3)).length() - 1).split(", ");
            
            Integer i = this.highLevelProfiles.get(service_flavor);
            
            if (this.ultimate_kickass_table.containsKey(i)) {
                tmp = this.ultimate_kickass_table.get(i);
                Utils.makeOR(timeline, tmp);
            } else {
                this.ultimate_kickass_table.put(i, timeline);
            }           
        }
        
        // We get the first table, we dont care about the first iteration
        // because we do an AND with self.
        if (this.ultimate_kickass_table.size() > this.nGroups) {
//            this.output_table = new String[24];
//            Utils.makeMiss(this.output_table);
            throw new UnsupportedOperationException("A site has more flavors than expected. Something is terribly wrong!");
        } else {
            this.output_table = this.ultimate_kickass_table.values().iterator().next();
            for (String[] tb : this.ultimate_kickass_table.values()) {
                Utils.makeAND(tb, this.output_table);
            }
        }
        
        return getReport();
    }
    
    @Override
    public Schema outputSchema(Schema input) {

        // Construct our output schema, which is:
        // (OK: INTEGER, WARNING: INTEGER, UNKNOWN: INTEGER, CRITICAL: INTEGER, MISSING: INTEGER)        
        try {
            Schema.FieldSchema availA   = new Schema.FieldSchema("availability", DataType.DOUBLE);
            Schema.FieldSchema availR   = new Schema.FieldSchema("reliability",  DataType.DOUBLE);
            Schema.FieldSchema up       = new Schema.FieldSchema("up",           DataType.DOUBLE);
            Schema.FieldSchema unknown  = new Schema.FieldSchema("unknown",      DataType.DOUBLE);
            Schema.FieldSchema down     = new Schema.FieldSchema("downtime",     DataType.DOUBLE);
            
            Schema p_metricS = new Schema();
            p_metricS.add(availA);
            p_metricS.add(availR);
            p_metricS.add(up);
            p_metricS.add(unknown);
            p_metricS.add(down);
            
            return new Schema(new Schema.FieldSchema("Availability_Report", p_metricS, DataType.TUPLE));
        } catch (FrontendException ex) {
            Logger.getLogger(AggregateSiteAvailability.class.getName()).log(Level.SEVERE, null, ex);
        }

        return null;
    }

}
