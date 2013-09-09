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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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
    
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    
    private String[] output_table = null;
    private List<String[]> ultimate_kickass_table = new ArrayList<String[]>(20);
    
    private Map<String, Integer> highLevelProfiles = null;
    
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
        this.highLevelProfiles.put("ARC-CE", 1);
        this.highLevelProfiles.put("GRAM5", 1);
        this.highLevelProfiles.put("unicore6.TargetSystemFactory", 1);
        this.highLevelProfiles.put("SRM", 2);
        this.highLevelProfiles.put("SRMv2", 2);
        this.highLevelProfiles.put("Site-BDII", 3);
    }
    
    private static double round(double unrounded, int precision, int roundingMode) {
        try {
            BigDecimal bd = new BigDecimal(unrounded);
            BigDecimal rounded = bd.setScale(precision, roundingMode);
            return rounded.doubleValue();
        } catch (NumberFormatException e) {
            return 0;
        }
        
    }
    
    private Tuple getReport() {
        int UP, UNKNOWN, DOWN, DOWNTIME;
        UP = UNKNOWN = DOWN = DOWNTIME = 0;
        
        for (String s : this.output_table) {
            State st = State.valueOf(s);
            
            switch (st) {
                case OK:
                case WARNING:
                    UP++;
                    break;                
                case CRITICAL:
                    DOWN++;
                    break;
                case UNKNOWN:
                case MISSING:
                    UNKNOWN++;
                    break;
                case DOWNTIME:
                    DOWNTIME++;
                    break;
            }            
        }
        
        Tuple t = mTupleFactory.newTuple();
        
        // Availability = UP period / KNOWN period = UP period / (Total period – UNKNOWN period)
        t.append(round(((UP/24.0)/(1.0 - (UNKNOWN/24.0)))*100, 3, BigDecimal.ROUND_HALF_UP));
        
        // Reliability = UP period / (KNOWN period – Scheduled Downtime)
        //             = UP period / (Total period – UNKNOWN period – ScheduledDowntime)
        t.append(round(((UP/24.0)/(1.0 - (UNKNOWN/24.0) - (DOWNTIME/24.0)))*100, 3, BigDecimal.ROUND_HALF_UP));
        
        return t;
    }
    
    @Override
    public Tuple exec(Tuple tuple) throws IOException {
        DataBag in = (DataBag) tuple.get(0);
        
        if (this.highLevelProfiles == null) {
            getHighLevelProfiles();
        }
        
        Tuple t;
        String service_flavor;
        String[] timeline, tmp;
        
        Iterator<Tuple> iterator = in.iterator();
        
        while (iterator.hasNext()) {
            t = iterator.next();
            service_flavor = (String) t.get(1);
            timeline       = ((String) t.get(4)).substring(1, ((String)t.get(4)).length() - 1).split(", ");
            
            int i = this.highLevelProfiles.get(service_flavor);
            
            if (this.ultimate_kickass_table.get(i) != null) {
                tmp = this.ultimate_kickass_table.get(i);
                Utils.makeOR(timeline, tmp);
            } else {
                this.ultimate_kickass_table.set(i, timeline);
            }
        }
        
        // We get the first table, we dont care about the first iteration
        // because we do an AND with self.
        this.output_table = this.ultimate_kickass_table.get(0);
        
        for (String[] tb : this.ultimate_kickass_table) {
            Utils.makeAND(tb, this.output_table);
        }

        return getReport();
    }
    
    @Override
    public Schema outputSchema(Schema input) {

        // Construct our output schema, which is:
        // (OK: INTEGER, WARNING: INTEGER, UNKNOWN: INTEGER, CRITICAL: INTEGER, MISSING: INTEGER)        
        try {
            Schema.FieldSchema availA = new Schema.FieldSchema("availability", DataType.DOUBLE);
            Schema.FieldSchema availR  = new Schema.FieldSchema("reliability", DataType.DOUBLE);
            
            Schema p_metricS = new Schema();
            p_metricS.add(availA);
            p_metricS.add(availR);
            
            return new Schema(new Schema.FieldSchema("Availability_Report", p_metricS, DataType.TUPLE));
        } catch (FrontendException ex) {
            Logger.getLogger(AppendPOEMrules.class.getName()).log(Level.SEVERE, null, ex);
        }

        return null;
    }
    
//    @Override
//    public List<String> getCacheFiles() {
//        List<String> list = new ArrayList<String>(1);
//        list.add("/user/root/highlevelprofiles.txt#highlevelprofiles.txt");
//        return list;
//    }
}
