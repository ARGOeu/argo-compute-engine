package myudf;

import static utils.State.OK;
import static utils.State.WARNING;
import static utils.State.UNKNOWN;
import static utils.State.CRITICAL;
import static utils.State.MISSING;
import static utils.State.DOWNTIME;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
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
import utils.EntryPair;
import utils.State;
import utils.Utils;

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class AggrigateSiteAvailability extends EvalFunc<Tuple> {
    
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    
    private String[] output_table = null;
    private String[] or_table = null;
    
    private Map<String, EntryPair<Integer,Integer>> downtimes = null;
    
    private static double round(double unrounded, int precision, int roundingMode) {
        BigDecimal bd = new BigDecimal(unrounded);
        BigDecimal rounded = bd.setScale(precision, roundingMode);
        return rounded.doubleValue();
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

    private void getDowntimes() throws FileNotFoundException, IOException {
        this.downtimes = new HashMap<String, EntryPair<Integer, Integer>>();

        FileReader fr = new FileReader("./downtimes.txt");
        BufferedReader d = new BufferedReader(fr);
        String line = d.readLine();

        String host, serviceFlavor;
        EntryPair<Integer, Integer> period;
        while (line != null) {
            String[] tokens = line.split("\u0001");
            System.out.println(Arrays.toString(tokens));
            host = tokens[0];
            serviceFlavor = tokens[1];
            period = new EntryPair<Integer, Integer>(Integer.parseInt(tokens[2].split("T")[1].split(":")[0]),
                    Integer.parseInt(tokens[3].split("T")[1].split(":")[0]));

            this.downtimes.put(host + " " + serviceFlavor, period);
            line = d.readLine();
        }
    }

    private void addDowntimes(String[] tmp_timelineTable, String host, String flavor) {
        String key = host + " " + flavor;
        if (this.downtimes.containsKey(key)) {
            EntryPair<Integer, Integer> p = this.downtimes.get(key);
            for (int i = p.First; i <= p.Second; i++) {
                tmp_timelineTable[i] = State.DOWNTIME.toString();
            }
        }
    }
    
    @Override
    public Tuple exec(Tuple tuple) throws IOException {
        DataBag in = (DataBag) tuple.get(0);
        
        String service_flavor, hostname;
        hostname = "";
        String[] timeline;
        String prev_service_flavor = null;
        
        output_table = null;
        or_table    = null;
        Iterator<Tuple> iterator = in.iterator();
        
        if (this.downtimes == null) {
            getDowntimes();
        }

        
        // Take the first tuple
        if (iterator.hasNext()) {
            Tuple t = iterator.next();
            hostname             = (String) t.get(0);
            prev_service_flavor  = (String) t.get(1);
            this.or_table        = ((String) t.get(4)).substring(1, ((String)t.get(4)).length() - 1).split(", ");
            
            // Add downtime ranges.
            addDowntimes(this.or_table, hostname, prev_service_flavor);
        }
        
        while (iterator.hasNext()) {
            Tuple t = iterator.next();
            hostname       = (String) t.get(0);
            service_flavor = (String) t.get(1);
            timeline       = ((String) t.get(4)).substring(1, ((String)t.get(4)).length() - 1).split(", ");
            
            // Add downtime ranges.
            addDowntimes(timeline, hostname, service_flavor);
            
            if (prev_service_flavor.equals(service_flavor)) {
                Utils.makeOR(timeline, this.or_table);
            } else {
                if (this.or_table != null) {
                    if (this.output_table == null) {
                        this.output_table = this.or_table;
                    } else {
                        Utils.makeAND(this.or_table, this.output_table);
                    }
                }
                this.or_table = timeline;
                prev_service_flavor = service_flavor;
            }
        }
        
        if (this.output_table == null) {
            this.output_table = this.or_table;
        } else {
            Utils.makeAND(this.or_table, this.output_table);
        }
        
        System.out.println(hostname);
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

    @Override
    public List<String> getCacheFiles() {
        List<String> list = new ArrayList<String>(1);
        list.add("/user/root/downtimes.txt#downtimes.txt");
        return list;
    }

}
