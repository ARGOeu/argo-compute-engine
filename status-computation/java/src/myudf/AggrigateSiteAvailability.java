package myudf;

import static utils.State.OK;
import static utils.State.WARNING;
import static utils.State.UNKNOWN;
import static utils.State.CRITICAL;
import static utils.State.MISSING;
import static utils.State.DOWNTIME;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;
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
public class AggrigateSiteAvailability extends EvalFunc<Tuple> {
    
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    
    private String[] output_table = null;
    private String[] or_table = null;
    
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
        
        String service_flavor;
        String[] timeline;
        String prev_service_flavor = "";
        
        output_table = null;
        or_table    = null;
        Iterator<Tuple> iterator = in.iterator();
        
        // Take the first tuple
        if (iterator.hasNext()) {
            Tuple t = iterator.next();
            prev_service_flavor  = (String) t.get(1);
            this.or_table        = ((String) t.get(4)).substring(1, ((String)t.get(4)).length() - 1).split(", ");
            
        }
        
        while (iterator.hasNext()) {
            Tuple t = iterator.next();
            service_flavor = (String) t.get(1);
            timeline       = ((String) t.get(4)).substring(1, ((String)t.get(4)).length() - 1).split(", ");
            
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
}
