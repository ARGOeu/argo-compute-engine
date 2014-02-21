/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package myudf;

import java.io.IOException;
import java.util.HashMap;
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
public class VOAvailability extends EvalFunc<Tuple> {
    private final TupleFactory mTupleFactory = TupleFactory.getInstance();
    private final double quantum = 288.0;
    
    private State[] output_table = null;

    @Override
    public Tuple exec(Tuple input) throws IOException {
        State[] timeline = new State[(int) this.quantum];
        
        // Input: timetables: {(hostname: chararray,service_flavour: chararray,profile: chararray,vo: chararray,date: chararray,timeline: chararray)
        for (Tuple t : (DataBag) input.get(0)) {
            String [] tmp = ((String) t.get(5)).substring(1, ((String)t.get(5)).length() - 1).split(", ", (int) this.quantum);
            
            for (int i = 0; i<tmp.length; i++) {
                timeline[i] = State.valueOf(tmp[i]);
            }
            
            if (this.output_table != null) {
                Utils.makeOR(timeline, this.output_table);
            } else {
                this.output_table = timeline.clone();
            }
        }
        
        Tuple t = Utils.getARReport(this.output_table, mTupleFactory.newTuple(5), this.quantum);
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

            Schema p_metricS = new Schema();
            p_metricS.add(availA);
            p_metricS.add(availR);
            p_metricS.add(up);
            p_metricS.add(unknown);
            p_metricS.add(down);
            
            return new Schema(new Schema.FieldSchema("VO_Report", p_metricS, DataType.TUPLE));
        } catch (FrontendException ex) {
            Logger.getLogger(AggregateSiteAvailability.class.getName()).log(Level.SEVERE, null, ex);
        }

        return null;
    }

}
