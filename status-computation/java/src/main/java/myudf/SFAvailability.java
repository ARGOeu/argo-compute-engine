/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package myudf;

import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
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
public class SFAvailability extends EvalFunc<DataBag> {

    private final double quantum = 288.0;
    private final TupleFactory mTupleFactory = TupleFactory.getInstance();
    private final BagFactory mBagFactory = BagFactory.getInstance();    
    
    @Override
    public DataBag exec(Tuple input) throws IOException {
        final DataBag out_bag = mBagFactory.newDefaultBag();
        State[] output_table = null;
        
        String prev_service_flavor = null;
        State[] timeline = new State[(int)this.quantum];
                
        Iterator<Tuple> it = ((DataBag) input.get(0)).iterator();
        
        // The first loop, is always special
        if (it.hasNext()) {
            Tuple t = it.next();
            String[] tmpa = ((String) t.get(2)).substring(1, ((String) t.get(2)).length() - 1).split(", ");
            prev_service_flavor = (String) t.get(4);
            
            for (int i = 0; i < tmpa.length; i++) {
                timeline[i] = State.valueOf(tmpa[i]);
            }
            
            output_table = timeline;
        } else {
            throw new IOException("Empty Bag! Something is wrong with the input!");
        }
        
        while (it.hasNext()) {
            Tuple t = it.next();
            String[] tmpa = ((String) t.get(2)).substring(1, ((String) t.get(2)).length() - 1).split(", ");
            
            for (int i = 0; i < tmpa.length; i++) {
                timeline[i] = State.valueOf(tmpa[i]);
            }
            
            
            if (prev_service_flavor.equals((String) t.get(4))) {
                Utils.makeOR(timeline, output_table);
            } else {                
                Tuple t_o = Utils.getARReport(output_table, mTupleFactory.newTuple(6), this.quantum);
                t_o.set(5, prev_service_flavor);
                
                out_bag.add(t_o);
                prev_service_flavor = (String) t.get(4);
            }
            
        }
        
        return out_bag;
    }
    
    @Override
    public Schema outputSchema(Schema input) {
        try {
            Schema.FieldSchema availA = new Schema.FieldSchema("availability", DataType.DOUBLE);
            Schema.FieldSchema availR = new Schema.FieldSchema("reliability", DataType.DOUBLE);
            Schema.FieldSchema up = new Schema.FieldSchema("up", DataType.DOUBLE);
            Schema.FieldSchema unknown = new Schema.FieldSchema("unknown", DataType.DOUBLE);
            Schema.FieldSchema down = new Schema.FieldSchema("downtime", DataType.DOUBLE);
            Schema.FieldSchema weight = new Schema.FieldSchema("service_flavour", DataType.CHARARRAY);

            Schema p_metricS = new Schema();
            p_metricS.add(availA);
            p_metricS.add(availR);
            p_metricS.add(up);
            p_metricS.add(unknown);
            p_metricS.add(down);
            p_metricS.add(weight);

            Schema tuples = new Schema(new Schema.FieldSchema("Flavor_Report", p_metricS, DataType.TUPLE));
            
            return new Schema(new Schema.FieldSchema("Flavor_Report_Bag", tuples, DataType.BAG));
        } catch (FrontendException ex) {
            Logger.getLogger(SiteAvailability.class.getName()).log(Level.SEVERE, null, ex);
        }

        return null;
    }
    
}
