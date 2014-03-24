/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package myudf;

import java.io.IOException;
import java.util.Arrays;
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
import utils.ExternalResources;
import utils.State;
import utils.Utils;

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class VOAvailability extends EvalFunc<Tuple> {

    private final TupleFactory mTupleFactory = TupleFactory.getInstance();
    private final double quantum = 288.0;

    private Map<String, Map<String, Integer>> allAPs = null;

    @Override
    public Tuple exec(Tuple input) throws IOException {
        State[] timeline = new State[(int) this.quantum];
        String serviceFlavor = null;
        String availabilityProfile = (String) input.get(1);
        String mongoInfo = (String) input.get(2);
        Map<Integer, State[]> groupingTable = new HashMap<Integer, State[]>();

        // Connect to mongo and retrive a Map that contains Service Flavours as keys
        // and as values, a bag with the appropriate availability profiles.
        if (this.allAPs == null) {
            String mongoHostname = mongoInfo.split(":", 2)[0];
            int mongoPort = Integer.parseInt(mongoInfo.split(":", 2)[1]);

            this.allAPs = ExternalResources.initAPs(mongoHostname, mongoPort);
        }

        Map<String, Integer> currentAP = this.allAPs.get(availabilityProfile);
        if (currentAP == null) {
            return mTupleFactory.newTuple(6);
        }

        // Input: vo_s: {(hostname: chararray,service_flavour: chararray,profile: chararray,date: int,vo: chararray,timeline: chararray,availability_profile: chararray)}
        for (Tuple t : (DataBag) input.get(0)) {
            serviceFlavor = (String) t.get(1);
            String[] tmp = ((String) t.get(5)).substring(1, ((String) t.get(5)).length() - 1).split(", ", (int) this.quantum);

            for (int i = 0; i < tmp.length; i++) {
                timeline[i] = State.valueOf(tmp[i]);
            }

            Integer group_id = currentAP.get(serviceFlavor);

            if (groupingTable.containsKey(group_id)) {
                Utils.makeOR(timeline, groupingTable.get(group_id));
            } else {
                if (group_id != null) {
                    groupingTable.put(group_id, timeline.clone());
                }
            }
        }

        // We get the first table, we dont care about the first iteration
        // because we do an AND with self.
        State[] outputTable = null;
        if (groupingTable.values().size() > 0) {
            outputTable = groupingTable.values().iterator().next();
            for (State[] tb : groupingTable.values()) {
                Utils.makeAND(tb, outputTable);
            }
        } else {
            outputTable = new State[(int) this.quantum];
            Utils.makeMiss(outputTable);
        }

        Tuple t = Utils.getARReport(outputTable, mTupleFactory.newTuple(5), this.quantum);
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
            Logger.getLogger(SiteAvailability.class.getName()).log(Level.SEVERE, null, ex);
        }

        return null;
    }

}
