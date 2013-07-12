/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package unused;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import utils.POEMProfile;

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class CORRELATE_PROFILES extends EvalFunc<DataBag> {
    private BagFactory mBagFactory = BagFactory.getInstance();
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    
    private Map<String, ArrayList<POEMProfile>> map = null;
    
    @Override
    public DataBag exec(Tuple tuple) throws IOException {
        
        if (map == null) {
            this.map = new HashMap<String, ArrayList<POEMProfile>>();
            FileReader fr = new FileReader("./profiles.txt");
            BufferedReader d = new BufferedReader(fr);
            String line = d.readLine();

            while (line != null) {
                String[] tokens = line.split("\u0001");
                // Schema:
                //  [0]: profile_name, [1]:service_flavor, [2]:metric, [3]vo

                String key = tokens[1]; //+ " " + tokens[3];
                String profile_name = tokens[0];
                String value = tokens[2];

                if (this.map.containsKey(key)) {
                    boolean didNotFind = true;
                    ArrayList<POEMProfile> groups = this.map.get(key);
                    for (POEMProfile g : groups) {
                        if (g.getName().equals(profile_name)) {
                            g.appendMetrics(value);
                            didNotFind = false;
                            break;
                        }
                    }
                    if (didNotFind) {
                        groups.add(new POEMProfile(profile_name, value));
                    }
                } else {
                    ArrayList<POEMProfile> groups = new ArrayList<POEMProfile>();
                    groups.add(new POEMProfile(profile_name, value));
                    this.map.put(key, groups);
                }

                line = d.readLine();
            }
        }

        String serviceFlavor, VO;
        
        try {
            serviceFlavor = (String) tuple.get(0);
            if (serviceFlavor.split(",").length > 0) {
                serviceFlavor = serviceFlavor.split(",")[0];
            }
//            VO = (String) tuple.get(1);
        } catch (Exception e) {
            throw new IOException("Expected input to be Strings, but  got " + e);
        }
        
        String key = serviceFlavor; //+ " " + VO;
        ArrayList<POEMProfile> groups = this.map.get(key);
        
        DataBag out = mBagFactory.newDefaultBag();
        
        if (groups == null) {
            return out;
        }
        
        for (POEMProfile group : groups) {
            Tuple profile_group = mTupleFactory.newTuple();
            Tuple profile_metrics = mTupleFactory.newTuple();
            
            for (String metric : group.getMetrics()) {
                profile_metrics.append(metric);
            }
            
            profile_group.append(group.getName());
            profile_group.append(profile_metrics);
            out.add(profile_group);
        }
        
        return out;
    }
    
    @Override
    public List<String> getCacheFiles() {
        List<String> list = new ArrayList<String>(1);
        list.add("/user/root/profiles.txt#profiles.txt");
//        list.add("/root/profiles.txt#profiles.txt");
        return list;
    }
    
    @Override
    public Schema outputSchema(Schema input) {
        if (input.size() != 2) {
            throw new RuntimeException(
                    "Expected (Service Flavor:Chararray and VO:Chararray), input does not have 2 fields");
        }
        
        try {
            // Get the types for both columns and check them. If they are
            // wrong, figure out what types were passed and give a good error // message.
            if (input.getField(0).type != DataType.CHARARRAY
                    || input.getField(1).type != DataType.CHARARRAY) {
                String msg = "Expected input (Service Flavor:Chararray and VO:Chararray), received schema (";
                msg += DataType.findTypeName(input.getField(0).type);
                msg += ", ";
                msg += DataType.findTypeName(input.getField(1).type);
                msg += ")";
                throw new RuntimeException(msg);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
        // Construct our output schema, which is:
        //   profile_groups: {(profile_name: chararray,
        //                     profile_metrics: {(profile_metric: chararray)})}
                
        try {
            Schema.FieldSchema profile_metricFs = new Schema.FieldSchema("profile_metric", DataType.CHARARRAY);
            Schema p_metricS = new Schema(profile_metricFs);

            Schema.FieldSchema profile_nameFs = new Schema.FieldSchema("profile_name", DataType.CHARARRAY);

            Schema.FieldSchema p_metricsFs;

            p_metricsFs = new Schema.FieldSchema("profile_metrics", p_metricS, DataType.TUPLE);
            
            Schema outBugSchema = new Schema();
            outBugSchema.add(profile_nameFs);
            outBugSchema.add(p_metricsFs);
            
            return new Schema(new FieldSchema("profile_groups", outBugSchema, DataType.BAG));
        } catch (FrontendException ex) {
            Logger.getLogger(CORRELATE_PROFILES.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return null;
    }
}
