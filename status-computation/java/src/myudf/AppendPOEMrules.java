package myudf;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class AppendPOEMrules extends EvalFunc<Tuple> {
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    
    private Map<String, ArrayList<String>> map = null;
    
    private void initDB() throws FileNotFoundException, IOException {
        this.map = new HashMap<String, ArrayList<String>>();
        FileReader fr = new FileReader("./poem_profiles.txt");
        BufferedReader d = new BufferedReader(fr);
        String line = d.readLine();

        while (line != null) {
            String[] tokens = line.split("\u0001");
            // Input:
            //  [0]:profile_name, [1]:service_flavor, [2] metric

            String key = tokens[0] + " " + tokens[1];
            String metric = tokens[2];

            if (this.map.containsKey(key)) {
                this.map.get(key).add(metric);
            } else {
                ArrayList<String> metrics = new ArrayList<String>();
                metrics.add(metric);
                this.map.put(key, metrics);
            }

            line = d.readLine();
        }
    }
    
    @Override
    public Tuple exec(Tuple tuple) throws IOException {
        
        if (map == null) {
            initDB();
        }

        String serviceFlavor, profile, VO;
        
        try {
            serviceFlavor = (String) tuple.get(0);

            profile = (String) tuple.get(1);
//            VO = (String) tuple.get(1);
        } catch (Exception e) {
            throw new IOException("Expected input to be Strings, but  got " + e);
        }
        
        String key = profile + " " + serviceFlavor;
        ArrayList<String> metrics = this.map.get(key);
        
        if (metrics == null) {
            return mTupleFactory.newTuple();
        }

        Tuple outProfileRules = mTupleFactory.newTuple();
        for (String metric : metrics) {
            outProfileRules.append(metric);
        }
        
        return outProfileRules;
    }
    
    @Override
    public List<String> getCacheFiles() {
        List<String> list = new ArrayList<String>(1);
        list.add("/user/root/poem_profiles.txt#poem_profiles.txt");
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
        //   profile_metrics: (profile_name: chararray)        
        try {
            Schema.FieldSchema profile_metricFs = new Schema.FieldSchema("profile_metric", DataType.CHARARRAY);
            Schema p_metricS = new Schema(profile_metricFs);

            Schema.FieldSchema p_metricsFs;

            return new Schema(new Schema.FieldSchema("profile_metrics", p_metricS, DataType.TUPLE));
        } catch (FrontendException ex) {
            Logger.getLogger(AppendPOEMrules.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return null;
    }
}
