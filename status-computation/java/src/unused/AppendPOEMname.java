/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package unused;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class AppendPOEMname extends EvalFunc<DataBag> {
    private BagFactory mBagFactory = BagFactory.getInstance();
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    
    private Map<String, List<Tuple>> profile_hash = null;
    private Map<String, Set<String>> vo_index = null;
    private Map<String, Set<String>> fqan_index = null;
    
    private void initDB() throws FileNotFoundException, IOException {
        Map<String, Set<String>> map = new HashMap<String, Set<String>>();
        FileReader fr = new FileReader("./profile_names.txt");
        BufferedReader d = new BufferedReader(fr);
        String line = d.readLine();

        while (line != null) {
            String[] tokens = line.split("\u0001");
            // Schema:
            // [0] Profile, [1] SAMServer, [2] ServiceFlavour, [3] Metric, [4] Vo, [5] VoFqan

            String profile_name = tokens[0];
            
            // [1] SAMServer, [2] ServiceFlavour, [3] Metric
            String master_key = tokens[1] + " " + tokens[2] + " " + tokens[3];
            
            String vo = tokens[4];
            String voFqan = tokens[5];
            
            if (this.vo_index.containsKey(vo)) {
                this.vo_index.get(vo).add(profile_name);
            } else {
               Set<String> set = new HashSet<String>();
//               this.vo_index.
            }
            
            if (map.containsKey(master_key)) {
                map.get(master_key).add(profile_name);
            } else {
                Set<String> profile_names = new HashSet<String>();
                profile_names.add(profile_name);
                map.put(master_key, profile_names);
            }

            line = d.readLine();
        }
        
        this.profile_hash = new HashMap<String, List<Tuple>>();
        
        String[] tmp = new String[0];
        for (String key : map.keySet()) {
            List<Tuple> l = new ArrayList<Tuple>();
            for (String v : map.get(key).toArray(tmp)) {
                l.add(mTupleFactory.newTuple(v));
            }
            this.profile_hash.put(key, l);
        }
    }

    @Override
    public DataBag exec(Tuple tuple) throws IOException {
        
        if (this.profile_hash == null) {
           initDB();
        }

        String serviceFlavor, metric, samHost, VO, FQAN;
        
        try {
            serviceFlavor = (String) tuple.get(0);
            metric        = (String) tuple.get(1);
            samHost       = (String) tuple.get(2);
            VO            = (String) tuple.get(3);
            FQAN          = (String) tuple.get(4);
            
            
        } catch (Exception e) {
            throw new IOException("Expected input to be Strings, but  got " + e);
        }
        
        String key = serviceFlavor; //+ " " + VO;
        List<Tuple> profile_names = this.profile_hash.get(key);
        
        if (profile_names == null) {
            return mBagFactory.newDefaultBag();
        }
        
//        List<Tuple> outA = new ArrayList<Tuple>(10);
//        for (Tuple profile_name : profile_names) {
//            outA.add(profile_name);
//        }
        
        return mBagFactory.newDefaultBag(profile_names);
    }
    
    @Override
    public List<String> getCacheFiles() {
        List<String> list = new ArrayList<String>(1);
        list.add("/user/root/profile_names.txt#profile_names.txt");
//        list.add("/root/profiles_names.txt#profile_names.txt");
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
        //   profiles: {(profile_name: chararray)}
        try {
            Schema.FieldSchema profile_nameFs = new Schema.FieldSchema("profile_name", DataType.CHARARRAY);
            
            Schema outBugSchema = new Schema();
            outBugSchema.add(profile_nameFs);
            
            return new Schema(new FieldSchema("profiles", outBugSchema, DataType.BAG));
        } catch (FrontendException ex) {
            Logger.getLogger(AppendPOEMname.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return null;
    }
}
