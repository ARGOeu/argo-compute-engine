package myudf;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import utils.ExternalResources;

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class AddTopology extends EvalFunc<Tuple> {    
    private final TupleFactory mTupleFactory = TupleFactory.getInstance();
    private Map<String, String[]> topology = null;
    private Map<String, DataBag> avail_profs = null;

    private void initTopology(final String topology) throws FileNotFoundException, IOException {

        this.topology = new HashMap<String, String[]>();

        byte[] decodedBytes = javax.xml.bind.DatatypeConverter.parseBase64Binary(topology);
        BufferedReader in = new BufferedReader(new InputStreamReader(new GZIPInputStream(new ByteArrayInputStream(decodedBytes))));

        String input = in.readLine();

        StringTokenizer tokenizer = new StringTokenizer(input, "\\|");

        String host, serviceFlavor;
        String[] topol;
        while (tokenizer.hasMoreTokens()) {
            // (hostname:chararray, service_flavour:chararray, production:chararray, monitored:chararray, scope:chararray, site:chararray, ngi:chararray, infrastructure:chararray, certification_status:chararray, site_scope:chararray)
            String[] tokens = tokenizer.nextToken().split("\u0001", 3);
            if (tokens.length > 2) {
                host = tokens[0];
                serviceFlavor = tokens[1];
                topol = tokens[2].split("\u0001");

                this.topology.put(host + " " + serviceFlavor, topol);
            }
        }
    }

    @Override
    public Tuple exec(Tuple tuple) throws IOException {
        if (this.topology == null) {
            this.initTopology((String) tuple.get(2) + (String) tuple.get(3) + (String) tuple.get(4));
        }

        if (this.avail_profs == null) {
            this.avail_profs = ExternalResources.getSFtoAvailabilityProfileNames("192.168.0.99", 27017);
        }
        
        String key = (String) tuple.get(0) + " " + (String) tuple.get(1);

        Tuple out = mTupleFactory.newTuple(9);
        String[] s = this.topology.get(key);
        if (s == null) {
//            AddTopology.log.error("I dont have host: " + tuple.get(0) + " and flavor: " + (String) tuple.get(1));
//            this.log.error("I dont have host: " + tuple.get(0) + " and flavor: " + (String) tuple.get(1));
//            return out;
            throw new IOException("I dont have host: " + tuple.get(0) + " and flavor: " + (String) tuple.get(1));
        }

        for (int i = 0; i < s.length; i++) {
            out.set(i, s[i]);
        }

        // Add Availability profiles as a bag
        out.set(8, this.avail_profs.get((String) tuple.get(1)));
        return out;
    }

    @Override
    public Schema outputSchema(Schema input) {
        Schema.FieldSchema production = new Schema.FieldSchema("production", DataType.CHARARRAY);
        Schema.FieldSchema monitored = new Schema.FieldSchema("monitored", DataType.CHARARRAY);
        Schema.FieldSchema scope = new Schema.FieldSchema("scope", DataType.CHARARRAY);
        Schema.FieldSchema site = new Schema.FieldSchema("site", DataType.CHARARRAY);
        Schema.FieldSchema ngi = new Schema.FieldSchema("ngi", DataType.CHARARRAY);
        Schema.FieldSchema infrastructure = new Schema.FieldSchema("infrastructure", DataType.CHARARRAY);
        Schema.FieldSchema certification_status = new Schema.FieldSchema("certification_status", DataType.CHARARRAY);
        Schema.FieldSchema site_scope = new Schema.FieldSchema("site_scope", DataType.CHARARRAY);

        
        Schema.FieldSchema availability_profile = new Schema.FieldSchema("availability_profile", DataType.CHARARRAY);
        Schema apS = new Schema();
        apS.add(availability_profile);
        Schema.FieldSchema availability_profiles = null;
        try {
            Schema tuples = new Schema(new Schema.FieldSchema("availability_profile_t", apS, DataType.TUPLE));
            availability_profiles = new Schema.FieldSchema("availability_profiles", apS, DataType.BAG);
        } catch (FrontendException ex) {
            Logger.getLogger(AddTopology.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        Schema p_metricS = new Schema();
        p_metricS.add(production);
        p_metricS.add(monitored);
        p_metricS.add(scope);
        p_metricS.add(site);
        p_metricS.add(ngi);
        p_metricS.add(infrastructure);
        p_metricS.add(certification_status);
        p_metricS.add(site_scope);
        p_metricS.add(availability_profiles);
        
        try {
            return new Schema(new Schema.FieldSchema("topology", p_metricS, DataType.TUPLE));
        } catch (FrontendException ex) {
            Logger.getLogger(AddTopology.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return null;
    }

}
