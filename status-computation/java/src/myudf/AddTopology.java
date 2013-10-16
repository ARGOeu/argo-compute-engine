/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package myudf;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.zip.GZIPInputStream;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class AddTopology extends EvalFunc<Tuple> {
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    private Map<String, String[]> topology = null;

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
            String[] tokens = tokenizer.nextToken().split("\u0001",3);
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
            this.initTopology((String) tuple.get(2)+(String) tuple.get(3)+(String) tuple.get(4));
        }
        
        String key = (String) tuple.get(0) + " " + (String) tuple.get(1);
        
        Tuple out = mTupleFactory.newTuple(8);
        String[] s = this.topology.get(key);
        for (int i=0; i<s.length; i++){
            out.set(i, s[i]);
        }
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

            Schema p_metricS = new Schema();
            p_metricS.add(production);
            p_metricS.add(monitored);
            p_metricS.add(scope);
            p_metricS.add(site);
            p_metricS.add(ngi);
            p_metricS.add(infrastructure);
            p_metricS.add(certification_status);
            p_metricS.add(site_scope);

            return p_metricS;
    }
    
}
