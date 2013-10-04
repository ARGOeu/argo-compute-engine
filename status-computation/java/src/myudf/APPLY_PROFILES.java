package myudf;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import javax.xml.bind.DatatypeConverter;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
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
public class APPLY_PROFILES extends EvalFunc<Tuple> {
    private int quontum = 24;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    private String prev_date = null;
    private Set<String> profile = new HashSet<String>();
    
    private Tuple point = null;
    private Iterator<Tuple> timeLineIt = null;
    
    private Map<String, Entry<String, Integer>> getBeakons() throws ExecException {
        String metric, status, timeStamp;
        boolean sameDate = true;
        // Key: metric, Value: Time + "@" + Status
        Map<String, Entry<String, Integer>> beakons = new HashMap<String, Entry<String, Integer>>();
                
        while (sameDate && this.timeLineIt.hasNext()) {
            Tuple t = this.timeLineIt.next();

            metric    = (String) t.get(0);
            status    = (String) t.get(1);
            timeStamp = (String) t.get(2);
            
            if (!this.profile.contains(metric)) {
                continue;
            }

            // Find if input report is newer from the stored.
            // We need the latest report for each metric.
            // The input is sorted by date. We dont need to check timestamps.
            if (timeStamp.startsWith(this.prev_date)) {
                Entry<String, Integer> e = new SimpleEntry<String, Integer>(status, Integer.valueOf(timeStamp.split("T")[1].split(":")[0]));
                beakons.put(metric, e);
            } else {
                this.point = t;
                sameDate = false;
            }
        }
        
        return beakons;
    }
    
    private void addBeakon(String[] tmp_timelineTable, String status, Integer expire_hour) {
        tmp_timelineTable[0] = status;

        int g = 1;
        while (g <= expire_hour && tmp_timelineTable[g] == null) {
            tmp_timelineTable[g] = tmp_timelineTable[0];
            g++;
        }

        if (g < this.quontum && tmp_timelineTable[g] == null) {
            tmp_timelineTable[g] = "MISSING";
        }

        if (g == this.quontum) {
            tmp_timelineTable[g - 1] = "MISSING";
        }
    }

    private Map<String, List<Entry<Calendar, String>>> getDailyReports() throws ExecException {
        String metric, status, timeStamp;
        
        // metric -> sortedmap(time -> status)
        Map<String, List<Entry<Calendar, String>>> groupOfMetrics = new HashMap<String, List<Entry<Calendar, String>>>();
        
        if (this.point!=null) {
            metric    = (String) this.point.get(0);
            status    = (String) this.point.get(1);
            timeStamp = (String) this.point.get(2);
            
            if (this.profile.contains(metric)) {
                Entry<Calendar, String> e = new SimpleEntry<Calendar, String>(DatatypeConverter.parseDateTime(timeStamp), status);
                ArrayList<Entry<Calendar, String>> l = new ArrayList<Entry<Calendar, String>>(50);
                l.add(e);
                groupOfMetrics.put(metric, l);
            } 
            
            this.point = null;
        }
        
        while (this.timeLineIt.hasNext()) {
            Tuple t = this.timeLineIt.next();
            
            metric    = (String) t.get(0);
            status    = (String) t.get(1);
            timeStamp = (String) t.get(2);
            
            if (!this.profile.contains(metric)) {
                continue;
            }
            
            // If we are in the same day.
            if (groupOfMetrics.containsKey(metric)) {
                groupOfMetrics.get(metric).add(
                        new SimpleEntry<Calendar, String>
                            (DatatypeConverter.parseDateTime(timeStamp), status));
            } else {
                Entry<Calendar, String> e = new SimpleEntry<Calendar, String>(DatatypeConverter.parseDateTime(timeStamp), status);
                ArrayList<Entry<Calendar, String>> l = new ArrayList<Entry<Calendar, String>>(50);
                l.add(e);
                groupOfMetrics.put(metric, l);
            }
        }
       
        return groupOfMetrics;
    }
    
    private void makeIntegral(String[] points) throws IOException {
        String integral_status = points[0];
        
        for (int i = 1; i < points.length; i++) {
            if (points[i] == null) {
                points[i] = integral_status;
            } else {
                integral_status = points[i];
            }
        }
    }
    
    private Map<String, Entry<Integer, Integer>> downtimes = null;

    private void getDowntimes(final String downtimes) throws FileNotFoundException, IOException {
        this.downtimes = new HashMap<String, Entry<Integer, Integer>>();
        
        byte[] decodedBytes = javax.xml.bind.DatatypeConverter.parseBase64Binary(downtimes);
        BufferedReader in = new BufferedReader(new InputStreamReader(new GZIPInputStream(new ByteArrayInputStream(decodedBytes))));

        String input = in.readLine();

        StringTokenizer tokenizer = new StringTokenizer(input, "\\|");

        String host, serviceFlavor;
        Entry<Integer, Integer> period;
        while (tokenizer.hasMoreTokens()) {
            String[] tokens = tokenizer.nextToken().split("\u0001");
            if (tokens.length > 2) {
                host = tokens[0];
                serviceFlavor = tokens[1];
                period = new SimpleEntry<Integer, Integer>(Integer.parseInt(tokens[2].split("T")[1].split(":")[0]),
                        (Integer.parseInt(tokens[3].split("T")[1].split(":")[0])));

                this.downtimes.put(host + " " + serviceFlavor, period);
            }
        }
    }

    private void addDowntimes(String[] tmp_timelineTable, String host, String flavor) {
        String key = host + " " + flavor;
        if (this.downtimes.containsKey(key)) {
            Entry<Integer, Integer> p = this.downtimes.get(key);
            for (int i = p.getKey(); i <= p.getValue(); i++) {
                tmp_timelineTable[i] = State.DOWNTIME.toString();
            }
        }
    }
    
    private Map<String, ArrayList<String>> poems = null;

    // SAM_Server, NGI, Profile, ServiceFlavour, Metric, Vo, VoFqan
    private void initPOEMs(final String poems) throws FileNotFoundException, IOException {
        this.poems = new HashMap<String, ArrayList<String>>();

        byte[] decodedBytes = javax.xml.bind.DatatypeConverter.parseBase64Binary(poems);
        BufferedReader in = new BufferedReader(new InputStreamReader(new GZIPInputStream(new ByteArrayInputStream(decodedBytes))));

        String input = in.readLine();

        StringTokenizer tokenizer = new StringTokenizer(input, "\\|");

        while (tokenizer.hasMoreTokens()) {
            String[] tokens = tokenizer.nextToken().split("\u0001");
            // Input:
            //  [0]:profile_name, [1]:service_flavor, [2] metric
            if (tokens.length > 2) {
                String key = tokens[0] + " " + tokens[1];
                String metric = tokens[2];

                if (this.poems.containsKey(key)) {
                    this.poems.get(key).add(metric);
                } else {
                    ArrayList<String> metrics = new ArrayList<String>();
                    metrics.add(metric);
                    this.poems.put(key, metrics);
                }
            }
        }
    }
    
    @Override
    // Input: timeline: {(metric, status, time_stamp)},profile_metrics: {metric}, previous_date, hostname, service_flavor
    public Tuple exec(Tuple tuple) throws IOException {
        try {
            String hostname, service_flavor, pprofile, calculationDate;
            calculationDate = null;

            // Get timeline and profiles to two different stractures.
            try {
                this.timeLineIt = ((DataBag) tuple.get(0)).iterator();
                this.point      = null;
                pprofile        = (String) tuple.get(1);
                this.prev_date  = (String) tuple.get(2);
                hostname        = (String) tuple.get(3);
                service_flavor  = (String) tuple.get(4);
                calculationDate = ((String) tuple.get(5)).replaceAll("-| ", "");
                if (this.downtimes == null) {
                    getDowntimes((String) tuple.get(6));
                }
                if (this.poems == null) {
                    initPOEMs((String) tuple.get(7));
                }
            } catch (Exception e) {
                throw new IOException("There is a problem with the input in APPLY_PROFILES: " + e);
            }
            
            // Read the profile.
            this.profile.clear();
            this.profile.addAll(this.poems.get(pprofile + " " + service_flavor));
            
            if (this.profile.isEmpty()) {
                throw new IOException("Profile is empty!");
            }
            
            // Beakon poems. Key: Metric, Value: (status, hour)
            Map<String, Entry<String, Integer>> beakon_map = getBeakons();
            
            // The input is order by timestamps. We are going from past
            // to future. e.g. 2013-06-03 --> 2013-06-05
            // input: timeline: {(metric, status, time_stamp)}
            // Daily poems: Key: Metric, Value: (List(timestamp, status))
            Map<String, List<Entry<Calendar, String>>> dailyReports = getDailyReports();

            // Initialize timelineTable.
            String[] timelineTable = new String[quontum];
            for (int i=0; i<timelineTable.length; i++) {
                timelineTable[i] = "OK";
            }
            
            // For each metric, we will take the timestamps 
            // and start building the timeline.
            for (String k_metric : dailyReports.keySet()) {
                String[] tmp_timelineTable = new String[quontum];

                this.profile.remove(k_metric);

                List<Entry<Calendar, String>> metricReports = dailyReports.get(k_metric);

                Calendar time_stamp;
                String status;
                for (Entry<Calendar, String> e : metricReports) {
                    time_stamp = e.getKey();
                    status = e.getValue();
                    int hour    = time_stamp.get(Calendar.HOUR_OF_DAY);
                    int minutes = time_stamp.get(Calendar.MINUTE);
                    int timeGroup  = (hour*60 +  minutes) / (24*60/this.quontum);

                    if (tmp_timelineTable[timeGroup] == null) {
                        tmp_timelineTable[timeGroup] = status;
                    } else if (State.valueOf(tmp_timelineTable[timeGroup]).compareTo(State.valueOf(status)) < 0) {
                        tmp_timelineTable[timeGroup] = status;
                    // If we have 2 reports in the same quantum of time (e.g. 01:10 CRITICAL and 01:40 OK) we
                    // need to continiu the next hour with the last state (e.g. in our case we must do 02:00 OK).
                    } else if (State.valueOf(tmp_timelineTable[timeGroup]).compareTo(State.valueOf(status)) > 0) {
                        if (timeGroup < this.quontum-1) {
                            tmp_timelineTable[timeGroup+1] = status;
                        }
                    }
                }

                // Add beakon.
                if (tmp_timelineTable[0] == null) {
                    if (beakon_map.containsKey(k_metric)) {
                        addBeakon(tmp_timelineTable,
                                beakon_map.get(k_metric).getKey(),
                                beakon_map.get(k_metric).getValue());
                    } else {
                        tmp_timelineTable[0] = "MISSING";
                    }
                }

                // Make the integral to the tmp array.
                makeIntegral(tmp_timelineTable);

                // Add downtime ranges.
                addDowntimes(tmp_timelineTable, hostname, service_flavor);

                // Merge the inner timeTable with the global.
                Utils.makeAND(tmp_timelineTable, timelineTable);
            }

            if (!this.profile.isEmpty()) {
                for (int i=0; i<timelineTable.length; i++) {
                    if (State.valueOf(timelineTable[i]).compareTo(State.valueOf("MISSING")) < 0) {
                        timelineTable[i] = "MISSING";
                    }
                }
            }
            
            // OUTPUT SECTION
            // Schema: timeline: (date(e.g. 20130823), (quontum*"OK"))
            Tuple t = mTupleFactory.newTuple();
            t.append(calculationDate);
            t.append(Arrays.toString(timelineTable));
            
            return t;
        } catch (ExecException ee) {
            throw ee;
        }
    }
    
    @Override
    public Schema outputSchema(Schema input) {

        // Construct our output schema, which is:
        // (OK: INTEGER, WARNING: INTEGER, UNKNOWN: INTEGER, CRITICAL: INTEGER, MISSING: INTEGER)
        try {
            Schema.FieldSchema date     = new Schema.FieldSchema("date",     DataType.CHARARRAY);
            Schema.FieldSchema timeline = new Schema.FieldSchema("timeline", DataType.CHARARRAY);

            Schema tuple = new Schema();
            tuple.add(date);
            tuple.add(timeline);

            return new Schema(new Schema.FieldSchema("date_timeline", tuple, DataType.TUPLE));
        } catch (FrontendException ex) {
            Logger.getLogger(APPLY_PROFILES.class.getName()).log(Level.SEVERE, null, ex);
        }

        return null;
    }
}
