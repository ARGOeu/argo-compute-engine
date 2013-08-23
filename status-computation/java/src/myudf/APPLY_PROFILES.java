package myudf;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.xml.bind.DatatypeConverter;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import utils.EntryPair;
import utils.State;
import utils.Utils;

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class APPLY_PROFILES extends EvalFunc<String> {
//    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    private String prev_date = null;
    private Set<String> profile = new HashSet<String>();
    
    private Tuple point = null;
    private Iterator<Tuple> timeLineIt = null;
    
    private Map<String, EntryPair<String, Integer>> getBeakons() throws ExecException {
        String metric, status, timeStamp;
        boolean sameDate = true;
        // Key: metric, Value: Time + "@" + Status
        Map<String, EntryPair<String, Integer>> beakons = new HashMap<String, EntryPair<String, Integer>>();
                
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
                EntryPair<String, Integer> e = new EntryPair<String, Integer>(status, Integer.valueOf(timeStamp.split("T")[1].split(":")[0]));
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

        if (g < 24 && tmp_timelineTable[g] == null) {
            tmp_timelineTable[g] = "MISSING";
        }

        if (g == 24) {
            tmp_timelineTable[g - 1] = "MISSING";
        }
    }

    private Map<String, List<EntryPair<Calendar, String>>> getDailyReports() throws ExecException {
        String metric, status, timeStamp;
        
        // metric -> sortedmap(time -> status)
        Map<String, List<EntryPair<Calendar, String>>> groupOfMetrics = new HashMap<String, List<EntryPair<Calendar, String>>>();
        
        if (this.point!=null) {
            metric    = (String) this.point.get(0);
            status    = (String) this.point.get(1);
            timeStamp = (String) this.point.get(2);
            
            if (this.profile.contains(metric)) {
                EntryPair<Calendar, String> e = new EntryPair<Calendar, String>(DatatypeConverter.parseDateTime(timeStamp), status);
                ArrayList<EntryPair<Calendar, String>> l = new ArrayList<EntryPair<Calendar, String>>(50);
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
                        new EntryPair<Calendar, String>
                            (DatatypeConverter.parseDateTime(timeStamp), status));
            } else {
                EntryPair<Calendar, String> e = new EntryPair<Calendar, String>(DatatypeConverter.parseDateTime(timeStamp), status);
                ArrayList<EntryPair<Calendar, String>> l = new ArrayList<EntryPair<Calendar, String>>(50);
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
    
    private Map<String, EntryPair<Integer, Integer>> downtimes = null;

    private void getDowntimes() throws FileNotFoundException, IOException {
        this.downtimes = new HashMap<String, EntryPair<Integer, Integer>>();

        FileReader fr = new FileReader("./downtimes.txt");
        BufferedReader d = new BufferedReader(fr);
        String line = d.readLine();

        String host, serviceFlavor;
        EntryPair<Integer, Integer> period;
        while (line != null) {
            String[] tokens = line.split("\u0001");
            host = tokens[0];
            serviceFlavor = tokens[1];
            period = new EntryPair<Integer, Integer>(Integer.parseInt(tokens[2].split("T")[1].split(":")[0]),
                    (Integer.parseInt(tokens[3].split("T")[1].split(":")[0]) - 1));

            this.downtimes.put(host + " " + serviceFlavor, period);
            line = d.readLine();
        }
    }

    private void addDowntimes(String[] tmp_timelineTable, String host, String flavor) {
        String key = host + " " + flavor;
        if (this.downtimes.containsKey(key)) {
            EntryPair<Integer, Integer> p = this.downtimes.get(key);
            for (int i = p.First; i <= p.Second; i++) {
                tmp_timelineTable[i] = State.DOWNTIME.toString();
            }
        }
    }
    
    @Override
    // Input: timeline: {(metric, status, time_stamp)},profile_metrics: {metric}, previous_date, hostname, service_flavor
    public String exec(Tuple tuple) throws IOException {
        try {
            String hostname, service_flavor;
            Tuple p_metrics;
            
            // Get timeline and profiles to two different stractures.
            try {
                this.timeLineIt = ((DataBag) tuple.get(0)).iterator();
                this.point      = null;
                p_metrics       = (Tuple) tuple.get(1);
                this.prev_date  = (String) tuple.get(2);
                hostname        = (String) tuple.get(3);
                service_flavor  = (String) tuple.get(3);
            } catch (Exception e) {
                throw new IOException("Expected input to be (DataBag, Tuple, String), but  got " + e);
            }
            
            // Read the profile.
            this.profile.clear();
            for (Object t : p_metrics) {
                this.profile.add((String) t);
            }
            
            if (this.profile.isEmpty()) {
                throw new IOException("Prifile is empty!");
            }
            
            if (this.downtimes == null) {
                getDowntimes();
            }

            // Beakon map. Key: Metric, Value: (status, hour)
            Map<String, EntryPair<String, Integer>> beakon_map = getBeakons();
            
            // The input is order by timestamps. We are going from past
            // to future. e.g. 2013-06-03 --> 2013-06-05
            // input: timeline: {(metric, status, time_stamp)}            
            Map<String, List<EntryPair<Calendar, String>>> dailyReports = getDailyReports();

            // Initialize timelineTable.
            String[] timelineTable = new String[24];
            for (int i=0; i<timelineTable.length; i++) {
                timelineTable[i] = "OK";
            }
            
            // For each metric, we will take the timestamps 
            // and start building the timeline.
            for (String k_metric : dailyReports.keySet()) {
                String[] tmp_timelineTable = new String[24];
                
                profile.remove(k_metric);

                List<EntryPair<Calendar, String>> metricReports = dailyReports.get(k_metric);
                
                Calendar time_stamp;
                String status;
                for (EntryPair<Calendar, String> e : metricReports) {
                    time_stamp = e.First;
                    status = e.Second;
                    int hour = time_stamp.get(Calendar.HOUR_OF_DAY);
                    
                    if (tmp_timelineTable[hour] == null) {
                        tmp_timelineTable[hour] = status;
                    } else if (State.valueOf(tmp_timelineTable[hour]).compareTo(State.valueOf(status)) < 0) {
                        tmp_timelineTable[hour] = status;
                    }
                }
                
                // Add beakon.
                if (tmp_timelineTable[0] != null) {
                } else if (beakon_map.containsKey(k_metric)) {
                    addBeakon(tmp_timelineTable, 
                            beakon_map.get(k_metric).First, 
                            beakon_map.get(k_metric).Second);                    
                } else {
                    tmp_timelineTable[0] = "MISSING";
                }
                
                // Make the integral to the tmp array.
                makeIntegral(tmp_timelineTable);
                
                // Add downtime ranges.
                addDowntimes(tmp_timelineTable, hostname, service_flavor);
                
                // Merge the inner timeTable with the global.
                Utils.makeAND(tmp_timelineTable, timelineTable);
            }
            
            if (!profile.isEmpty()) {
                for (int i=0; i<timelineTable.length; i++) {
                    if (!State.valueOf(timelineTable[i]).equals(State.valueOf("DOWNTIME"))) {
                        timelineTable[i] = "MISSING";
                    }
                }
            }
            
            // OUTPUT SECTION
            // Schema: timeline: (24*"OK")
                                    
            return Arrays.toString(timelineTable);
        } catch (ExecException ee) {
            throw ee;
        }
    }
    
    @Override
    public List<String> getCacheFiles() {
        List<String> list = new ArrayList<String>(1);
        list.add("/user/root/downtimes.txt#downtimes.txt");
        return list;
    }
}
