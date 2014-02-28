package myudf;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
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
public class HostServiceTimelines extends EvalFunc<Tuple> {

    private final int quantum = 288;
    private final TupleFactory mTupleFactory = TupleFactory.getInstance();
    private String prev_date = null;
    private final Set<String> profile = new HashSet<String>();
    
    private Map<String, Entry<Integer, Integer>> downtimes = null;
    private Map<String, ArrayList<String>> poems = null;
    
    private Tuple point = null;
    private Iterator<Tuple> timeLineIt = null;

    private Map<String, Entry<State, Integer>> getBeakons() throws ExecException {
        String metric, status, timeStamp;
        boolean sameDate = true;
        // Key: metric, Value: Time + "@" + Status
        Map<String, Entry<State, Integer>> beakons = new HashMap<String, Entry<State, Integer>>();

        while (sameDate && this.timeLineIt.hasNext()) {
            Tuple t = this.timeLineIt.next();

            metric = (String) t.get(0);
            status = (String) t.get(1);
            timeStamp = (String) t.get(2);

            if (!this.profile.contains(metric)) {
                continue;
            }

            // Find if input report is newer from the stored.
            // We need the latest report for each metric.
            // The input is sorted by date. We dont need to check timestamps.
            if (timeStamp.startsWith(this.prev_date)) {
                int inMinutes = Integer.valueOf(timeStamp.split("T")[1].split(":")[0]) * 60 + Integer.valueOf(timeStamp.split("T")[1].split(":")[1]);
                Entry<State, Integer> e = new SimpleEntry<State, Integer>(State.valueOf(status), inMinutes);
                beakons.put(metric, e);
            } else {
                this.point = t;
                sameDate = false;
            }
        }

        return beakons;
    }

    private void addBeakon(State[] tmp_timelineTable, State status, Integer expire_minutes) {
        tmp_timelineTable[0] = status;

        int timeGroup = expire_minutes / (24 * 60 / this.quantum);

        int g = 1;
        while (g <= timeGroup && tmp_timelineTable[g] == null) {
            tmp_timelineTable[g] = tmp_timelineTable[0];
            g++;
        }

        if (g < this.quantum && tmp_timelineTable[g] == null) {
            tmp_timelineTable[g] = State.MISSING;
        }

        if (g == this.quantum) {
            tmp_timelineTable[g - 1] = State.MISSING;
        }
    }

    private Map<String, List<Entry<String, State>>> getDailyReports() throws ExecException {
        String metric, status, timeStamp;

        // metric -> sortedmap(time -> status)
        Map<String, List<Entry<String, State>>> groupOfMetrics = new HashMap<String, List<Entry<String, State>>>();

        if (this.point != null) {
            metric = (String) this.point.get(0);
            status = (String) this.point.get(1);
            timeStamp = (String) this.point.get(2);

            if (this.profile.contains(metric)) {
                Entry<String, State> e = new SimpleEntry<String, State>(timeStamp.substring(11, 16), State.valueOf(status));
                ArrayList<Entry<String, State>> l = new ArrayList<Entry<String, State>>(50);
                l.add(e);
                groupOfMetrics.put(metric, l);
            }

            this.point = null;
        }

        while (this.timeLineIt.hasNext()) {
            Tuple t = this.timeLineIt.next();

            metric = (String) t.get(0);
            status = (String) t.get(1);
            timeStamp = (String) t.get(2);

            if (!this.profile.contains(metric)) {
                continue;
            }

            // If we are in the same day.
            if (groupOfMetrics.containsKey(metric)) {
                groupOfMetrics.get(metric).add(
                        new SimpleEntry<String, State>(timeStamp.substring(11, 16), State.valueOf(status)));
            } else {
                Entry<String, State> e = new SimpleEntry<String, State>(timeStamp.substring(11, 16), State.valueOf(status));
                ArrayList<Entry<String, State>> l = new ArrayList<Entry<String, State>>(50);
                l.add(e);
                groupOfMetrics.put(metric, l);
            }
        }

        return groupOfMetrics;
    }

    private void makeIntegral(State[] points) throws IOException {
        State integral_status = points[0];

        for (int i = 1; i < points.length; i++) {
            if (points[i] == null) {
                points[i] = integral_status;
            } else {
                integral_status = points[i];
            }
        }
    }

    private void addDowntimes(State[] tmp_timelineTable, String host, String flavor) {
        String key = host + " " + flavor;
        if (this.downtimes.containsKey(key)) {
            Entry<Integer, Integer> p = this.downtimes.get(key);
            for (int i = p.getKey(); i <= p.getValue(); i++) {
                tmp_timelineTable[i] = State.DOWNTIME;
            }
        }
    }

    @Override
    // Input: timeline: {(metric, status, time_stamp)},profile_metrics: {metric}, previous_date, hostname, service_flavor
    public Tuple exec(Tuple tuple) throws IOException {
        try {
            String hostname, service_flavor, pprofile, calculationDate;
            calculationDate = null;

            // Get timeline and profiles to two different structures.
            try {
                this.timeLineIt = ((DataBag) tuple.get(0)).iterator();
                this.point = null;
                pprofile = (String) tuple.get(1);
                this.prev_date = (String) tuple.get(2);
                hostname = (String) tuple.get(3);
                service_flavor = (String) tuple.get(4);
                calculationDate = (String) tuple.get(5);
                if (this.downtimes == null) {
                    this.downtimes = ExternalResources.getDowntimes((String) tuple.get(6), this.quantum);
                }
                if (this.poems == null) {
                    this.poems = ExternalResources.initPOEMs((String) tuple.get(7));
                }
            } catch (Exception e) {
                throw new IOException("There is a problem with the input in HostServiceTimelines: " + e);
            }

            // Read the profile.
            this.profile.clear();
            this.profile.addAll(this.poems.get(pprofile + " " + service_flavor));

            if (this.profile.isEmpty()) {
                throw new IOException("Profile is empty!");
            }

            // Beakon poems. Key: Metric, Value: (status, hour)
            Map<String, Entry<State, Integer>> beakon_map = getBeakons();

            // The input is order by timestamps. We are going from past
            // to future. e.g. 2013-06-03 --> 2013-06-05
            // input: timeline: {(metric, status, time_stamp)}
            // Daily poems: Key: Metric, Value: (List(timestamp, status))
            Map<String, List<Entry<String, State>>> dailyReports = getDailyReports();

            // Initialize timelineTable.
            State[] timelineTable = new State[quantum];
            for (int i = 0; i < timelineTable.length; i++) {
                timelineTable[i] = State.OK;
            }

            // For each metric, we will take the timestamps 
            // and start building the timeline.
            for (String k_metric : dailyReports.keySet()) {
                State[] tmp_timelineTable = new State[quantum];

                this.profile.remove(k_metric);

                List<Entry<String, State>> metricReports = dailyReports.get(k_metric);

                String time_stamp;
                State status;
                for (Entry<String, State> e : metricReports) {
                    time_stamp = e.getKey();
                    status = e.getValue();

                    int hour = Integer.parseInt(time_stamp.substring(0, 2));
                    int minutes = Integer.parseInt(time_stamp.substring(3, 5));

                    int timeGroup = (hour * 60 + minutes) / (24 * 60 / this.quantum);

                    if (tmp_timelineTable[timeGroup] == null) {
                        tmp_timelineTable[timeGroup] = status;
                    } else if (tmp_timelineTable[timeGroup].ordinal() < status.ordinal()) {
                        tmp_timelineTable[timeGroup] = status;
                        // If we have 2 reports in the same quantum of time (e.g. 01:10 CRITICAL and 01:40 OK) we
                        // need to continue the next hour with the last state (e.g. in our case we must do 02:00 OK).
                    } else if (tmp_timelineTable[timeGroup].ordinal() > status.ordinal()) {
                        if (timeGroup < this.quantum - 1) {
                            tmp_timelineTable[timeGroup + 1] = status;
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
                        tmp_timelineTable[0] = State.MISSING;
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
                for (int i = 0; i < timelineTable.length; i++) {
                    if (timelineTable[i].ordinal() < State.MISSING.ordinal()) {
                        timelineTable[i] = State.MISSING;
                    }
                }
            }

            // OUTPUT SECTION
            // Schema: timeline: (date(e.g. 20130823), (quantum*"OK"))
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
            Schema.FieldSchema date = new Schema.FieldSchema("date", DataType.CHARARRAY);
            Schema.FieldSchema timeline = new Schema.FieldSchema("timeline", DataType.CHARARRAY);

            Schema tuple = new Schema();
            tuple.add(date);
            tuple.add(timeline);

            return new Schema(new Schema.FieldSchema("date_timeline", tuple, DataType.TUPLE));
        } catch (FrontendException ex) {
            Logger.getLogger(HostServiceTimelines.class.getName()).log(Level.SEVERE, null, ex);
        }

        return null;
    }
}
