package utils;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map.Entry;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class Utils {

    public static void makeMiss(State[] t) {
        for (int i = 0; i < t.length; i++) {
            t[i] = State.MISSING;
        }
    }

    public static void makeOR(State[] table1, State[] table2) throws IOException {
        for (int i = 0; i < table1.length; i++) {
            if (table1[i].ordinal() < table2[i].ordinal()) {
                table2[i] = table1[i];
            }
        }
    }

    public static void makeAND(State[] table1, State[] table2) throws IOException {
        for (int i = 0; i < table1.length; i++) {
            if (table1[i].ordinal() > table2[i].ordinal()) {
                table2[i] = table1[i];
            }
        }
    }

    public static double round(double unrounded, int precision, int roundingMode) {
        try {
            BigDecimal bd = new BigDecimal(unrounded);
            BigDecimal rounded = bd.setScale(precision, roundingMode);
            return rounded.doubleValue();
        } catch (NumberFormatException e) {
            // I sould check for 3/0 cases.???
            return -1;
            // return 0;
        }
    }
    
    public static void putRecalculations(Entry<Integer, Integer> e, State[] tmp_timelineTable) {
        for (int i = e.getKey(); i <= e.getValue(); i++) {
            tmp_timelineTable[i] = State.UNKNOWN;
        }
    }

    public static Tuple getARReport(State[] state_table, Tuple outputTuple, double quantum) throws ExecException {
        int UP, UNKNOWN, DOWNTIME;
        UP = UNKNOWN = DOWNTIME = 0;

        for (State s : state_table) {
            switch (s) {
                case OK:
                case WARNING:
                    UP++;
                    break;
                case CRITICAL:
                    break;
                case MISSING:
                case UNKNOWN:
                    UNKNOWN++;
                    break;
                case DOWNTIME:
                    DOWNTIME++;
                    break;
            }
        }

        // Availability = UP period / KNOWN period = UP period / (Total period – UNKNOWN period)
        outputTuple.set(0, Utils.round(((UP / quantum) / (1.0 - (UNKNOWN / quantum))) * 100, 3, BigDecimal.ROUND_HALF_UP));

        // Reliability = UP period / (KNOWN period – Scheduled Downtime)
        //             = UP period / (Total period – UNKNOWN period – ScheduledDowntime)
        outputTuple.set(1, Utils.round(((UP / quantum) / (1.0 - (UNKNOWN / quantum) - (DOWNTIME / quantum))) * 100, 3, BigDecimal.ROUND_HALF_UP));

        outputTuple.set(2, Utils.round(UP / quantum, 5, BigDecimal.ROUND_HALF_UP));
        outputTuple.set(3, Utils.round(UNKNOWN / quantum, 5, BigDecimal.ROUND_HALF_UP));
        outputTuple.set(4, Utils.round(DOWNTIME / quantum, 5, BigDecimal.ROUND_HALF_UP));
        return outputTuple;
    }
    
    // The timeStamp string should be zulu.split("T")[1]. So if we are in the
    // standard format that we get from input (2013...T00:00:00Z), we should split
    // before using.
    public static int getTimeGroup(final String timeStamp, final int quantum) {
        int hour = Integer.parseInt(timeStamp.substring(0, 2));
        int minutes = Integer.parseInt(timeStamp.substring(3, 5));

        return (hour * 60 + minutes) / (24 * 60 / quantum);
    }

    // From mongodb, we are getting a time range. 
    // e.g.
    // "start_time" : "2013-12-08T12:03:44Z"
    // "end_time" : "2013-12-10T12:03:44Z"
    // 
    // If we are calculating for date 20131209 (this is the format of the input data)
    // we need understand if we have to compare the dates or the times, to get the group.
    // If we are on the same date with start_time||end_time, we determine the group
    // from the time. But if we are on a date between, we determine the group from
    // the date.
    public static int determineTimeGroup(final String zuluTime, final int runningDate, final int quantum) throws IOException {
        int date = Integer.parseInt(zuluTime.split("T", 2)[0].replaceAll("-", ""));

        if (runningDate > date) {
            return 0;
        } else if (runningDate < date) {
            return quantum - 1;
        } else { // if (runningDate == date)
            return getTimeGroup(zuluTime.split("T", 2)[1], quantum);
        }
    }
}
