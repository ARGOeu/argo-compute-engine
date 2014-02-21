package utils;

import java.io.IOException;
import java.math.BigDecimal;
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
}
