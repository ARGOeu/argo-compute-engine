package utils;

import java.io.IOException;
import java.math.BigDecimal;

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class Utils {

    public static void makeMiss(String[] t) {
        for (int i = 0; i < t.length; i++) {
            t[i] = "MISSING";
        }
    }

    public static void makeOR(String[] table1, String[] table2) throws IOException {
        for (int i = 0; i < table1.length; i++) {
            if (State.valueOf(table1[i]).compareTo(State.valueOf(table2[i])) < 0) {
                table2[i] = table1[i];
            }
        }
    }

    public static void makeAND(String[] table1, String[] table2) throws IOException {
        for (int i = 0; i < table1.length; i++) {
            if (State.valueOf(table1[i]).compareTo(State.valueOf(table2[i])) > 0) {
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
}
