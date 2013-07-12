package utils;

import java.io.IOException;

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class Utils {
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
}
