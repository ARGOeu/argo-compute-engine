/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package myudf;

import java.io.IOException;
import java.util.Arrays;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import utils.State;
import static utils.State.CRITICAL;
import static utils.State.DOWNTIME;
import static utils.State.MISSING;
import static utils.State.OK;
import static utils.State.UNKNOWN;
import static utils.State.WARNING;

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class TimelineToPercentage extends EvalFunc<String> {
    
    private String[] calculate(String[] tb) {
        String[] out = new String[tb.length];

        for (int i = 0; i < tb.length; i++) {
            State st = State.valueOf(tb[i]);

            switch (st) {
                case OK:
                case WARNING:
                    out[i] = "1:1:-1";
                    break;
                case CRITICAL:
                case UNKNOWN:
                case MISSING:
                    out[i] = "0:0:-1";
                    break;
                case DOWNTIME:
                    out[i] = "0:0:1";
                    break;
                
                    
            }

        }
        return out;
    }

    @Override
    public String exec(Tuple t) throws IOException {        
        String time_table = (String) t.get(4);

        String[] tb = time_table.split("\\[")[1].split("\\]")[0].split(", ");
        
        String[] array = calculate(tb);
                
        return Arrays.toString(array);
    }
}
