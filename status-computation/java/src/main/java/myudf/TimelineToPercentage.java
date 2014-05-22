package myudf;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import utils.State;
import utils.Utils;
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
        String[] out = new String[24];
        int groupSize = tb.length/24;
        int UP, UNKNOWN, DOWNTIME;
        UP = UNKNOWN = DOWNTIME = 0;

        
        for (int i = 0; i < 24; i++) {
            double av,re;
            av = re = 0;
            int ma = -1;
            
            for (int g = 0; g < groupSize; g++) {
                
                State st = State.valueOf(tb[i*12+g]);
                
                switch (st) {
                    case OK:
                    case WARNING:
                        UP++;
                        break;
                    case CRITICAL:
                        break;
                    case UNKNOWN:
                    case MISSING:
                        UNKNOWN++;
                        break;
                    case DOWNTIME:
                        DOWNTIME++;
                        break;
                }
            }
            if (DOWNTIME > 0) { ma = 1; }
            av = Utils.round(((UP/groupSize)/(1.0 - (UNKNOWN/groupSize)))*100, 3, BigDecimal.ROUND_HALF_UP);
            re = Utils.round(((UP/groupSize)/(1.0 - (UNKNOWN/groupSize) - (DOWNTIME/groupSize)))*100, 3, BigDecimal.ROUND_HALF_UP);
            out[i] = av + ":" + re + ":" + ma;
        }
        
        return out;
    }

    @Override
    public String exec(Tuple t) throws IOException {        
        String time_table = (String) t.get(5);

        String[] tb = time_table.split("\\[")[1].split("\\]")[0].split(", ");
                
        return Arrays.toString(calculate(tb));
    }
}
