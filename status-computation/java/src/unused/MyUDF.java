/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package unused;

import java.io.IOException;
import myudf.AppendPOEMrules;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class MyUDF {
    
    /**
     * @param args the command line arguments
     * @throws IOException  
     */
    public static void main(String[] args) throws IOException {
//        AppendPOEMname pn = new AppendPOEMname();
//        
//        TupleFactory mTupleFactory = TupleFactory.getInstance();
//        
//        Tuple t = mTupleFactory.newTuple();
//        t.append("unicore6.Gateway");
//        long i=0;
//        for (; i<400000; i++) {
//            pn.exec(t);
//        }
//        System.out.println(i);
//        
        
        AppendPOEMrules pr = new AppendPOEMrules();
        
        TupleFactory mTupleFactory = TupleFactory.getInstance();
        
        Tuple t = mTupleFactory.newTuple();
        t.append("CREAM-CE");
        t.append("WLCG_CREAM_LCGCE_CRITICAL");
        long i=0;
        for (; i<400000; i++) {
            pr.exec(t);
        }
        System.out.println(i);
    }
}
