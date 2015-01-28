package ops;

import static org.junit.Assert.*;

import org.junit.Test;

public class DTimelineTest {

	@Test
	public void test() {
		
		// Initialize Discrete Timeline for testing
		DTimeline dtl = new DTimeline();
		// Clear Samples
	    dtl.clearSamples();
	    // Assert each initialized element that is = -1
	    for (int i=0;i<dtl.samples.length;i++)
	    {
	    	assertEquals("Init element must be -1",dtl.samples[i],-1);
	    }
		
		
	}

}
