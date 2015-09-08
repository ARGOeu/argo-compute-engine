package ops;

import java.text.ParseException;


import org.junit.Test;

public class CTimelineTest {

	@Test
	public void test() throws ParseException  {

		CTimeline ctl = new CTimeline();
		ctl.insert("2015-05-01T11:00:00Z", 1);
		ctl.insert("2015-05-01T12:00:00Z", 1);
		ctl.insert("2015-05-01T13:00:00Z", 2);
		ctl.insert("2015-05-01T15:00:00Z", 2);
		ctl.insert("2015-05-01T16:00:00Z", 2);
		ctl.insert("2015-05-01T20:00:00Z", 2);
		ctl.insert("2015-05-01T22:00:00Z", 1);
		ctl.insert("2015-05-01T23:00:00Z", 1);
		//System.out.println(ctl.toString());
		
		
		System.out.println(ctl);

	
	}

}