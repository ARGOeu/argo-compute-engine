package ops;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TreeMap;

public class CTimeline {
	
	private TreeMap<Date,Integer> samples;
	
	public CTimeline(){
		this.samples = new TreeMap<Date,Integer>();
	}
	
	public void insert(String timestamp, int status) throws ParseException
	{
		//Convert timestamp into date
		SimpleDateFormat w3c_date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		Date date = w3c_date.parse(timestamp);
		samples.put(date, status);
		
	}
	
}