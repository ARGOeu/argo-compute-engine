package sync;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Recomputations {

	private static final Logger LOG = Logger.getLogger(Recomputations.class.getName());

	public Map<String,ArrayList<Map<String,String>>> groups;
	
	
	public Recomputations() {
		this.groups = new HashMap<String,ArrayList<Map<String,String>>>();
	}

	// Clear all the recomputation data
	public void clear() {
		this.groups = new HashMap<String,ArrayList<Map<String,String>>>();
	}
	
	// Insert new recomputation data for a specific endpoint group
	public void insert(String group, String start, String end) {
		
		Map<String,String>temp = new HashMap<String,String>();
		temp.put("start", start);
		temp.put("end",end);
		
		if (this.groups.containsKey(group) == false){
			this.groups.put(group, new ArrayList<Map<String,String>>());
		} 
		
		this.groups.get(group).add(temp);
		
	}

	// Check if group is excluded in recomputations
	public boolean isExcluded (String group){
		return this.groups.containsKey(group);
	}
	
	// Check if a recomputation period is valid for target date
	public boolean validPeriod(String target, String start, String end) throws ParseException {

		SimpleDateFormat dmy = new SimpleDateFormat("yyyy-MM-dd");
		Date tDate = dmy.parse(target);
		Date sDate = dmy.parse(start);
		Date eDate = dmy.parse(end);

		return (tDate.compareTo(sDate) >= 0 && tDate.compareTo(eDate) <= 0);

	}

	public ArrayList<Map<String,String>> getPeriods(String group,String targetDate) throws ParseException {
		ArrayList<Map<String,String>> periods = new ArrayList<Map<String,String>>();
		
		if (this.groups.containsKey(group)){
			for (Map<String,String> period : this.groups.get(group)){
				if (this.validPeriod(targetDate, period.get("start"), period.get("end"))){
					periods.add(period);
				}
			}
			
		}
		
		return periods;
	}

	

	public void loadJson(File jsonFile) throws IOException {

		this.clear();

		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(jsonFile));

			JsonParser jsonParser = new JsonParser();
			JsonElement jRootElement = jsonParser.parse(br);
			JsonArray jRootObj = jRootElement.getAsJsonArray();

			for (JsonElement item : jRootObj) {
				String start = item.getAsJsonObject().get("start_time").getAsString();
				String end = item.getAsJsonObject().get("end_time").getAsString();

				// Get the excluded
				JsonArray jExclude = item.getAsJsonObject().get("exclude").getAsJsonArray();
				for (JsonElement subitem : jExclude) {
					this.insert(subitem.getAsString(),start,end);
				}

			}

		} catch (FileNotFoundException ex) {
			LOG.error("Could not open file:" + jsonFile.getName());
			throw ex;

		} finally {
			// Close quietly without exceptions the buffered reader
			IOUtils.closeQuietly(br);
		}

	}

}
