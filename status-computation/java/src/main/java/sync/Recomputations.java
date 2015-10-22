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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Recomputations {

	private static final Logger LOG = Logger.getLogger(Recomputations.class.getName());

	public ArrayList<RecompItem> list;

	private class RecompItem {
		String start; // Start timestamp
		String end; // Start timestamp
		ArrayList<String> exclude; // Exclude list

		public RecompItem() {
			// Initializations

			this.start = "";
			this.end = "";
			this.exclude = new ArrayList<String>();
		}

		public RecompItem( String start, String end, ArrayList<String> exclude) {

			this.start = start;
			this.end = end;

			this.exclude = exclude;
		}
	}

	public Recomputations() {
		this.list = new ArrayList<RecompItem>();
	}

	// Clear all the recalc data
	public void clear() {
		this.list = new ArrayList<RecompItem>();
	}

	public void insert(String start, String end, ArrayList<String> exclude) {
		this.list.add(new RecompItem(start, end, exclude));
	}

	public int count() {
		return this.list.size();
	}

	public boolean shouldRecompute(String groupname, String targetDate) throws ParseException {

		for (RecompItem item : this.list) {
			// supergroup found

			// loop through all excluded sites
			for (String subitem : item.exclude) {
				// if site exists in the exclude list
				if (groupname.equalsIgnoreCase(subitem)) {

					// check if the dates alingn
					SimpleDateFormat dmy = new SimpleDateFormat("yyyy-MM-dd");
					Date sDate = dmy.parse(item.start);
					Date eDate = dmy.parse(item.end);
					Date tDate = dmy.parse(targetDate);

					return (tDate.compareTo(sDate) >= 0 && tDate.compareTo(eDate) <= 0);

				}
			}

		}

		// Site doesn't belong in recomputation exclude list
		return false;
	}

	public String getStart(String excluded) {
		for (RecompItem item : this.list) {
			for (String exItem : item.exclude) {
				if (exItem.equals(excluded)) {
					return item.start;
				}
			}
		}

		return null;
	}

	public String getEnd(String excluded) {
		for (RecompItem item : this.list) {
			for (String exItem : item.exclude) {
				if (exItem.equals(excluded)) {
					return item.end;
				}
			}
		}

		return null;
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


				ArrayList<String> exclude = new ArrayList<String>();
				// Get the excluded
				JsonArray jExclude = item.getAsJsonObject().get("exclude").getAsJsonArray();
				for (JsonElement subitem : jExclude) {
					exclude.add(subitem.getAsString());
				}

				this.insert(start, end, exclude);
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
