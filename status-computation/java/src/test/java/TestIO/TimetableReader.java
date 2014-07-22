package TestIO;

import java.io.IOException;

import java.util.Map;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

import utils.State;


public class TimetableReader  {
	
	private static State strToState(String st) throws IOException {
		
		if (st.equals("OK")) return State.OK;
		else if (st.equals("WARNING")) return State.WARNING;
		else if (st.equals("UNKNOWN")) return State.UNKNOWN;
		else if (st.equals("MISSING")) return State.MISSING;
		else if (st.equals("CRITICAL")) return State.CRITICAL;
		else if (st.equals("DOWNTIME")) return State.DOWNTIME;
		
		throw new IOException("Unrecognized state");
	}
	
	public static State[] fromJson( String jStr) throws IOException {
		
		JsonParser parser = new JsonParser();
		JsonObject job = parser.parse(jStr).getAsJsonObject();
		for(Map.Entry<String, JsonElement> entry : job.entrySet()) {
			JsonArray arr = entry.getValue().getAsJsonArray();
			//Get size of timeline
			int size = arr.size();
			State[] timeline = new State[size];
			for (int i=0; i<size; i++) {
				timeline[i] = strToState(arr.get(i).getAsString());
			}
			return timeline;
		}
		
		return null;	
	}
	
	public static JsonObject toJson(State[] tbl, String lbl) throws IOException {
		
		JsonObject job = new JsonObject();
		JsonArray arr = new JsonArray();
		
		for (int i=0;i<tbl.length;i++) {
			arr.add(new JsonPrimitive(tbl[i].toString()));
			System.out.println(tbl[i].toString());
		}
		
		job.add(lbl, arr);
		
		return job;
		
	}
}
