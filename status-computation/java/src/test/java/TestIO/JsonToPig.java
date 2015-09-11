package TestIO;

import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

public class JsonToPig {

	public static Tuple dblToTuple(double[] arr) {
		if (arr.length < 1)
			return null;

		TupleFactory tf = TupleFactory.getInstance();
		Tuple t = tf.newTuple();

		for (int i = 0; i < arr.length; i++) {
			t.append(arr[i]);
		}

		return t;
	}

	public static Tuple jsonToTuple(String json_str) throws ExecException {
		JsonParser parser = new JsonParser();
		JsonObject job = parser.parse(json_str).getAsJsonObject();
		TupleFactory tf = TupleFactory.getInstance();
		Tuple t = tf.newTuple();
		t = dig(job);
		return t;
	}

	private static Tuple dig(JsonElement el) throws ExecException {
		TupleFactory tf = TupleFactory.getInstance();
		Tuple tup = tf.newTuple();
		BagFactory bf = BagFactory.getInstance();
		DataBag dbg = bf.newDefaultBag();

		if (el.isJsonNull()) {
			tup.append(null);
			return tup;
		} else if (el.isJsonPrimitive()) {
			tup.append(el.getAsString());
			return tup;
		} else if (el.isJsonArray()) {
			JsonArray arr = el.getAsJsonArray();

			for (JsonElement item : arr) {
				dbg.add(dig(item));
			}
			tup.append(dbg);
			return tup;
		} else if (el.isJsonObject()) {
			JsonObject job = el.getAsJsonObject();
			for (Map.Entry<String, JsonElement> entry : job.entrySet()) {
				if (entry.getValue().isJsonPrimitive()) {
					JsonPrimitive item = (JsonPrimitive) entry.getValue();
					if (item.isNumber()) {
						tup.append(item.getAsInt());
					} else {
						tup.append(entry.getValue().getAsString());
					}

				} else {

					Tuple tmp = dig(entry.getValue());
					if (tmp.get(0).getClass() == DefaultDataBag.class) {
						tup.append(tmp.get(0));
					} else {
						tup.append(tmp);
					}

				}
			}

			return tup;

		}

		return null;

	}

}
