package sync;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

public class WeightGroups {

	private HashMap<String, ArrayList<WeightItem>> list;

	static Logger log = Logger.getLogger(WeightGroups.class.getName());

	private class WeightItem {
		String group; // name of the group
		String weight; // weight value

		public WeightItem() {
			// Initializations
			this.group = "";
			this.weight = "";

		}

		public WeightItem(String group, String weight) {
			this.group = group;
			this.weight = weight;
		}
	}

	public WeightGroups() {
		list = new HashMap<String, ArrayList<WeightItem>>();
	}

	public int insert(String type, String group, String weight) {
		WeightItem tmpItem = new WeightItem(group, weight);
		if (this.list.containsKey(type)) {
			this.list.get(type).add(tmpItem);
		} else {
			this.list.put(type, new ArrayList<WeightItem>());
			this.list.get(type).add(tmpItem);
		}

		return 0; // All good
	}

	public int getWeight(String type, String group) {
		if (list.containsKey(type)) {
			for (WeightItem item : list.get(type)) {
				if (item.group.equals(group)) {
					return Integer.parseInt(item.weight);
				}
			}
		}

		return 0;

	}

	public int loadAvro(File avroFile) throws IOException {

		// Prepare Avro File Readers
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
		DataFileReader<GenericRecord> dataFileReader = null;
		try {
			dataFileReader = new DataFileReader<GenericRecord>(avroFile,
					datumReader);

			// Grab avro schema
			Schema avroSchema = dataFileReader.getSchema();

			// Generate 1st level generic record reader (rows)
			GenericRecord avroRow = new GenericData.Record(avroSchema);

			// For all rows in file repeat
			while (dataFileReader.hasNext()) {
				// read the row
				avroRow = dataFileReader.next(avroRow);
				HashMap<String, String> tagMap = new HashMap<String, String>();

				// Generate 2nd level generic record reader (tags)
				GenericRecord tags = (GenericRecord) avroRow.get("tags");
				// Grab all available tag fields
				if (tags != null) {
					List<Field> tagList = tags.getSchema().getFields();
					// Prepare Hashmap

					// Iterate over tag fields & values
					for (Field item : tagList) {
						String fieldName = item.name(); // grab field name
						String fieldValue = null;
						// if field value not null store it as string value
						if (tags.get(fieldName) != null) {
							fieldValue = tags.get(fieldName).toString();
						}
						tagMap.put(fieldName, fieldValue); // update the tag
															// hashmap
					}
				}
				// Grab 1st level mandatory fields
				String type = avroRow.get("type").toString();
				String group = avroRow.get("site").toString();
				String weight = avroRow.get("weight").toString();

				// Insert data to list
				this.insert(type, group, weight);

			} // end of avro rows

			dataFileReader.close();

		} catch (IOException ex) {
			log.error("Could not open avro file:" + avroFile.getName());
			throw ex;
		} finally {
			if (dataFileReader != null) {
				try {
					dataFileReader.close();
				} catch (IOException ex) {
					log.error("Cannot close file:" + avroFile.getName());
					throw ex;
				}
			}
		}

		return 0; // allgood
	}

}
