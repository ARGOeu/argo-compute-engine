package sync;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.log4j.Logger;

public class EndpointGroups {

	private static final Logger LOG = Logger.getLogger(EndpointGroups.class.getName());

	private ArrayList<EndpointItem> list;
	private ArrayList<EndpointItem> fList;

	private class EndpointItem {
		String type; // type of group
		String group; // name of the group
		String service; // type of the service
		String hostname; // name of host
		HashMap<String, String> tags; // Tag list

		public EndpointItem() {
			// Initializations
			this.type = "";
			this.group = "";
			this.service = "";
			this.hostname = "";
			this.tags = new HashMap<String, String>();
		}

		public EndpointItem(String type, String group, String service,
				String hostname, HashMap<String, String> tags) {
			this.type = type;
			this.group = group;
			this.service = service;
			this.hostname = hostname;
			this.tags = tags;

		}

	}

	public EndpointGroups() {
		this.list = new ArrayList<EndpointItem>();
		this.fList = new ArrayList<EndpointItem>();

	}

	public int insert(String type, String group, String service,
			String hostname, HashMap<String, String> tags) {
		EndpointItem new_item = new EndpointItem(type, group, service,
				hostname, tags);
		this.list.add(new_item);
		return 0; // All good
	}

	public boolean checkEndpoint(String hostname, String service) {

		for (EndpointItem item : fList) {
			if (item.hostname.equals(hostname) && item.service.equals(service)) {
				return true;
			}
		}

		return false;
	}

	public String getGroup(String type, String hostname, String service) {

		for (EndpointItem item : fList) {
			if (item.type.equals(type) && item.hostname.equals(hostname)
					&& item.service.equals(service)) {
				return item.group;
			}
		}

		return null;
	}

	public HashMap<String, String> getGroupTags(String type, String hostname,
			String service) {

		for (EndpointItem item : fList) {
			if (item.type.equals(type) && item.hostname.equals(hostname)
					&& item.service.equals(service)) {
				return item.tags;
			}
		}

		return null;
	}

	public int count() {
		return this.fList.size();
	}

	public void unfilter() {
		this.fList.clear();
		for (EndpointItem item : this.list) {
			this.fList.add(item);
		}
	}

	public void filter(TreeMap<String, String> fTags) {
		this.fList.clear();
		boolean trim;
		for (EndpointItem item : this.list) {
			trim = false;
			HashMap<String, String> itemTags = item.tags;
			for (Entry<String, String> fTagItem : fTags.entrySet()) {

				if (itemTags.containsKey(fTagItem.getKey())) {
					// First Check binary tags as Y/N 0/1

					if (fTagItem.getValue().equalsIgnoreCase("y")
							|| fTagItem.getValue().equalsIgnoreCase("n")) {
						String binValue = "";
						if (fTagItem.getValue().equalsIgnoreCase("y"))
							binValue = "1";
						if (fTagItem.getValue().equalsIgnoreCase("n"))
							binValue = "0";

						if (itemTags.get(fTagItem.getKey()).equalsIgnoreCase(
								binValue) == false) {
							trim = true;
						}
					} else if (itemTags.get(fTagItem.getKey())
							.equalsIgnoreCase(fTagItem.getValue()) == false) {
						trim = true;
					}

				}
			}

			if (trim == false) {
				fList.add(item);
			}
		}
	}

	public int loadAvro(File avroFile) throws IOException {

		// Prepare Avro File Readers
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
		DataFileReader<GenericRecord> dataFileReader = null;
		try {
			dataFileReader = new DataFileReader<GenericRecord>(avroFile,
					datumReader);

			// Grab Avro schema
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
				String group = avroRow.get("group").toString();
				String service = avroRow.get("service").toString();
				String hostname = avroRow.get("hostname").toString();

				// Insert data to list
				this.insert(type, group, service, hostname, tagMap);

			} // end of avro rows

			this.unfilter();

			

		} catch (IOException ex) {
			LOG.error("Could not open avro file:" + avroFile.getName());
			throw ex;
		} finally {
			if (dataFileReader != null) {
				try {
					dataFileReader.close();
				} catch (IOException ex) {
					LOG.error("Cannot close file:" + avroFile.getName());
					throw ex;
				}
			}
		}

		return 0; // allgood
	}

}
