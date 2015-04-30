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

public class Downtimes {

	private ArrayList<DowntimeItem> list;
	private static final Logger LOG = Logger.getLogger(Downtimes.class.getName());

	private class DowntimeItem {
		String hostname; // name of host
		String service; // name of service
		String startTime; // declare start time of downtime
		String endTime; // declare end time of downtime

		public DowntimeItem() {
			// Initializations
			this.hostname = "";
			this.service = "";
			this.startTime = "";
			this.endTime = "";
		}

		public DowntimeItem(String hostname, String service, String startTime,
				String endTime) {
			this.hostname = hostname;
			this.service = service;
			this.startTime = startTime;
			this.endTime = endTime;
		}

	}

	public Downtimes() {
		this.list = new ArrayList<DowntimeItem>();
	}

	public int insert(String hostname, String service, String startTime,
			String endTime) {
		DowntimeItem tmpItem = new DowntimeItem(hostname, service, startTime,
				endTime);
		this.list.add(tmpItem);
		return 0; // All good
	}

	public ArrayList<String> getPeriod(String hostname, String service) {

		ArrayList<String> period = new ArrayList<String>();

		for (DowntimeItem item : this.list) {

			if (item.hostname.equals(hostname)) {
				if (item.service.equals(service)) {
					period.add(item.startTime);
					period.add(item.endTime);
					return period;
				}
			}
		}

		return null;

	}

	public int loadAvro(File avroFile) throws IOException  {

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
				String hostname = avroRow.get("hostname").toString();
				String service = avroRow.get("service").toString();
				String startTime = avroRow.get("start_time").toString();
				String endTime = avroRow.get("end_time").toString();

				// Insert data to list
				this.insert(hostname, service, startTime, endTime);

			} // end of avro rows

			

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
