CREATE TABLE downtimes
  PARTITIONED BY (date STRING)
  ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  STORED as INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
  LOCATION '/user/root/argo/${hiveconf:tenant}'
  TBLPROPERTIES (
     'avro.schema.literal'='{"namespace": "argo.avro",
     "type": "record",
     "name": "downtimes",
     "fields": [
        {"name": "hostname", "type": "string"},
        {"name": "service", "type": "string"},
        {"name": "start_time", "type": "string"},
        {"name": "end_time", "type": "string"}
     ]
    }');
