CREATE TABLE weights
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
     "name": "weight_sites",
     "fields": [
        {"name": "type", "type": "string"},
        {"name": "site", "type": "string"},
        {"name": "weight", "type": "string"}
     ]
    }');
