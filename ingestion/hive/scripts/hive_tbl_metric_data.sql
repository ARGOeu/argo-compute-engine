CREATE TABLE metric_data
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
    "name": "metric_data",
    "fields": [
          {"name": "timestamp", "type": "string"},
          {"name": "service", "type": "string"},
          {"name": "hostname", "type": "string"},
          {"name": "metric", "type": "string"},
          {"name": "status", "type": "string"},
          {"name": "monitoring_host", "type": ["null", "string"]},
          {"name": "summary", "type": ["null", "string"]},
          {"name": "message", "type": ["null", "string"]},
          {"name": "tags", "type" : ["null", {"name" : "Tags",
                                               "type" : "map",
                                               "values" : ["null", "string"]
                                             }]
          }]
    }');
