CREATE TABLE metric_profiles
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
     "name": "metric_profiles",
     "fields": [
        {"name": "profile", "type": "string"},
        {"name": "service", "type": "string"},
        {"name": "metric", "type": "string"},
        {"name": "tags", "type" : ["null", {"name" : "Tags",
                                            "type" : "map",
                                            "values" : ["int", "string"]
                                           }]
        }]
    }');
