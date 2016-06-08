CREATE TABLE endpoint_groups
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
     "name": "group_of_service_endpoints",
     "fields": [
            {"name": "type", "type": "string"},
            {"name": "group", "type": "string"},
            {"name": "service", "type": "string"},
            {"name": "hostname", "type": "string"},
            {"name": "tags", "type" : ["null", { "name" : "Tags",
                                                 "type" : "map",
                                                 "values" : ["int", "string"]
                                              }]
            }]
    }');
