CREATE TABLE group_groups
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
     "name": "group_groups",
     "fields": [
        {"name": "type", "type": "string"},
        {"name": "group", "type": "string"},
        {"name": "subgroup", "type": "string"},
        {"name": "tags", "type" : ["null", { "name" : "Tags",
                                             "type" : "map",
                                             "values" : ["int", "string"]
                                          }]
        }]
    }');
