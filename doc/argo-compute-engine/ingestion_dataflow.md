---
title: Compute Engine documentation | ARGO
page_title: Compute Engine Ingestion | Ingestion Dataflow
font_title: 'fa fa-cog'
description: Description of the Ingestion Dataflow
---

## Description

Ingestion dataflow in the compute engine involves the orchestration of the following components:
 - ARGO Messaging API endpoint (properly configured)
 - Kafka broker network infrastructure
 - Apache Flume configuration on hadoop cluster (for ingestion of messages to HDFS)

##  API ingestion endpoint:  ARGO Messaging API in front of Compute Engine

The API ingestion endpoint will be implemented by configuring and running ARGO Messaging API service in front of the Compute Engine ( & hadoop cluster). Clients should be able to deliver a specific AVRO encoded payload based on already defined AVRO file spec. Type of AVRO payloads (and specs) include:
- Metric data
- Endpoint Topology sync data
- Group Endpoint Topology sync data
- Poem profile sync data
- Weight sync data
- Downtime sync data

The client should choose the right ARGO Messaging API project (based on tenant) and a specific topic based on the payload
For e.g. for sending metric data for tenant TA a request should be made to `/v1/projects/TA/metric_data`

Then the client should publish the corresponding message in encoded AVRO format and further encoded to base64.
Should also supply two message attributes:
- `type` : type of payload (metric_data,weight,downtime etc)
- `date` : partition date (daily) in the format: `YYYY-MM-DD`

An example of an encoded message for metric_data ingestion would be the following:
`
{
   "attributes": [
      {
         "key": "date",
         "value": "2015-10-06"
      },
      {
         "key": "type",
         "value": "metric_data"
      }
   ],
   "data": "CONTENTS_OF_BASE64_ENCODED_STRING_OF_AVRO_BINARY_REPRESENTATION",

}

`

## KAFKA Broker infrastructure

ARGO Messaging API will forward the client messages to a kafka backend. Kafka backend should be accessible by the API endpoint node and visible to the Compute Engine's Hadoop Cluster. Tenant namespacing is accomplished by the following kafka topic convention: "TENANT_NAME.topic_name". Names produced by this convention should not conflict with topic names already present in kafka infrastructure.


## Apache FLUME set-up and configuration for metric data

Apache Flume ingests relevant messages from the kafka back-end and stores them to HDFS destinations. In order to fill the role properly must be configured in the following fashion:

For each tenant Apache Flume should have a kafka source configured to the corresponding metric_data topic. Usually the topic is named 'TENANT_NAME.metric_data'. A corresponding memory channel should follow the kafka-source and the flume events should end in an HDFS sink.  

`KAFKA SOURCE > MEMORY CHANNEL > HDFS SINK`


## HDFS destinations

Flume dataflow store the metric data in the following hdfs destination:
`/argo/{TENANT}/metric_data/date={date_timestamp}`

Files are also accessible by hive if corresponding tables are created and set to those locations. Folder `./ingestion/hive/` includes scripts for creation of hive tables that query data on ingestion HDFS directories.

Avro schema's used should be stored in an accessible hdfs destination such as '/argo/schemas/'

## Apache Flume Configuration

Apache flume agent for ingestion of metric data is configured based on configuration template provided in `./ingestion/flume/conf/lume-agent-metric-data.conf.template`
Flume Decode interceptor must be build and the target `decode-interceptor.jar` placed on the node's `/usr/lib/flume-ng/plugins.d/decode-intereptor/lib` directory. Also Cloudera's CDK AvroEventSerializer.jar must be placed on the node's `/usr/lib/flume-ng/plugins.d/avro-serializer/lib`
