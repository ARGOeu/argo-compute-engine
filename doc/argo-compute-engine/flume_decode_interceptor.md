---
title: Compute Engine documentation | ARGO
page_title: Compute Engine Ingestion | Flume Decode Interceptor
font_title: 'fa fa-cog'
description: Using a custom flume interceptor while ingesting data from kafka to hdfs
---

## Description

When using the ARGO Messaging API for ingestion in-front of the compute engine, a broker network is needed for the relay of messages and an ingestion mechanism for storing the data on HDFS. The ingestion mechanism used is the flume service.

ARGO Messaging API produces JSON messages in the following schema:

	{
	   "messageId": "12",
	   "attributes": [
	      {
		 "key": "foo",
		 "value": "bar"
	      }
	   ],
	   "data": "dGhpcyBpcyB0aGUgcGF5bG9hZA==",
	   "publishTime": "2016-03-09T13:02:21.139873825Z"
	}

Notice that the payload is included in the "data" field and is encoded in base64. Usually in the compute engine the
encoded payload when decoded will result in a binary AVRO file.

Flume is configured to listen to a KAFKA source (broker network) and transfer though a memory channel each event to an HDFS sink (destination).

##  The need for the interceptor

In order to keep only the integral part of the payload and transfer it decoded to it's final destination we need to define a flume
interceptor on the KAFKA source. Interceptors give the ability to flume to drop or modify events. Interceptors are actually JAVA classes that implement the Interceptor Interface. Flume comes with a bunch of build in interceptors but gives also the ability to define custom ones.

For the ingestion part of the Compute Engine a custom flume Decode Interceptor was developed as a small maven project.

## How to test the Decode Interceptor

The Decode Interceptor project is hosted in the directory `/flume/decode_interceptor` of the argo-compute-engine repo.

To build the jar file use the following commands:

	cd /flume/decode_interceptor
	mvn clean
	mvn package

The build process produces the following file:

	/flume/decode_interceptor/target/decode-interceptor-x.y.z-SNAPSHOT.jar

_(where x,y,z are integers used for versioning)_

To only test the interceptor, just issue

	cd /flume/decode_interceptor
	mvn test


## How to deploy the Decode Interceptor at flume node

This file must be deployed to the node that flume-agent runs
At flume-agent node, deploy the JAR file to the following path (please create if doesn't exist)

	/usr/lib/flume-ng/plugins.d/decode_interceptor/lib/


Then on the flume configuration file the interceptor must be added to the relevant source
for e.g.

_Contents of flume configuration file:_

	flume1.sources.kafka-source-1.interceptors = DecodeInterceptor
	flume1.sources.kafka-source-1.interceptors.DecodeInterceptor.type=argo.DecodeInterceptor$Builder
