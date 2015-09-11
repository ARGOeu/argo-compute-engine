REGISTER /usr/libexec/ar-compute/lib/piggybank.jar
REGISTER /usr/libexec/ar-compute/lib/avro-1.7.4.jar
REGISTER /usr/libexec/ar-compute/lib/jackson-core-asl-1.8.8.jar
REGISTER /usr/libexec/ar-compute/lib/jackson-mapper-asl-1.8.8.jar
REGISTER /usr/libexec/ar-compute/lib/snappy-java-1.0.4.1.jar
REGISTER /usr/libexec/ar-compute/lib/json-simple-1.1.jar

REGISTER /usr/libexec/ar-compute/lib/mongo-hadoop-core.jar
REGISTER /usr/libexec/ar-compute/lib/mongo-hadoop-pig.jar
REGISTER /usr/libexec/ar-compute/lib/mongo-java-driver-2.11.4.jar

REGISTER /usr/libexec/ar-compute/lib/gson-2.2.4.jar

REGISTER /usr/libexec/ar-compute/MyUDF.jar

DEFINE f_PickEndpoints ar.PickEndpoints('$egs','$mps','$aps','$ggs','$cfg', '$flt' , '$mode');
DEFINE f_PrepStatus  status.PrepStatusDetails('$ggs','$egs','$cfg','$dt','$mode');
DEFINE f_EndpointAggr status.EndpointStatus('$ops', '$aps', '$mps', '$dt','$mode');


p_mdata = LOAD '$p_mdata' using org.apache.pig.piggybank.storage.avro.AvroStorage();
p_mdata_trim = FOREACH p_mdata GENERATE monitoring_host,service,hostname,metric,timestamp,status,summary,message;
p_mdata_clean = FILTER p_mdata_trim BY f_PickEndpoints(hostname,service,metric);

--- Produce the latest previous statuses as references
p_ref = FOREACH (GROUP p_mdata_clean BY (monitoring_host,service,hostname,metric)) {
	timeline = ORDER p_mdata_clean by timestamp DESC;
	big_t = limit timeline 1;
	GENERATE FLATTEN(big_t) as (monitoring_host,service,hostname,metric,timestamp,status,summary,message);
};

mdata = LOAD '$mdata' using org.apache.pig.piggybank.storage.avro.AvroStorage();
mdata_trim = FOREACH mdata GENERATE  monitoring_host,service,hostname,metric,timestamp,status,summary,message;
mdata_clean = FILTER mdata_trim BY f_PickEndpoints(hostname,service,metric);

mdata_full = UNION mdata_clean,p_ref;
-- Group by hostname,metric to create timelines
status_detail =	FOREACH  (GROUP mdata_full BY (monitoring_host,service,hostname,metric)) {
	t = ORDER mdata_full BY timestamp ASC; 
	GENERATE  FLATTEN( f_PrepStatus(group.monitoring_host, group.service, group.hostname, group.metric, t.(timestamp,status,summary,message)) );
};

status_unwrap = FOREACH status_detail GENERATE $0 as report, $1 as endpoint_group, $2 as monitoring_box, $3 as service, $4 as host, $5 as metric, FLATTEN($6) as (timestamp,status,summary,message,previous_state,previous_timestamp,date_integer,time_integer);

-- Continue here 
endpoint_aggr = FOREACH (GROUP status_unwrap BY(report,endpoint_group,service,host)) {
	t = ORDER status_unwrap BY metric ASC, timestamp ASC;
	GENERATE  FLATTEN(f_EndpointAggr(group.report, group.endpoint_group, group.service, group.host, t.(metric,timestamp,status,previous_state))) as (report,date_integer,endpoint_group,service,host,timeline);
}

endpoint_data = FOREACH endpoint_aggr GENERATE $0 as report, $1 as data_integer, $2 as endpoint_group, $3 as service, $4 as host, FLATTEN($5) as (timestamp,status);
---STORE status_unwrap INTO '$mongo_status_detail' USING com.mongodb.hadoop.pig.MongoInsertStorage();
---STORE service_aggr INTO 'yo.txt' USING PigStorage(',');
--- debug schemas

service_aggr = FOREACH (GROUP endpoint_data BY(report,endpoint_group,service)) {
	t = ORDER endpoint_data BY host ASC, timestamp ASC;
	GENERATE  group.report as report, group.endpoint_group as endpoint_group, service service, t.(host,timestamp);
}


--STORE endpoint_data INTO '$mongo_status_endpoints' USING com.mongodb.hadoop.pig.MongoInsertStorage();
STORE service_aggr INTO 'yo.txt' USING JsonStorage();
describe service_aggr;

