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

DEFINE f_PickEndpoints ar.PickEndpoints('$rec','$egs','$mps','$aps','$ggs','$cfg', '$flt' , '$mode');
DEFINE f_PrepStatus  status.PrepStatusDetails('$ggs','$egs','$cfg','$dt','$mode');
DEFINE f_EndpointAggr status.EndpointStatus('$ops', '$aps', '$mps', '$dt','$mode');
DEFINE f_ServiceAggr status.ServiceStatus('$ops', '$aps', '$mps', '$dt','$mode');
DEFINE f_GroupEndpointAggr status.GroupEndpointStatus('$ops', '$aps', '$mps', '$dt','$mode');

p_mdata = LOAD '$p_mdata' using org.apache.pig.piggybank.storage.avro.AvroStorage();
p_mdata_trim = FOREACH p_mdata GENERATE monitoring_host,service,hostname,metric,timestamp,status,summary,message;
p_mdata_clean = FILTER p_mdata_trim BY f_PickEndpoints(hostname,service,metric,monitoring_host,timestamp);


--- Produce the latest previous statuses as references
p_ref = FOREACH (GROUP p_mdata_clean BY (service,hostname,metric)) {
	timeline = ORDER p_mdata_clean by timestamp DESC;
	big_t = limit timeline 1;
	GENERATE FLATTEN(big_t) as (monitoring_host,service,hostname,metric,timestamp,status,summary,message);
};

mdata = LOAD '$mdata' using org.apache.pig.piggybank.storage.avro.AvroStorage();
mdata_trim = FOREACH mdata GENERATE  monitoring_host,service,hostname,metric,timestamp,status,summary,message;
mdata_clean = FILTER mdata_trim BY f_PickEndpoints(hostname,service,metric,monitoring_host,timestamp);


mdata_full = UNION mdata_clean,p_ref;



-- Group by hostname,metric to create timelines
status_detail =	FOREACH  (GROUP mdata_full BY (service,hostname,metric)) {
	t = ORDER mdata_full BY timestamp ASC;
	GENERATE  FLATTEN( f_PrepStatus(group.service, group.hostname, group.metric, t.(timestamp,status,summary,message,monitoring_host)) );
};

status_final = FOREACH status_detail GENERATE $0, FLATTEN($1), $2, $3, $4, FLATTEN($5);


status_unwrap = FOREACH status_final GENERATE $0 as report, $1 as endpoint_group, $2 as service, $3 as host, $4 as metric,  $5 as timestamp, $6 as status, $7 as summary,$8 as message,$10 as previous_state,$11 as previous_timestamp,$12 as date_integer:int,$13 as time_integer:int;



-- Continue here
endpoint_aggr = FOREACH (GROUP status_unwrap BY(report,endpoint_group,service,host)) {
	t = ORDER status_unwrap BY metric ASC, timestamp ASC;
	GENERATE  FLATTEN(f_EndpointAggr(group.report, group.endpoint_group, group.service, group.host, t.(metric,timestamp,status,previous_state))) as (report,date_integer,endpoint_group,service,host,timeline);
}



service_aggr = FOREACH (GROUP endpoint_aggr BY(report,date_integer,endpoint_group,service))
		GENERATE FLATTEN(f_ServiceAggr(group.report, group.date_integer, group.endpoint_group, group.service, endpoint_aggr.(host,timeline))) as (report, date_integer, endpoint_group, service, timeline);




endpoint_group_aggr = FOREACH (GROUP service_aggr BY(report,date_integer,endpoint_group))
		GENERATE FLATTEN(f_GroupEndpointAggr(group.report, group.date_integer, group.endpoint_group, service_aggr.(service,timeline))) as (report, date_integer, endpoint_group, timeline);

endpoint_data = FOREACH endpoint_aggr GENERATE $0 as report, $1 as date_integer, $2 as endpoint_group, $3 as service, $4 as host, FLATTEN($5) as (timestamp,status);
service_data = FOREACH service_aggr GENERATE $0 as report, $1 as date_integer, $2 as endpoint_group, $3 as service, FLATTEN($4) as (timestamp,status);
endpoint_group_data = FOREACH endpoint_group_aggr GENERATE $0 as report, $1 as date_integer, $2 as endpoint_group, FLATTEN($3) as (timestamp,status);

STORE status_unwrap INTO '$mongo_status_metrics' USING com.mongodb.hadoop.pig.MongoInsertStorage();
STORE endpoint_data INTO '$mongo_status_endpoints' USING com.mongodb.hadoop.pig.MongoInsertStorage();
STORE service_data INTO '$mongo_status_services' USING com.mongodb.hadoop.pig.MongoInsertStorage();
STORE endpoint_group_data INTO '$mongo_status_endpoint_groups' USING com.mongodb.hadoop.pig.MongoInsertStorage();
