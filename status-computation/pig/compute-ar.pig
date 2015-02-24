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

DEFINE f_PickEndpoints ar.PickEndpoints('$egs','$mps','$ggs','$cfg','$mode');
DEFINE f_EndpointTl ar.EndpointTimelines('$ops','$dts', '$aps', '$mps', '$dt','$mode','$s_period','$s_interval');
DEFINE f_MetricTl ar.MetricTimelines('$ops','$dt','$mode','$s_period','$s_interval');
DEFINE f_AddGroupInfo ar.AddGroupInfo('$egs','$ggs','$name_eg','$mode');
DEFINE f_ServiceTl ar.ServiceTimelines('$aps','$ops','$mode','$s_period','$s_interval');
DEFINE f_EndpointGroupTl ar.GroupEndpointTimelines('$ops', '$aps', '$rec', '$ggs', '$cfg', '$dt', '$mode','$s_period','$s_interval');
DEFINE f_EndpointGroupAR ar.GroupEndpointIntegrate('$ops', '$aps', '$mode');
DEFINE f_ServiceAR ar.ServiceIntegrate('$ops', '$aps', '$mode');
DEFINE f_egroupDATA ar.GroupEndpointMap('$cfg', '$aps', '$weight', '$ggs', '$egs', '$dt', '$mode', '$localCfg');
DEFINE f_ServiceDATA ar.ServiceMap('$cfg', '$aps', '$ggs', '$egs', '$dt', '$mode', '$localCfg');

--- Get One Day Before metric data in ordet to get past status references 
p_mdata = LOAD '$p_mdata' using org.apache.pig.piggybank.storage.avro.AvroStorage();
p_mdata_trim = FOREACH p_mdata GENERATE  service, hostname, metric, timestamp, status;
p_mdata_clean = FILTER p_mdata_trim BY f_PickEndpoints(hostname,service,metric);

--- Produce the latest previous statuses as references
p_ref = FOREACH (GROUP p_mdata_clean BY (service,hostname,metric)) {
	timeline = ORDER p_mdata_clean by timestamp DESC;
	big_t = limit timeline 1;
	GENERATE FLATTEN(big_t) as (service,hostname,metric,timestamp,status);
};


--- Get Target Day metric data 
mdata= LOAD '$mdata' using org.apache.pig.piggybank.storage.avro.AvroStorage();
mdata_trim = FOREACH mdata GENERATE  service, hostname, metric, timestamp, status;
mdata_clean = FILTER mdata_trim BY f_PickEndpoints(hostname,service,metric);

--- Union previous mdata with current 
mdata_full = UNION mdata_clean,p_ref;


endpoints =	FOREACH  (GROUP mdata_full BY (service,hostname)) {
	GENERATE FLATTEN(f_EndpointTl(group.service, group.hostname, mdata_full.(metric,timestamp,status)));
};

-- Add Group info (SITES)
endpoints_info = FOREACH endpoints GENERATE FLATTEN(f_AddGroupInfo(service,hostname,timeline)) as (service,hostname,timeline,groupname);

service_flavors = FOREACH (GROUP endpoints_info BY (groupname,service)) {
	GENERATE FLATTEN(f_ServiceTl(group.groupname, group.service , endpoints_info.(hostname,timeline))) as (groupname, service, timeline);
}

endpoint_groups = FOREACH (GROUP service_flavors BY (groupname)) {
	GENERATE FLATTEN(f_EndpointGroupTl(group, service_flavors.(service,timeline))) as (groupname,timeline);
}

service_ar = FOREACH service_flavors GENERATE FLATTEN(f_ServiceAR(groupname,service,timeline));
endpoint_groups_ar = FOREACH endpoint_groups GENERATE FLATTEN(f_EndpointGroupAR(groupname,timeline)) as (groupname,availability,reliability,up_f,unknown_f,down_f);
service_data = FOREACH service_ar GENERATE FLATTEN(f_ServiceDATA(service,groupname,availability,reliability,up_f,unknown_f,down_f)) AS ($s_map);
endpoint_groups_data = FOREACH endpoint_groups_ar GENERATE FLATTEN(f_egroupDATA(groupname,availability,reliability,up_f,unknown_f,down_f)) AS ($e_map); 

STORE service_data INTO '$mongo_service' USING com.mongodb.hadoop.pig.MongoInsertStorage();
STORE endpoint_groups_data INTO '$mongo_egroup' USING com.mongodb.hadoop.pig.MongoInsertStorage();
