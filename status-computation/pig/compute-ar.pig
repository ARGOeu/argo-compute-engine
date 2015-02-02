REGISTER /usr/libexec/ar-compute/lib/piggybank.jar
REGISTER /usr/libexec/ar-compute/lib/avro-1.7.4.jar
REGISTER /usr/libexec/ar-compute/lib/jackson-core-asl-1.8.8.jar
REGISTER /usr/libexec/ar-compute/lib/jackson-mapper-asl-1.8.8.jar
REGISTER /usr/libexec/ar-compute/lib/snappy-java-1.0.4.1.jar
REGISTER /usr/libexec/ar-compute/lib/json-simple-1.1.jar

REGISTER /usr/libexec/ar-compute/lib/mongo-hadoop-core.jar
REGISTER /usr/libexec/ar-compute/lib/mongo-hadoop-pig.jar
REGISTER /usr/libexec/ar-compute/lib/mongo-java-driver-2.11.4.jar

REGISTER /usr/libexec/ar-compute/dude/MyUDF.jar

DEFINE f_PickEndpoints ar.PickEndpoints('$endpoint_groups','$metric_profile');
DEFINE f_EndpointTl ar.EndpointTimelines('$ops','$dt');
DEFINE f_MetricTl ar.MetricTimelines('$ops','$dt');


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



metric_tline_raw  = FOREACH (GROUP mdata_full BY (service,hostname,metric)){
	t = ORDER mdata_full by timestamp ASC;
	generate group.service, group.hostname, group.metric, t.(status);
}


metric_tlines = FOREACH (GROUP mdata_full BY (service,hostname,metric)){
	t = ORDER mdata_full by timestamp ASC;
	generate f_MetricTl(group.service, group.hostname, group.metric, t.(timestamp, status));
}

-- Group by service,hostname to create endpoint timelines
endpoints =	FOREACH  (GROUP mdata_full BY (service,hostname)) {
	GENERATE FLATTEN(f_EndpointTl(group.service, group.hostname, mdata_full.(metric,timestamp,status)));
};


store metric_tline_raw into './log/metric_tline_raw';
store metric_tlines into './log/metric_tlines';
store endpoints into './log/endpoints';
