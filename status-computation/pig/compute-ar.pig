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
DEFINE f_EndpointTl ar.EndpointTimelines('$ops','$dts', '$aps', '$mps', '$dt','$mode','$s_period','$s_interval');
DEFINE f_MetricTl ar.MetricTimelines('$ops','$dt','$mode','$s_period','$s_interval');
DEFINE f_AddGroupInfo ar.AddGroupInfo('$egs','$ggs','$name_eg','$mode');
DEFINE f_ServiceTl ar.ServiceTimelines('$aps','$ops','$mode','$s_period','$s_interval');
DEFINE f_EndpointGroupTl ar.GroupEndpointTimelines('$ops', '$aps', '$rec', '$ggs', '$cfg', '$dt', '$mode','$s_period','$s_interval');
DEFINE f_EndpointGroupAR ar.GroupEndpointIntegrate('$ops', '$aps', '$mode');
DEFINE f_ServiceAR ar.ServiceIntegrate('$ops', '$aps', '$mode');
DEFINE f_egroupDATA ar.GroupEndpointMap('$cfg', '$aps', '$weight', '$ggs', '$egs', '$dt', '$mode', '$localCfg');
DEFINE f_ServiceDATA ar.ServiceMap('$cfg', '$aps', '$ggs', '$egs', '$dt', '$mode', '$localCfg');
DEFINE f_Unwind ar.UnwindServiceMetrics('$mps','$mode');
DEFINE f_Missing ar.FillMissing('$dt','$cfg','$mode');

--- Read the topology file
t_data = LOAD '$egs' using org.apache.pig.piggybank.storage.avro.AvroStorage();
t_data_trim = distinct t_data;
t_fill_a = FOREACH t_data_trim generate FLATTEN(f_Unwind(service,hostname));
t_fill_clean = FILTER t_fill_a by ($0 is not null);
t_fill = FOREACH t_fill_clean generate $0 as service, $1 as hostname, FLATTEN($2) as metric;




--- Get One Day Before metric data in ordet to get past status references



p_mdata = LOAD '$p_mdata' using org.apache.pig.piggybank.storage.avro.AvroStorage();
p_mdata_trim = FOREACH p_mdata GENERATE  monitoring_host, service, hostname, metric, timestamp, status;
p_mdata_clean = FILTER p_mdata_trim BY f_PickEndpoints(hostname,service,metric,monitoring_host,timestamp);
p_mdata_clean_trim = FOREACH p_mdata_clean GENERATE service,hostname,metric,timestamp,status;




--- Produce the latest previous statuses as references
p_ref = FOREACH (GROUP p_mdata_clean_trim BY (service,hostname,metric)) {
	timeline = ORDER p_mdata_clean_trim by timestamp DESC;
	big_t = limit timeline 1;
	GENERATE FLATTEN(big_t) as (service,hostname,metric,timestamp,status);
};


--- Get Target Day metric data
mdata= LOAD '$mdata' using org.apache.pig.piggybank.storage.avro.AvroStorage();
mdata_trim = FOREACH mdata GENERATE  monitoring_host, service, hostname, metric, timestamp, status;
mdata_clean = FILTER mdata_trim BY f_PickEndpoints(hostname,service,metric,monitoring_host,timestamp);
mdata_clean_trim = FOREACH mdata_clean GENERATE service,hostname,metric,timestamp,status;



--- Union previous mdata with current
mdata_full = UNION mdata_clean_trim,p_ref;

mdata_topo = FOREACH mdata_full GENERATE service,hostname,metric;
mdata_topo_trim = DISTINCT mdata_topo;



unwind_mdata = cogroup t_fill by *, mdata_topo_trim by *;
unwind_minus = filter unwind_mdata by IsEmpty(mdata_topo_trim);
missing_items = foreach unwind_minus generate flatten(t_fill);

missing = foreach missing_items generate FLATTEN(f_Missing(service,hostname,metric));
missing_final = foreach missing generate $2 as service, $3 as hostname, $4 as metric, $5 as timestamp, $6 as status;

mdata_final = UNION mdata_full, missing_final;


endpoints =	FOREACH  (GROUP mdata_final BY (service,hostname)) {
	GENERATE FLATTEN(f_EndpointTl(group.service, group.hostname, mdata_final.(metric,timestamp,status)));
};

-- Add Group info (SITES)
endpoints_info = FOREACH endpoints GENERATE FLATTEN(f_AddGroupInfo(service,hostname,timeline)) as (service,hostname,timeline,grouplist);

endpoints_info_final = FOREACH endpoints_info GENERATE service,hostname,timeline,FLATTEN(grouplist) as groupname;

service_flavors = FOREACH (GROUP endpoints_info_final BY (groupname,service)) {
	GENERATE FLATTEN(f_ServiceTl(group.groupname, group.service , endpoints_info_final.(hostname,timeline))) as (groupname, service, timeline);
}

endpoint_groups = FOREACH (GROUP service_flavors BY (groupname)) {
	GENERATE FLATTEN(f_EndpointGroupTl(group, service_flavors.(service,timeline))) as (groupname,timeline);
}

service_ar = FOREACH service_flavors GENERATE FLATTEN(f_ServiceAR(groupname,service,timeline));
endpoint_groups_ar = FOREACH endpoint_groups GENERATE FLATTEN(f_EndpointGroupAR(groupname,timeline)) as (groupname,availability,reliability,up_f,unknown_f,down_f);
service_data = FOREACH service_ar GENERATE FLATTEN(f_ServiceDATA(service,groupname,availability,reliability,up_f,unknown_f,down_f)) AS (report,date,name,supergroup,availability,reliability,up,down,unknown);
endpoint_groups_data = FOREACH endpoint_groups_ar GENERATE FLATTEN(f_egroupDATA(groupname,availability,reliability,up_f,unknown_f,down_f)) AS (report,date,name,supergroup,weight,availability,reliability,up,down,unknown);


STORE service_data INTO '$mongo_service'
	 USING com.mongodb.hadoop.pig.MongoUpdateStorage(
		  '{report:"\$report", date:"\$date", name:"\$name", supergroup:"\$supergroup" }',
			'{report:"\$report", date:"\$date", name:"\$name", supergroup:"\$supergroup", availability:"\$availability", reliability:"\$reliability", up:"\$up", down:"\$down", unknown:"\$unknown" }',
			'report: chararray,date: int,name: chararray,supergroup: chararray,availability: double,reliability: double,up: double,down: double,unknown: double',
			'{upsert:true}'
		 );
STORE endpoint_groups_data INTO '$mongo_egroup'
		USING com.mongodb.hadoop.pig.MongoUpdateStorage(
		 '{report:"\$report", date:"\$date", name:"\$name", supergroup:"\$supergroup" }',
		 '{report:"\$report", date:"\$date", name:"\$name", supergroup:"\$supergroup", weight:"\$weight", availability:"\$availability", reliability:"\$reliability", up:"\$up", down:"\$down", unknown:"\$unknown" }',
		 'report: chararray,date: int,name: chararray,supergroup: chararray,weight: int,availability: double,reliability: double,up: double,down: double,unknown: double',
		 '{upsert:true}'
		);
