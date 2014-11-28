REGISTER /usr/libexec/ar-compute/lib/piggybank.jar
REGISTER /usr/libexec/ar-compute/lib/avro-1.7.4.jar
REGISTER /usr/libexec/ar-compute/lib/jackson-core-asl-1.8.8.jar
REGISTER /usr/libexec/ar-compute/lib/jackson-mapper-asl-1.8.8.jar
REGISTER /usr/libexec/ar-compute/lib/snappy-java-1.0.4.1.jar
REGISTER /usr/libexec/ar-compute/lib/json-simple-1.1.jar

REGISTER /usr/libexec/ar-compute/lib/mongo-hadoop-core.jar
REGISTER /usr/libexec/ar-compute/lib/mongo-hadoop-pig.jar
REGISTER /usr/libexec/ar-compute/lib/mongo-java-driver-2.11.4.jar

REGISTER /usr/libexec/ar-compute/MyUDF.jar

define PREVSTATE myudf.PrevState('$mongo_host','$mongo_port');

-- Load yesterday's statuses
YESTERDAY_RAW = LOAD '$prev_status' using org.apache.pig.piggybank.storage.avro.AvroStorage();
-- Load today's statuses 
TODAY_RAW = LOAD '$today_status' using org.apache.pig.piggybank.storage.avro.AvroStorage();

-- Trim yesterday statuses keep relevant fields 
YESTERDAY = FOREACH YESTERDAY_RAW GENERATE vo,vo_fqan,monitoring_box,roc,service_type,hostname,metric,timestamp,status,summary,message;
-- Trim today's statuses keep relevant fields
TODAY  = FOREACH TODAY_RAW GENERATE vo,vo_fqan,monitoring_box,roc,service_type,hostname,metric,timestamp,status,summary,message;

-- Group yesterday statuses, order each metric by descending timestamp
-- Select Latest metrics to calculate first previous state
LAST =	FOREACH  (GROUP YESTERDAY BY (vo,vo_fqan,monitoring_box,roc,service_type,hostname,metric)) {
	timeline = ORDER YESTERDAY by timestamp DESC;
	big_t = limit timeline 1;
	GENERATE FLATTEN(big_t) as (vo,vo_fqan,monitoring_box,roc,service_type,hostname,metric,timestamp,status,summary,message);

};

-- Union todays statuses with latest statuses from yesterday 
STATUS_FULL = UNION LAST, TODAY;

-- Group by hostname,metric to create timelines
STATUS_GROUP =	FOREACH  (GROUP STATUS_FULL BY (vo,vo_fqan,monitoring_box,roc,service_type,hostname,metric)) {
	t = ORDER STATUS_FULL BY timestamp ASC; 
	GENERATE  group.vo, group.vo_fqan, group.monitoring_box, group.roc, group.service_type, group.hostname, group.metric, t.(timestamp,status,summary,message);
};

-- Pass each timeline trough PREVSTATE function which calculates previous states and also assigns site info 
STATUS_FL1 = FOREACH STATUS_GROUP GENERATE FLATTEN(PREVSTATE(*)); 
-- Use flatten to unwind results (crossproduct)
STATUS_FL2 = FOREACH STATUS_FL1 GENERATE $0 as vo, $1 as vo_fqan, $2 as monitor_box,$3 as roc, $8 as site, $4 as service, $5 as hostname, $6 as metric, FLATTEN($7) as (timestamp,status,summary,message,prevstate,date_int,time_int);
-- Remove old dates 
STATUS_NEW = FILTER STATUS_FL2 BY date_int != (int)'$last_date';
-- Order results 
STATUS_ORD = ORDER STATUS_NEW BY vo,vo_fqan,monitor_box,roc,site,hostname,metric,timestamp;
-- Prepare for mongodb
STATUS_MONGO = FOREACH STATUS_ORD GENERATE vo as vo, vo_fqan as vof, monitor_box as mb, roc as roc, site as st, service as srv, hostname as h, metric as m, timestamp as ts, status as s, summary as sum, message as msg, prevstate as ps, date_int as di, time_int as ti;
-- Store to mongodb
STORE STATUS_MONGO INTO 'mongodb://$mongo_host:$mongo_port/AR.status_metric'     USING com.mongodb.hadoop.pig.MongoInsertStorage();
