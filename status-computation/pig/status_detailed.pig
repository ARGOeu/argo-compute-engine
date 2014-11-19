
-- OUR CUSTOM UDF 
REGISTER /usr/libexec/ar-compute/MyUDF.jar
-- FOR AVRO USAGE ANS SUPPORT
REGISTER /usr/libexec/ar-compute/piggybank.jar
REGISTER /usr/libexec/ar-compute/lib/avro-1.7.4.jar
REGISTER /usr/libexec/ar-compute/lib/jackson-mapper-asl-1.8.8.jar
REGISTER /usr/libexec/ar-compute/lib/jackson-core-asl-1.8.8.jar
REGISTER /usr/libexec/ar-compute/lib/snappy-java-1.0.4.1.jar
REGISTER /usr/libexec/ar-compute/lib/json-simple-1.1.jar
-- FOR MONGO USAGE ANS SUPPORT
REGISTER /usr/libexec/ar-compute/lib/mongo-hadoop-core.jar
REGISTER /usr/libexec/ar-compute/lib/mongo-hadoop-pig.jar
REGISTER /usr/libexec/ar-compute/lib/mongo-java-driver-2.11.4.jar

-- DEFINE THE PREVIOUS STATE UDF 
define PREVSTATE myudf.PrevState();

-- LOAD YESTERDAYS STATUSES FROM AN AVRO FILE
STATUS_OLD_R = LOAD '$prev_status' USING org.apache.pig.piggybank.storage.avro.AvroStorage();
-- KEEP ONLY RELEVANT FIELDS
STATUS_OLD = foreach STATUS_OLD_R generate $0,$1,$2,$3,$4,$5,$6,$7,$8,$9,$10;

-- LOAD TODAYS STATUSES FROM AN AVRO FILE
STATUS_R = LOAD '$daily_status' USING org.apache.pig.piggybank.storage.avro.AvroStorage();
-- KEEP ONLY RELEVANT FIELDS
STATUS = foreach STATUS_R generate $0,$1,$2,$3,$4,$5,$6,$7,$8,$9,$10;

-- GROUP YESTERDAYS COLLECTION BY HOSTNAME+SERVICE TYPE + METRIC AND ARRANGE ORDER ROWS BY TIMESTAMP
-- USE LIMIT 1 TO FIND THE LATEST STATUS REPORT FOR EACH METRIC
LAST =	FOREACH  (GROUP STATUS_OLD BY (hostName,serviceType,metricName)) {
	timeline = ORDER STATUS_OLD by timestamp DESC;
	big_t = limit timeline 1;
	GENERATE FLATTEN(big_t) as (timestamp,ROC,nagios_host,metricName,serviceType,hostName,metricStatus,vo_name,vo_fqan,summary,message);

};

-- ADD YESTERDAYS LAST STATUSES WITH TODAYS STATUSES BY PERFORMING A UNION
STATUS_FULL = UNION STATUS , LAST;
-- ORDER DATA 
STATUS_ORDER = ORDER STATUS_FULL BY hostName,serviceType,metricName,timestamp ASC;

-- CALL UDF FOR EACH ROW SEQUENTIALLY TO CALCULATE PREVIOUS STATES
STATUS_PR = FOREACH STATUS_ORDER GENERATE FLATTEN(PREVSTATE(timestamp,ROC,nagios_host,metricName,serviceType,hostName,metricStatus,voName,voFqan,summary,message)) PARALLEL 1;

-- CLEAR YESTERDAYS' ROW ENTRIES
STATUS_C = FILTER STATUS_PR BY date_int != (int)'$lastdate';

-- LOAD SITES FILE 
SITES = LOAD '$sites' using PigStorage('\u0001') as (hostName:chararray,serviceType:chararray,production:chararray,monitored:chararray,scope:chararray,site:chararray,ngi:chararray,infrastructure:chararray,certificationStatus:chararray,sitescope:chararray);
-- KEEP ONLY THE FIELDS WE NEED FOR JOIN (HOSTNAME,SITE)
SITES_S = FOREACH SITES GENERATE hostName,site;


-- PERFORM A JOIN OF STATUS ROWS WITH SITES_S BY HOSTNAME -- EACH STATUS ROW GETS THE SITE INFORMATION
STATUS_SITE = JOIN STATUS_C by hostname LEFT OUTER, SITES_S by hostName;

-- PREPARE FIELDS FOR MONGO STORAGE (2-3 CHARS MAX)
STATUS_FIN = FOREACH STATUS_SITE GENERATE STATUS_C::status_detail::timestamp as ts, STATUS_C::status_detail::roc as roc, 
					 					  STATUS_C::status_detail::nagios_host as mb, STATUS_C::status_detail::metric_type as mn, 
					 					  STATUS_C::status_detail::service_type as sf, STATUS_C::status_detail::hostname as sh, 
					 					  STATUS_C::status_detail::metric_status as s, STATUS_C::status_detail::vo_name as vo, 
					 					  STATUS_C::status_detail::vo_fqan as vof, STATUS_C::status_detail::summary as sum, 
					 					  STATUS_C::status_detail::message as msg, STATUS_C::status_detail::date_int as di, 
					 					  STATUS_C::status_detail::time_int as ti, STATUS_C::status_detail::prev_state as ps, 
					 					  SITES_S::site as si;



-- STORE INTO MONGO
STORE STATUS_FIN   INTO 'mongodb://$mongoServer/AR.new_status'     USING com.mongodb.hadoop.pig.MongoInsertStorage();


