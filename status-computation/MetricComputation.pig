register MyUDF.jar
register /usr/lib/pig/datafu-0.0.4-cdh4.3.0.jar

define FirstTupleFromBag datafu.pig.bags.FirstTupleFromBag();

--- e.g. in_date = 2013-05-29, PREV_DATE = 2013-05-28
%declare PREV_DATE `date --date=@$(( $(date --date=$in_date +%s) - 86400 )) +'%Y-%m-%d'`
%declare PREV_DAY `date --date=@$(( $(date --date=$in_date +%s) - 86400 )) +'%d'`
%declare YEAR `echo $in_date | awk -F'-' '{print $1}'`
%declare MONTH `echo $in_date | awk -F'-' '{print $2}'`
%declare DAY `echo $in_date | awk -F'-' '{print $3}'`

/*SET pig.exec.reducers.bytes.per.reducer 3000000;
SET mapred.min.split.size 3000000;
SET mapred.max.split.size 3000000;
SET pig.noSplitCombination true;*/

SET hcat.desired.partition.num.splits 9;

SET io.sort.factor 100;
SET mapred.job.shuffle.merge.percent 0.33;

/*SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec gz;
SET mapred.compress.map.output true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.GzipCodec;*/

/*SET pig.tmpfilecompression true
SET pig.tmpfilecompression.codec lzo*/

--- LOADING PHASE ---

--- Get beakons (logs from previous day)
beakons_r = LOAD 'row_data' USING org.apache.hcatalog.pig.HCatLoader();
beakons = FILTER beakons_r BY year=='$YEAR' AND month=='$MONTH' AND day=='$PREV_DAY';

--- Get current logs
current_logs_r = LOAD 'row_data' USING org.apache.hcatalog.pig.HCatLoader();
current_logs = FILTER current_logs_r BY year=='$YEAR' AND month=='$MONTH' AND day=='$DAY';

--- MAIN ALGORITHM ---

--- Merge current logs with beakons
logs = UNION current_logs, beakons;

--- For each row we append as a new column the names of the corresponding POEM profiles
appended_prof_logs = FOREACH logs 
                        GENERATE *, FLATTEN(myudf.AppendPOEMname( (chararray)service_flavour, (chararray)vo)) as (profile_name);

--- Group rows so we can have for each hostname and flavor, the applied poem profile with reports
profile_groups = GROUP appended_prof_logs BY (hostname, service_flavour, profile_name) PARALLEL 9;

--- After the grouping, we append the actual rules of the POEM profiles
profiled_logs = FOREACH profile_groups 
        GENERATE group.hostname as hostname, group.service_flavour as service_flavour, 
                 group.profile_name as profile_name, 
                 FLATTEN(FirstTupleFromBag(appended_prof_logs.vo,null)) as vo, 
                 appended_prof_logs.(metric, status, time_stamp) as timeline, 
                 myudf.AppendPOEMrules(group.service_flavour, group.profile_name);


--- We calculate the timelines and create an integral of all reports
timetables = FOREACH profiled_logs {
        timeline_s = ORDER timeline BY time_stamp;
        GENERATE hostname, service_flavour, profile_name, vo, myudf.APPLY_PROFILES(timeline_s, profile_metrics, '$PREV_DATE') as timeline;
};

--- Create a single file to save on HDFS
merged = GROUP timetables ALL PARALLEL 1;
merged_f = FOREACH merged GENERATE FLATTEN(timetables) as (hostname, service_flavour, profile_name, vo, timeline);

--- Store the results on Hive
STORE merged_f INTO 'ar' USING org.apache.hcatalog.pig.HCatStorer('year=$YEAR, month=$MONTH, day=$DAY');
