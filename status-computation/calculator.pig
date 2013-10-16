register /usr/libexec/ar-compute/MyUDF.jar
register /usr/lib/pig/datafu-0.0.4-cdh4.3.1.jar

define FirstTupleFromBag datafu.pig.bags.FirstTupleFromBag();
define ApplyProfiles     myudf.APPLY_PROFILES();
define AddTopology       myudf.AddTopology();

--- e.g. in_date = 2013-05-29, PREV_DATE = 2013-05-28
%declare PREV_DATE `date --date=@$(( $(date --date=$in_date +%s) - 86400 )) +'%Y-%m-%d'`
%declare PREV_DAY `date --date=@$(( $(date --date=$in_date +%s) - 86400 )) +'%d'`
%declare YEAR `echo $in_date | awk -F'-' '{print $1}'`
%declare MONTH `echo $in_date | awk -F'-' '{print $2}'`
%declare PREV_MONTH `date --date=@$(( $(date --date=$in_date +%s) - 86400 )) +'%m'`
%declare DAY `echo $in_date | awk -F'-' '{print $3}'`
%declare PREV_YEAR `date --date=@$(( $(date --date=$in_date +%s) - 86400 )) +'%Y'`

/*%declare HLP       `cat poem.txt`*/

--- There is a bug in Pig that forbids shell output longer than 32kb. Issue: https://issues.apache.org/jira/browse/PIG-3515
%declare DOWNTIMES  `cat $downtimes_file`
%declare TOPOLOGY   `cat $topology_file1`
%declare TOPOLOGY2  `cat $topology_file2`
%declare TOPOLOGY3  `cat $topology_file3`
%declare POEMS      `cat $poem_file`

--- SET mapred.min.split.size 3000000;
--- SET mapred.max.split.size 3000000;
--- SET pig.noSplitCombination true;

--- SET hcat.desired.partition.num.splits 9;

SET io.sort.factor 100;
SET mapred.job.shuffle.merge.percent 0.33;

--- Get beacons (logs from previous day)
beacons_r = LOAD 'raw_data' USING org.apache.hcatalog.pig.HCatLoader();
beacons = FILTER beacons_r BY year=='$PREV_YEAR' AND month=='$PREV_MONTH' AND day=='$PREV_DAY' AND profile=='ch.cern.sam.ROC_CRITICAL';

--- Get current logs
current_logs_r = LOAD 'raw_data' USING org.apache.hcatalog.pig.HCatLoader();
current_logs = FILTER current_logs_r BY year=='$YEAR' AND month=='$MONTH' AND day=='$DAY' AND profile=='ch.cern.sam.ROC_CRITICAL';

--- Merge current logs with beacons
logs = UNION current_logs, beacons;

/*
logs_r = LOAD 'raw_data' USING org.apache.hcatalog.pig.HCatLoader();
logs   = FILTER logs_r BY ((year=='$PREV_YEAR' AND month=='$PREV_MONTH' AND day=='$PREV_DAY') OR (year=='$YEAR' AND month=='$MONTH' AND day=='$DAY')) AND profile=='ch.cern.sam.ROC_CRITICAL';

logs   = FILTER logs_r BY year>='$PREV_YEAR' AND month>='$PREV_MONTH' AND day>='$PREV_DAY'AND year<='$YEAR' AND month<='$MONTH' AND day<='$DAY' AND profile=='ch.cern.sam.ROC_CRITICAL';
*/
--- MAIN ALGORITHM ---

--- Group rows so we can have for each hostname and flavor, the applied poem profile with reports
profile_groups = GROUP logs BY (hostname, service_flavour, profile);

--- After the grouping, we append the actual rules of the POEM profiles
profiled_logs = FOREACH profile_groups 
        GENERATE group.hostname as hostname, group.service_flavour as service_flavour, 
                 group.profile as profile, 
                 FLATTEN(FirstTupleFromBag(logs.vo,null)) as vo, 
                 logs.(metric, status, time_stamp) as timeline;


--- We calculate the timelines and create an integral of all reports
timetables = FOREACH profiled_logs {
        timeline_s = ORDER timeline BY time_stamp;
        GENERATE hostname, service_flavour, profile, vo, FLATTEN(ApplyProfiles(timeline_s, profile, '$PREV_DATE', hostname, service_flavour, '$in_date', '$DOWNTIMES', '$POEMS')) as (date, timeline);
};

timetables2 = FOREACH timetables GENERATE date as dates, hostname, service_flavour, profile, vo, myudf.TimelineToPercentage(*) as timeline;

--- Join topology with logs, so we have have for each log raw all topology information
topologed = FOREACH timetables GENERATE dates, profile, vo, timeline, hostname, service_flavour, FLATTEN(AddTopology(hostname, service_flavour, '$TOPOLOGY', '$TOPOLOGY2', '$TOPOLOGY3'));

topology_g = GROUP topologed BY (dates, site, profile, production, monitored, scope, ngi, infrastructure, certification_status, site_scope);

topology = FOREACH topology_g {
        t = ORDER topologed BY service_flavour;
        GENERATE group.dates as dates, group.site as site, group.profile as profile,
            group.production as production, group.monitored as monitored, group.scope as scope,
            group.ngi as ngi, group.infrastructure as infrastructure,
            group.certification_status as certification_status, group.site_scope as site_scope,
            FLATTEN(myudf.AggregateSiteAvailability(t)) as (availability, reliability, up, unknown, downtime);
};

STORE topology    INTO 'sitereports' USING org.apache.hcatalog.pig.HCatStorer();
STORE timetables2 INTO 'apireports'  USING org.apache.hcatalog.pig.HCatStorer();
