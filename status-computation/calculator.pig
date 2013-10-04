register /usr/libexec/ar-compute/MyUDF.jar
register /usr/lib/pig/datafu-0.0.4-cdh4.3.1.jar

define FirstTupleFromBag datafu.pig.bags.FirstTupleFromBag();
define APPLY_PROFILES    myudf.APPLY_PROFILES();

--- e.g. in_date = 2013-05-29, PREV_DATE = 2013-05-28
%declare PREV_DATE `date --date=@$(( $(date --date=$in_date +%s) - 86400 )) +'%Y-%m-%d'`
%declare PREV_DAY `date --date=@$(( $(date --date=$in_date +%s) - 86400 )) +'%d'`
%declare YEAR `echo $in_date | awk -F'-' '{print $1}'`
%declare MONTH `echo $in_date | awk -F'-' '{print $2}'`
%declare PREV_MONTH `date --date=@$(( $(date --date=$in_date +%s) - 86400 )) +'%m'`
%declare DAY `echo $in_date | awk -F'-' '{print $3}'`
%declare PREV_YEAR `date --date=@$(( $(date --date=$in_date +%s) - 86400 )) +'%Y'`

/*%declare DOWNTIMES `cat topology.txt`
%declare TOPOLOGY  `cat topology.txt`
%declare POEM      `cat poem.txt`
%declare HLP       `cat poem.txt`
*/

%declare DOWNTIMES `cat $downtimes_file | awk 'BEGIN {ORS="|"; RS="\r\n"} {print $0}' | gzip -c | base64 | awk 'BEGIN {ORS=""} {print $0}'`
%declare POEMS     `cat $poem_file      | cut -d $(echo -e '\x01') --output-delimiter=$(echo -e '\x01') -f "3 4 5 6"| grep "ch.cern.sam.ROC_CRITICAL" | sort -u | awk 'BEGIN {ORS="|"; RS="\n"} {print $0}' | gzip -c | base64 | awk 'BEGIN {ORS=""} {print $0}'`

--- SET mapred.min.split.size 3000000;
--- SET mapred.max.split.size 3000000;
--- SET pig.noSplitCombination true;

--- SET hcat.desired.partition.num.splits 9;

SET io.sort.factor 100;
SET mapred.job.shuffle.merge.percent 0.33;

--- LOADING PHASE ---
topology  = load 'topology_$in_date.txt' using PigStorage('\\u001') as (hostname:chararray, service_flavour:chararray, production:chararray, monitored:chararray, scope:chararray, site:chararray, ngi:chararray, infrastructure:chararray, certification_status:chararray, site_scope:chararray);

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
        GENERATE hostname, service_flavour, profile, vo, FLATTEN(APPLY_PROFILES(timeline_s, profile, '$PREV_DATE', hostname, service_flavour, '$in_date', '$DOWNTIMES', '$POEMS')) as (date, timeline);
};

timetables2 = FOREACH timetables GENERATE date as dates, hostname, service_flavour, profile, vo, myudf.TimelineToPercentage(*) as timeline;

--- Join profiles with log, so we have have for each log raw the possible applied profiles
topologed_j = JOIN timetables BY (hostname, service_flavour), topology BY (hostname, service_flavour) USING 'replicated';

topologed = FOREACH topologed_j
                GENERATE timetables::date as date, timetables::hostname as hostname,
                         timetables::service_flavour as service_flavour, timetables::profile as profile,
                         timetables::vo as vo, timetables::timeline as timeline,
                         topology::production as production, topology::monitored as monitored,
                         topology::scope as scope, topology::site as site, topology::ngi as ngi,
                         topology::infrastructure as infrastructure,
                         topology::certification_status as certification_status, topology::site_scope as site_scope;

topology_g = GROUP topologed BY (date, site, profile, production, monitored, scope, ngi, infrastructure, certification_status, site_scope);

topology = FOREACH topology_g {
        t = ORDER topologed BY service_flavour;
        GENERATE group.date as dates, group.site as site, group.profile as profile,
            group.production as production, group.monitored as monitored, group.scope as scope,
            group.ngi as ngi, group.infrastructure as infrastructure,
            group.certification_status as certification_status, group.site_scope as site_scope,
            FLATTEN(myudf.AggregateSiteAvailability(t)) as (availability, reliability, up, unknown, downtime);
};

STORE topology    INTO 'sitereports' USING org.apache.hcatalog.pig.HCatStorer();
STORE timetables2 INTO 'apireports'  USING org.apache.hcatalog.pig.HCatStorer();
