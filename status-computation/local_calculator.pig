--- Copyright (c) 2013 GRNET S.A., SRCE, IN2P3 CNRS Computing Centre
---
--- Licensed under the Apache License, Version 2.0 (the "License");
--- you may not use this file except in compliance with the
--- License. You may obtain a copy of the License at
---
---     http://www.apache.org/licenses/LICENSE-2.0
---
--- Unless required by applicable law or agreed to in writing,
--- software distributed under the License is distributed on an "AS
--- IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
--- express or implied. See the License for the specific language
--- governing permissions and limitations under the License.
--- 
--- The views and conclusions contained in the software and
--- documentation are those of the authors and should not be
--- interpreted as representing official policies, either expressed
--- or implied, of either GRNET S.A., SRCE or IN2P3 CNRS Computing
--- Centre
--- 
--- The work represented by this source file is partially funded by
--- the EGI-InSPIRE project through the European Commission's 7th
--- Framework Programme (contract # INFSO-RI-261323) 

REGISTER /usr/libexec/ar-compute/MyUDF.jar
REGISTER /usr/libexec/ar-compute/lib/mongo-java-driver-2.11.4.jar   -- mongodb java driver  
REGISTER /usr/libexec/ar-compute/lib/mongo-hadoop-core.jar          -- mongo-hadoop core lib
REGISTER /usr/libexec/ar-compute/lib/mongo-hadoop-pig.jar           -- mongo-hadoop pig lib

define HST myudf.HostServiceTimelines();
define AT  myudf.AddTopology();
define SA  myudf.SiteAvailability();
define VOA myudf.VOAvailability();
define SFA myudf.SFAvailability();

--- e.g. in_date = 2013-05-29, PREV_DATE = 2013-05-28
%declare PREV_DATE `date --date=@$(( $(date --date=$in_date +%s) - 86400 )) +'%Y-%m-%d'`
%declare PREVDATE `echo $PREV_DATE | sed 's/-//g'`
%declare CUR_DATE `echo $in_date | sed 's/-//g'`

--- sanitize......
%declare IN_PREVDATE `echo $PREV_DATE | sed 's/-/_/g'`
%declare IN_CUR_DATE `echo $in_date | sed 's/-/_/g'`

--- There is a bug in Pig that forbids shell output longer than 32kb. Issue: https://issues.apache.org/jira/browse/PIG-3515
%declare DOWNTIMES  `cat $downtimes_file`
%declare TOPOLOGY   `cat $topology_file1`
%declare TOPOLOGY2  `cat $topology_file2`
%declare TOPOLOGY3  `cat $topology_file3`
%declare POEMS      `cat $poem_file`
%declare WEIGHTS    `cat $weights_file`
%declare HLP        `echo ""` --- `cat $hlp` --- high level profile.

SET mapred.child.java.opts -Xmx4048m
SET mapred.map.tasks.speculative.execution false
SET mapred.reduce.tasks.speculative.execution false

---SET mapred.min.split.size 3000000;
---SET mapred.max.split.size 3000000;
---SET pig.noSplitCombination true;

SET hcat.desired.partition.num.splits 2;

SET io.sort.factor 100;
SET mapred.job.shuffle.merge.percent 0.33;
SET io.sort.mb 50;
/*SET pig.udf.profile true;*/

--- Get beacons (logs from previous day)
beacons = load '$input_path$IN_PREVDATE.out' using PigStorage('\\u001') as (time_stamp:chararray, metric:chararray, service_flavour:chararray, hostname:chararray, status:chararray, vo:chararray, vofqan:chararray, profile:chararray);

--- Get current logs
current_logs = load '$input_path$IN_CUR_DATE.out' using PigStorage('\\u001') as (time_stamp:chararray, metric:chararray, service_flavour:chararray, hostname:chararray, status:chararray, vo:chararray, vofqan:chararray, profile:chararray);

--- Merge current logs with beacons
logs = UNION current_logs, beacons;

--- MAIN ALGORITHM ---

--- Group rows so we can have for each hostname and flavor, the applied poem profile with reports
--- After the grouping, we append the actual rules of the POEM profiles
profiled_logs = FOREACH (GROUP logs BY (hostname, service_flavour, profile) PARALLEL 4)
        GENERATE group.hostname as hostname, group.service_flavour as service_flavour,
                 group.profile as profile,
                 logs.vo as vo,
                 logs.(metric, status, time_stamp) as timeline;

--- We calculate the timelines and create an integral of all reports
timetables = FOREACH profiled_logs {
        timeline_s = ORDER timeline BY time_stamp;
        vos = DISTINCT vo;
        GENERATE hostname, service_flavour, profile, vos as vo,
            FLATTEN(HST(timeline_s, profile, '$PREV_DATE', hostname, service_flavour, '$CUR_DATE', '$DOWNTIMES', '$POEMS')) as (date, timeline);
};

--- Join topology with logs, so we have have for each log row, all topology information. Also append Availability Profiles.
topologed_j = FOREACH timetables GENERATE date, profile, timeline, hostname, service_flavour,
                 FLATTEN(AT(hostname, service_flavour, '$TOPOLOGY', '$TOPOLOGY2', '$TOPOLOGY3', '$mongoServer'));

topologed = FOREACH topologed_j GENERATE $0..$12, FLATTEN(availability_profiles) as availability_profile;

--- Group rows by important attributes. Note the date column, will be used for making a distinction in each day
--- After the grouping, we calculate AR for each site and append the weights
--- up, unknown, downtime columns are used for generalizing the calculation, so we can produce AR for months
sites = FOREACH (GROUP topologed BY (date, site, profile, production, monitored, scope, ngi, infrastructure, certification_status, site_scope, availability_profile) PARALLEL 3) {
        t = ORDER topologed BY service_flavour;
        GENERATE group.date as dates, group.site as site, group.profile as profile,
            group.production as production, group.monitored as monitored, group.scope as scope,
            group.ngi as ngi, group.infrastructure as infrastructure,
            group.certification_status as certification_status, group.site_scope as site_scope, group.availability_profile as availability_profile,
            FLATTEN(SA(t, group.availability_profile, '$WEIGHTS', group.site, '$mongoServer', group.date, group.ngi)) as (availability, reliability, up, unknown, downtime, weight);
};

--- Status computation for services
service_status = FOREACH timetables GENERATE date as dates, hostname, service_flavour, profile, vo, myudf.TimelineToPercentage(*) as timeline;

--- VO calculation
vo_s = FOREACH timetables GENERATE hostname, service_flavour, profile, date, FLATTEN(vo) as vo, timeline;

vo = FOREACH (GROUP vo_s BY (vo, profile, date) PARALLEL 4)
        GENERATE group.vo as vo, group.profile as profile, group.date as dates,
            FLATTEN(VOA(vo_s)) as (availability, reliability, up, unknown, downtime);

--- Group rows by important attributes. Note the date column, will be used for making a distinction in each day
--- Service flavor calculation
service_flavors = FOREACH (GROUP topologed BY (date, site, profile, production, monitored, scope, ngi, infrastructure, certification_status, site_scope) PARALLEL 3) {
    t = ORDER topologed BY service_flavour;
    GENERATE group.date as dates, group.site as site, group.profile as profile,
        group.production as production, group.monitored as monitored, group.scope as scope,
        group.ngi as ngi, group.infrastructure as infrastructure,
        group.certification_status as certification_status, group.site_scope as site_scope,
        FLATTEN(SFA(t)) as (availability, reliability, up, unknown, downtime, service_flavour);
};

--- OUTPUT SECTION

--- Fix format for MongoDB
sites_shrink = FOREACH sites
                   GENERATE dates as dt, site as s, profile as p, production as pr,
                            monitored as m, scope as sc, ngi as n, infrastructure as i,
                            certification_status as cs, site_scope as ss, availability_profile as ap,
                            availability as a, reliability as r, up as up, unknown as u, downtime as d, weight as hs;

service_status_shrink = FOREACH service_status
                            GENERATE dates as d, hostname as h, service_flavour as sf,
                                     profile as p, vo as vo, timeline as tm;

vo_shrink = FOREACH vo
               GENERATE dates as d, vo as v, profile as p, availability as a,
                        reliability as r, up as up, unknown as u, downtime as dt;

s_f_shrink = FOREACH service_flavors
                GENERATE dates as dt, site as s, profile as p, production as pr,
                         monitored as m, scope as sc, ngi as n, infrastructure as i,
                         certification_status as cs, site_scope as ss, availability as a, 
                         reliability as r, up as up, unknown as u, downtime as d, service_flavour as sf;

STORE sites_shrink          INTO 'mongodb://$mongoServer/AR.sites'     USING com.mongodb.hadoop.pig.MongoInsertStorage();
STORE service_status_shrink INTO 'mongodb://$mongoServer/AR.timelines' USING com.mongodb.hadoop.pig.MongoInsertStorage();
STORE vo_shrink             INTO 'mongodb://$mongoServer/AR.voreports' USING com.mongodb.hadoop.pig.MongoInsertStorage();
STORE s_f_shrink            INTO 'mongodb://$mongoServer/AR.sfreports' USING com.mongodb.hadoop.pig.MongoInsertStorage();
