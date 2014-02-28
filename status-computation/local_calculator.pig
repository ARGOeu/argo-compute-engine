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
REGISTER /usr/libexec/ar-local-compute/lib/mongo-java-driver-2.11.4.jar   -- mongodb java driver  
REGISTER /usr/libexec/ar-local-compute/lib/mongo-hadoop-core.jar          -- mongo-hadoop core lib
REGISTER /usr/libexec/ar-local-compute/lib/mongo-hadoop-pig.jar           -- mongo-hadoop pig lib

define ApplyProfiles     myudf.ApplyProfiles();
define AddTopology       myudf.AddTopology();

--- e.g. in_date = 2013-05-29, PREV_DATE = 2013-05-28
%declare PREV_DATE `date --date=@$(( $(date --date=$in_date +%s) - 86400 )) +'%Y-%m-%d'`
%declare PREVDATE `echo $PREV_DATE | sed 's/-//g'`
%declare CUR_DATE `echo $in_date | sed 's/-//g'`

--- sanitize......
%declare hack1 `echo sitereports_`
%declare OUT1 `echo $out_path$hack1$in_date`

%declare hack2 `echo apireports_`
%declare OUT2 `echo $out_path$hack2$in_date`

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

SET mapred.child.java.opts -Xmx2048m
SET mapred.map.tasks.speculative.execution false
SET mapred.reduce.tasks.speculative.execution false

---SET mapred.min.split.size 3000000;
---SET mapred.max.split.size 3000000;
---SET pig.noSplitCombination true;

SET hcat.desired.partition.num.splits 2;

SET io.sort.factor 100;
SET mapred.job.shuffle.merge.percent 0.33;
/*SET pig.udf.profile true;*/

--- Get beacons (logs from previous day)
beacons = load '$input_path$IN_PREVDATE.out' using PigStorage('\\u001') as (time_stamp:chararray, metric:chararray, service_flavour:chararray, hostname:chararray, status:chararray, vo:chararray, vofqan:chararray, profile:chararray);

--- Get current logs
current_logs = load '$input_path$IN_CUR_DATE.out' using PigStorage('\\u001') as (time_stamp:chararray, metric:chararray, service_flavour:chararray, hostname:chararray, status:chararray, vo:chararray, vofqan:chararray, profile:chararray);

--- Merge current logs with beacons
logs = UNION current_logs, beacons;

--- MAIN ALGORITHM ---

--- Group rows so we can have for each hostname and flavor, the applied poem profile with reports
profile_groups = GROUP logs BY (hostname, service_flavour, profile) PARALLEL 1;

--- After the grouping, we append the actual rules of the POEM profiles
profiled_logs = FOREACH profile_groups
        GENERATE group.hostname as hostname, group.service_flavour as service_flavour, 
                 group.profile as profile,
                 logs.vo as vo,
                 logs.(metric, status, time_stamp) as timeline;


--- We calculate the timelines and create an integral of all reports
timetables = FOREACH profiled_logs {
        timeline_s = ORDER timeline BY time_stamp;
        GENERATE hostname, service_flavour, profile, vo,
          FLATTEN(ApplyProfiles(timeline_s, profile, '$PREV_DATE', hostname, service_flavour, '$CUR_DATE', '$DOWNTIMES', '$POEMS')) as (date, timeline);
};

--- Join topology with logs, so we have have for each log raw all topology information
topologed = FOREACH timetables GENERATE date, profile, vo, timeline, hostname, service_flavour, FLATTEN(AddTopology(hostname, service_flavour, '$TOPOLOGY', '$TOPOLOGY2', '$TOPOLOGY3'));

topology_g = GROUP topologed BY (date, site, profile, production, monitored, scope, ngi, infrastructure, certification_status, site_scope) PARALLEL 1;

sites = FOREACH topology_g {
        t = ORDER topologed BY service_flavour;
        GENERATE group.date as dates, group.site as site, group.profile as profile,
            group.production as production, group.monitored as monitored, group.scope as scope,
            group.ngi as ngi, group.infrastructure as infrastructure,
            group.certification_status as certification_status, group.site_scope as site_scope,
            FLATTEN(myudf.AggregateSiteAvailability(t, '$HLP', '$WEIGHTS', group.site)) as (availability, reliability, up, unknown, downtime, weight);
};

--- Status computation for services
service_status = FOREACH timetables GENERATE date as dates, hostname, service_flavour, profile, vo, myudf.TimelineToPercentage(*) as timeline;

--- VO calculation
vo_s = FOREACH timetables {
    vos = DISTINCT vo;
    GENERATE hostname, service_flavour, profile, date, FLATTEN(vos) as vo, timeline;
}
vo_g = GROUP vo_s BY (vo, profile, date) PARALLEL 4;
vo = FOREACH vo_g GENERATE group.vo as vo, group.profile as profile, group.date as dates,
            FLATTEN(myudf.VOAvailability(vo_s)) as (availability, reliability, up, unknown, downtime);

--- Fix format for MongoDB
sites_shrink = FOREACH sites
                   GENERATE dates as dt, site as s, profile as p, production as pr,
                            monitored as m, scope as sc, ngi as n, infrastructure as i,
                            certification_status as cs, site_scope as ss, availability as a,
                            reliability as r, up as up, unknown as u, downtime as d, weight as hs;

service_status_shrink = FOREACH service_status
                            GENERATE dates as d, hostname as h, service_flavour as sf,
                                     profile as p, vo as vo, timeline as tm;

vo_shrink = FOREACH vo
                GENERATE dates as d, vo as v, profile as p, availability as a,
                         reliability as r, up as up, unknown as u, downtime as d;

s_f_shrink = FOREACH service_flavors
                GENERATE dates as dt, site as s, profile as p, production as pr,
                         monitored as m, scope as sc, ngi as n, infrastructure as i,
                         certification_status as cs, site_scope as ss, availability as a, 
                         reliability as r, up as up, unknown as u, downtime as d, weight as hs,
                         service_flavour as sf;

STORE sites_shrink          INTO 'mongodb://$mongoServer/AR.sites'     USING com.mongodb.hadoop.pig.MongoInsertStorage();
STORE service_status_shrink INTO 'mongodb://$mongoServer/AR.timelines' USING com.mongodb.hadoop.pig.MongoInsertStorage();
STORE vo_shrink             INTO 'mongodb://$mongoServer/AR.voreports' USING com.mongodb.hadoop.pig.MongoInsertStorage();
STORE s_f_shrink            INTO 'mongodb://$mongoServer/AR.sfreports' USING com.mongodb.hadoop.pig.MongoInsertStorage();
