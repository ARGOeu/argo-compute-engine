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

register /usr/libexec/ar-compute/MyUDF.jar

define HST myudf.HostServiceTimelines();
define AT  myudf.AddTopology();
define SA  myudf.SiteAvailability();
define VOA myudf.VOAvailability();
define SFA myudf.SFAvailability();

--- e.g. in_date = 2013-05-29, PREV_DATE = 2013-05-28
%declare PREV_DATE `date --date=@$(( $(date --date=$in_date +%s) - 86400 )) +'%Y-%m-%d'`
%declare PREVDATE `echo $PREV_DATE | sed 's/-//g'`
%declare CUR_DATE `echo $in_date | sed 's/-//g'`

--- There is a bug in Pig that forbids shell output longer than 32kb. Issue: https://issues.apache.org/jira/browse/PIG-3515
%declare DOWNTIMES  `cat $downtimes_file`
%declare TOPOLOGY   `cat $topology_file1`
%declare TOPOLOGY2  `cat $topology_file2`
%declare TOPOLOGY3  `cat $topology_file3`
%declare POEMS      `cat $poem_file`
%declare WEIGHTS    `cat $weights_file`
%declare HLP        `cat $hlp ` --- `cat $hlp` --- high level profiles.

---SET mapred.min.split.size 3000000;
---SET mapred.max.split.size 3000000;
---SET pig.noSplitCombination true;

/*SET hcat.desired.partition.num.splits 2;

SET io.sort.factor 100;
SET mapred.job.shuffle.merge.percent 0.33;
SET pig.udf.profile true;
*/
--- Get beacons (logs from previous day)
beacons_r = LOAD 'raw_data' USING org.apache.hcatalog.pig.HCatLoader();
beacons = FILTER beacons_r BY dates=='$PREV_DATE' AND profile=='ch.cern.sam.ROC_CRITICAL';

--- Get current logs
current_logs_r = LOAD 'raw_data' USING org.apache.hcatalog.pig.HCatLoader();
current_logs = FILTER current_logs_r BY dates=='$CUR_DATE' AND profile=='ch.cern.sam.ROC_CRITICAL';

--- Merge current logs with beacons
logs = UNION current_logs, beacons;

/*
logs_r = LOAD 'raw_data' USING org.apache.hcatalog.pig.HCatLoader();
logs   = FILTER logs_r BY ((dates=='$PREV_DATE') OR (dates=='$CUR_DATE')) AND profile=='ch.cern.sam.ROC_CRITICAL';
*/
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

--- Join topology with logs, so we have have for each log row, all topology information
topologed = FOREACH timetables GENERATE date, profile, timeline, hostname, service_flavour,
                    FLATTEN(AT(hostname, service_flavour, '$TOPOLOGY', '$TOPOLOGY2', '$TOPOLOGY3'));

--- Group rows by important attributes. Note the date column, will be used for making a distinction in each day
topologed_g = GROUP topologed BY (date, site, profile, production, monitored, scope, ngi, infrastructure, certification_status, site_scope) PARALLEL 3;

--- After the grouping, we calculate AR for each site and append the weights
--- up, unknown, downtime columns are used for generalizing the calculation, so we can produce AR for months
sites = FOREACH topologed_g {
        t = ORDER topologed BY service_flavour;
        GENERATE group.date as dates, group.site as site, group.profile as profile,
            group.production as production, group.monitored as monitored, group.scope as scope,
            group.ngi as ngi, group.infrastructure as infrastructure,
            group.certification_status as certification_status, group.site_scope as site_scope,
            FLATTEN(SA(t, '$HLP', '$WEIGHTS', group.site)) as (availability, reliability, up, unknown, downtime, weight);
};

--- Status computation for services
service_status = FOREACH timetables GENERATE date as dates, hostname, service_flavour, profile, vo, myudf.TimelineToPercentage(*) as timeline;

--- VO calculation
vo_s = FOREACH timetables GENERATE hostname, service_flavour, profile, date, FLATTEN(vo) as vo, timeline;

vo = FOREACH (GROUP vo_s BY (vo, profile, date) PARALLEL 4) 
				GENERATE group.vo as vo, group.profile as profile, group.date as dates,
						 FLATTEN(VOA(vo_s)) as (availability, reliability, up, unknown, downtime);

--- Service flavor calculation
service_flavors = FOREACH topologed_g {
    t = ORDER topologed BY service_flavour;
    GENERATE group.date as dates, group.site as site, group.profile as profile,
        group.production as production, group.monitored as monitored, group.scope as scope,
        group.ngi as ngi, group.infrastructure as infrastructure,
        group.certification_status as certification_status, group.site_scope as site_scope,
        FLATTEN(SFA(t)) as (availability, reliability, up, unknown, downtime, service_flavour);
};

STORE sites           INTO 'sitereports' USING org.apache.hcatalog.pig.HCatStorer();
STORE service_status  INTO 'apireports'  USING org.apache.hcatalog.pig.HCatStorer();
STORE vo              INTO 'voreports'   USING org.apache.hcatalog.pig.HCatStorer();
STORE service_flavors INTO 'sfreports'   USING org.apache.hcatalog.pig.HCatStorer();
