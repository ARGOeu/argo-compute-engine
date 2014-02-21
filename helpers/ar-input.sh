#!/bin/bash

# Copyright (c) 2013 GRNET S.A., SRCE, IN2P3 CNRS Computing Centre
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the
# License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an "AS
# IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language
# governing permissions and limitations under the License.
# 
# The views and conclusions contained in the software and
# documentation are those of the authors and should not be
# interpreted as representing official policies, either expressed
# or implied, of either GRNET S.A., SRCE or IN2P3 CNRS Computing
# Centre
# 
# The work represented by this source file is partially funded by
# the EGI-InSPIRE project through the European Commission's 7th
# Framework Programme (contract # INFSO-RI-261323) 

RUN_DATE=$1
RUN_DATE_UNDER=`echo $RUN_DATE | sed 's/-/_/g'`
PARTITIONDATE=`echo $RUN_DATE | sed 's/-//g'`
# Run prefilter
/usr/libexec/ar-sync/prefilter -d $RUN_DATE

# Upload onto hdfs
hive -e "LOAD DATA LOCAL INPATH \"/var/lib/ar-sync/prefilter_${RUN_DATE_UNDER}.out\" OVERWRITE INTO TABLE raw_data PARTITION (dates=${PARTITIONDATE})"

# Remove prefiltered file from local filesystem after it's been transferred on hdfs
/bin/rm -f /var/lib/ar-sync/prefilter_${RUN_DATE_UNDER}.out
  


