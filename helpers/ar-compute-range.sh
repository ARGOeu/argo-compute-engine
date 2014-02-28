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

mongoDBServer="83.212.110.19:27017"
currentdate=$1
loopenddate=$(/bin/date --date "$2 1 day" +%Y-%m-%d)

cd /var/lib/ar-sync/

until [ "$currentdate" == "$loopenddate" ]; do

  RUN_DATE=$currentdate
  RUN_DATE_UNDER=`echo $RUN_DATE | sed 's/-/_/g'`

  ### prepare MongoDB by cleaning the collections
  echo "Delete $RUN_DATE from MongoDB"
  /usr/libexec/ar-compute/lib/mongo-date_delete.py $RUN_DATE

  ### prepare poems
  echo "Prepare poems for $RUN_DATE"
  grep ROC_CRITICAL poem_sync_$RUN_DATE_UNDER.out | cut -d $(echo -e '\x01') --output-delimiter=$(echo -e '\x01') -f "3 4 5 6"| grep "ch.cern.sam.ROC_CRITICAL" | sort -u | awk 'BEGIN {ORS="|"; RS="\n"} {print $0}' | gzip -c | base64 | awk 'BEGIN {ORS=""} {print $0}' > poem_sync_$RUN_DATE_UNDER.out.clean

  ### prepare topology
  echo "Prepare topology for $RUN_DATE"
  cat sites_$RUN_DATE_UNDER.out | sort -u > sites_$RUN_DATE_UNDER.out.clean
  cat sites_$RUN_DATE_UNDER.out.clean | sed 's/\x01/ /g' | grep " SRM " | sed 's/ SRM / SRMv2 /g' | sed 's/ /\x01/g' >> sites_$RUN_DATE_UNDER.out.clean
  cat sites_$RUN_DATE_UNDER.out.clean | awk 'BEGIN {ORS="|"; RS="\r\n"} {print $0}' | gzip -c | base64 | awk 'BEGIN {ORS=""} {print $0}' > sites_$RUN_DATE_UNDER.zip
  rm -f sites_$RUN_DATE_UNDER.out.clean
  split -b 30092 sites_$RUN_DATE_UNDER.zip sites_$RUN_DATE_UNDER.
  rm -f sites_$RUN_DATE_UNDER.zip
  
  ### prepare downtimes
  echo "Prepare downtimes for $RUN_DATE"
  /usr/libexec/ar-sync/downtime-sync -d $RUN_DATE
  cat downtimes_$RUN_DATE.out | sed 's/\x01/ /g' | grep " SRM " | sed 's/ SRM / SRMv2 /g' | sed 's/ /\x01/g' > downtimes_cache_$RUN_DATE.out
  cat downtimes_$RUN_DATE.out >> downtimes_cache_$RUN_DATE.out
  cat downtimes_cache_$RUN_DATE.out | awk 'BEGIN {ORS="|"; RS="\r\n"} {print $0}' | gzip -c | base64 | awk 'BEGIN {ORS=""} {print $0}' > downtimes_$RUN_DATE.zip
  rm -f downtimes_cache_$RUN_DATE.out 

  ### prepare high level profiles
  echo "Prepare High Level Profiles for $RUN_DATE"
  cat hlp.out | awk 'BEGIN {ORS="|"; RS="\n"} {print $0}' | gzip -c | base64 | awk 'BEGIN {ORS=""} {print $0}' > hlp_$RUN_DATE_UNDER.zip
	
  ### prepare weights
  echo "Prepare HEPSPEC for $RUN_DATE"
  cat hepspec_sync_$RUN_DATE.out | awk 'BEGIN {ORS="|"; RS="\r\n"} {print $0}' | gzip -c | base64 | awk 'BEGIN {ORS=""} {print $0}' > hepspec_sync_$RUN_DATE_UNDER.zip

  ### run calculator.pig
  pig -useHCatalog -param in_date=$RUN_DATE \
      -param mongoServer=$mongoDBServer \
      -param hlp=hlp_$RUN_DATE_UNDER.zip \
      -param weights_file=hepspec_sync_$RUN_DATE_UNDER.zip \
      -param downtimes_file=downtimes_$RUN_DATE.zip \
      -param poem_file=poem_sync_$RUN_DATE_UNDER.out.clean \
      -param topology_file1=sites_$RUN_DATE_UNDER.aa \
      -param topology_file2=sites_$RUN_DATE_UNDER.ab \
      -param topology_file3=sites_$RUN_DATE_UNDER.ac \
      -f /usr/libexec/ar-compute/pig/calculator.pig

  rm -f poem_sync_$RUN_DATE_UNDER.out.clean
  rm -f downtimes_$RUN_DATE.zip
  rm -f hepspec_sync_$RUN_DATE_UNDER.zip
  rm -f hlp_$RUN_DATE_UNDER.zip
  rm -f sites_$RUN_DATE_UNDER.aa sites_$RUN_DATE_UNDER.ab sites_$RUN_DATE_UNDER.ac
  
  currentdate=$(/bin/date --date "$currentdate 1 day" +%Y-%m-%d)
  
done
