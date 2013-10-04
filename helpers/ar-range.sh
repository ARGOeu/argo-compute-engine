#!/bin/bash

currentdate=$1
loopenddate=$(/bin/date --date "$2 1 day" +%Y-%m-%d)

until [ "$currentdate" == "$loopenddate" ]; do

  RUN_DATE=$currentdate
  RUN_DATE_UNDER=`echo $RUN_DATE | sed 's/-/_/g'`

  YEAR=`echo $RUN_DATE_UNDER | awk -F'_' '{print $1}'`
  MONTH=`echo $RUN_DATE_UNDER | awk -F'_' '{print $2}'`
  DAY=`echo $RUN_DATE_UNDER | awk -F'_' '{print $3}'`

  ### prepare poems
  echo "Prepare poems for $RUN_DATE"
  grep ROC_CRITICAL /var/lib/ar-sync/poem_sync_$RUN_DATE_UNDER.out > /var/lib/ar-sync/poem_sync_$RUN_DATE_UNDER.out.roc_critical

  ### run topology and upload the topology with name: topology.txt (remove previous topology.txt)
  echo "Prepare topology for $RUN_DATE"
  hadoop fs -rm -skipTrash topology_$RUN_DATE.txt
  cat /var/lib/ar-sync/sites_$RUN_DATE_UNDER.out | sort -u > /var/lib/ar-sync/sites_$RUN_DATE_UNDER.out.clean
  cat /var/lib/ar-sync/sites_$RUN_DATE_UNDER.out.clean | sed 's/\x01/ /g' | grep " SRM " | sed 's/ SRM / SRMv2 /g' | sed 's/ /\x01/g' >> /var/lib/ar-sync/sites_$RUN_DATE_UNDER.out.clean
  hadoop fs -put /var/lib/ar-sync/sites_$RUN_DATE_UNDER.out.clean topology_$RUN_DATE.txt
  rm -f /var/lib/ar-sync/sites_$RUN_DATE_UNDER.out.clean

  ### prepare downtimes
  echo "Prepare downtimes for $RUN_DATE"
  # /usr/libexec/ar-sync/downtime-sync -d $RUN_DATE
  cat /var/lib/ar-sync/downtimes_$RUN_DATE.out | sed 's/\x01/ /g' | grep " SRM " | sed 's/ SRM / SRMv2 /g' | sed 's/ /\x01/g' > /var/lib/ar-sync/downtimes_cache_$RUN_DATE.out
  cat /var/lib/ar-sync/downtimes_$RUN_DATE.out >> /var/lib/ar-sync/downtimes_cache_$RUN_DATE.out

  ### run calculator.pig
  pig -useHCatalog -param in_date=$RUN_DATE -param downtimes_file=/var/lib/ar-sync/downtimes_cache_$RUN_DATE.out -param poem_file=/var/lib/ar-sync/poem_sync_$RUN_DATE_UNDER.out.roc_critical -f /usr/libexec/ar-compute/pig/calculator.pig

  rm -f /var/lib/ar-sync/poem_sync_$RUN_DATE_UNDER.out.roc_critical
  rm -f /var/lib/ar-sync/downtimes_cache_$RUN_DATE.out
  
  currentdate=$(/bin/date --date "$currentdate 1 day" +%Y-%m-%d)
  
done

echo "waiting for hadoop"

wait

hadoop fs -rm -skipTrash topology_*
