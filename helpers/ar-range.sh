#!/bin/bash

currentdate=$1
loopenddate=$(/bin/date --date "$2 1 day" +%Y-%m-%d)

cd /var/lib/ar-sync/

until [ "$currentdate" == "$loopenddate" ]; do

  RUN_DATE=$currentdate
  RUN_DATE_UNDER=`echo $RUN_DATE | sed 's/-/_/g'`

  YEAR=`echo $RUN_DATE_UNDER | awk -F'_' '{print $1}'`
  MONTH=`echo $RUN_DATE_UNDER | awk -F'_' '{print $2}'`
  DAY=`echo $RUN_DATE_UNDER | awk -F'_' '{print $3}'`

  ### prepare poems
  echo "Prepare poems for $RUN_DATE"
  grep ROC_CRITICAL poem_sync_$RUN_DATE_UNDER.out | cut -d $(echo -e '\x01') --output-delimiter=$(echo -e '\x01') -f "3 4 5 6"| grep "ch.cern.sam.ROC_CRITICAL" | sort -u | awk 'BEGIN {ORS="|"; RS="\n"} {print $0}' | gzip -c | base64 | awk 'BEGIN {ORS=""} {print $0}' > poem_sync_$RUN_DATE_UNDER.out.clean

  ### prepare topology
  echo "Prepare topology for $RUN_DATE"
  cat sites_$RUN_DATE_UNDER.out | sort -u > sites_$RUN_DATE_UNDER.out.clean
  cat sites_$RUN_DATE_UNDER.out.clean | sed 's/\x01/ /g' | grep " SRM " | sed 's/ SRM / SRMv2 /g' | sed 's/ /\x01/g' >> sites_$RUN_DATE_UNDER.out.clean
  cat sites_$RUN_DATE_UNDER.out.clean | awk 'BEGIN {ORS="|"; RS="\r\n"} {print $0}' | gzip -c | base64 | awk 'BEGIN {ORS=""} {print $0}'` > sites_$RUN_DATE_UNDER.zip
  rm -f sites_$RUN_DATE_UNDER.out.clean
  split -b 30092 sites_$RUN_DATE_UNDER.zip sites_$RUN_DATE_UNDER
  

  ### prepare downtimes
  echo "Prepare downtimes for $RUN_DATE"
  /usr/libexec/ar-sync/downtime-sync -d $RUN_DATE
  cat downtimes_$RUN_DATE.out | sed 's/\x01/ /g' | grep " SRM " | sed 's/ SRM / SRMv2 /g' | sed 's/ /\x01/g' > downtimes_cache_$RUN_DATE.out
  cat downtimes_$RUN_DATE.out >> downtimes_cache_$RUN_DATE.out
  cat downtimes_cache_$RUN_DATE.out | awk 'BEGIN {ORS="|"; RS="\r\n"} {print $0}' | gzip -c | base64 | awk 'BEGIN {ORS=""} {print $0}' > downtimes_$RUN_DATE.zip
  rm -f downtimes_cache_$RUN_DATE.out

  ### run calculator.pig
  pig -useHCatalog -param in_date=$RUN_DATE -param downtimes_file=downtimes_$RUN_DATE.zip -param poem_file=poem_sync_$RUN_DATE_UNDER.out.clean -param topology_file1=sites_$RUN_DATE_UNDERaa -param topology_file2=sites_$RUN_DATE_UNDERab -param topology_file3=sites_$RUN_DATE_UNDERac -f /usr/libexec/ar-compute/pig/calculator.pig

  rm -f poem_sync_$RUN_DATE_UNDER.out.clean
  rm -f downtimes_$RUN_DATE.zip
  rm -f sites_$RUN_DATE_UNDERaa sites_$RUN_DATE_UNDERab sites_$RUN_DATE_UNDERac
  
  currentdate=$(/bin/date --date "$currentdate 1 day" +%Y-%m-%d)
  
done

echo "waiting for hadoop"

wait
