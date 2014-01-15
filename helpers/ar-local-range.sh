#!/bin/bash

currentdate=$1
loopenddate=$(/bin/date --date "$2 1 day" +%Y-%m-%d)

echo "Prepare beacons for $(/bin/date --date "$1 -1 day" +%Y-%m-%d)"
# /usr/libexec/ar-sync/prefilter -d $(/bin/date --date "$1 -1 day" +%Y-%m-%d)

cd /var/lib/ar-sync/

until [ "$currentdate" == "$loopenddate" ]; do

  RUN_DATE=$currentdate
  RUN_DATE_UNDER=`echo $RUN_DATE | sed 's/-/_/g'`
	
	echo "Run prefilter for $RUN_DATE"
	# /usr/libexec/ar-sync/prefilter -d $RUN_DATE
	
	echo "Delete $RUN_DATE from MongoDB"
	/usr/libexec/ar-compute/lib/mongo-date_delete.py $RUN_DATE
	
  ### prepare poems
  echo "Prepare poems for $RUN_DATE"
  cat poem_sync_$RUN_DATE_UNDER.out | cut -d $(echo -e '\x01') --output-delimiter=$(echo -e '\x01') -f "3 4 5 6" | sort -u | awk 'BEGIN {ORS="|"; RS="\n"} {print $0}' | gzip -c | base64 | awk 'BEGIN {ORS=""} {print $0}' > poem_sync_$RUN_DATE_UNDER.out.clean

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

  ### prepare weights
  echo "Prepare HEPSPEC for $RUN_DATE"
  cat hepspec_sync.out | awk 'BEGIN {ORS="|"; RS="\r\n"} {print $0}' | gzip -c | base64 | awk 'BEGIN {ORS=""} {print $0}' > hepspec_sync_$RUN_DATE_UNDER.zip

  ### run calculator.pig
  pig -x local -useHCatalog -param mongoServer="83.212.110.19:27017" -param input_path=/var/lib/ar-sync/prefilter_ -param out_path=/usr/libexec/ar-compute/output/ -param in_date=$RUN_DATE -param weights_file=hepspec_sync_$RUN_DATE_UNDER.zip -param downtimes_file=downtimes_$RUN_DATE.zip -param poem_file=poem_sync_$RUN_DATE_UNDER.out.clean -param topology_file1=sites_$RUN_DATE_UNDER.aa -param topology_file2=sites_$RUN_DATE_UNDER.ab -param topology_file3=sites_$RUN_DATE_UNDER.ac -f /usr/libexec/ar-compute/pig/local_calculator.pig

  rm -f poem_sync_$RUN_DATE_UNDER.out.clean
  rm -f downtimes_$RUN_DATE.zip
  rm -f hepspec_sync_$RUN_DATE_UNDER.zip
  rm -f sites_$RUN_DATE_UNDER.aa sites_$RUN_DATE_UNDER.ab sites_$RUN_DATE_UNDER.ac
  
  curr