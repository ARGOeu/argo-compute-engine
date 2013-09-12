#!/bin/bash

currentdate=$1
loopenddate=$(/bin/date --date "$2 1 day" +%Y-%m-%d)

until [ "$currentdate" == "$loopenddate" ]
do

  RUN_DATE=$currentdate
  RUN_DATE_UNDER=`echo $RUN_DATE | sed 's/-/_/g'`

  YEAR=`echo $RUN_DATE_UNDER | awk -F'_' '{print $1}'`
  MONTH=`echo $RUN_DATE_UNDER | awk -F'_' '{print $2}'`
  DAY=`echo $RUN_DATE_UNDER | awk -F'_' '{print $3}'`

 #<---- Commenting out this column of lines for re-running without re-generating raw_data table on hadoop --pkoro
 # ### run prefilter  (Input: date e.g. 2013-07-05) (Output: /var/lib/ar-sync/prefilter_%Y_%m_%d.out)
 # /usr/libexec/ar-sync/prefilter -d $RUN_DATE

 # ### upload prefilter's output on hdfs with name prefilter_%s_%s_%s.out
 # ### note that on hdfs the file will reside under /user/$username (i.e. /user/root if run as root)
 # hadoop fs -put /var/lib/ar-sync/prefilter_${RUN_DATE_UNDER}.out

 # ### use pig importer to get data from hdfs to database
 # ### processed data will reside under /user/hive/warehouse/raw_data on hdfs
 # pig -useHCatalog -param in_date=${RUN_DATE_UNDER} -f /usr/libexec/ar-compute/pig/InputHandler.pig
 # 
 # ### remove prefilter_ file from hdfs
 # hadoop fs -rm -skipTrash prefilter_${RUN_DATE_UNDER}.out

  ### run poem_sync and upload the profiles with name: poem_profiles.txt (remove previous poem_profiles.txt)
  hadoop fs -ls poem_profiles_range.txt
  if [ $? -eq 0 ]
  then
    echo "poem file exists"
    hadoop fs -rm -skipTrash poem_profiles_range.txt
  fi

  grep ROC_CRITICAL /var/lib/ar-sync/poem_sync_$RUN_DATE_UNDER.out > /var/lib/ar-sync/poem_sync_$RUN_DATE_UNDER.out.roc_critical
  hadoop fs -put /var/lib/ar-sync/poem_sync_$RUN_DATE_UNDER.out.roc_critical poem_profiles_range.txt

  ### run topology and upload the topology with name: topology.txt (remove previous topology.txt)
  hadoop fs -ls topology_range.txt
  if [ $? -eq 0 ]
  then
    echo "topology file exists"
    hadoop fs -rm -skipTrash topology_range.txt
  fi

  cat /var/lib/ar-sync/sites_$RUN_DATE_UNDER.out | sort -u > /var/lib/ar-sync/sites_$RUN_DATE_UNDER.out.clean
  cat /var/lib/ar-sync/sites_$RUN_DATE_UNDER.out.clean | sed 's/\x01/ /g' | grep " SRM " | sed 's/ SRM / SRMv2 /g' | sed 's/ /\x01/g' > /var/lib/ar-sync/topology_cache_$RUN_DATE_UNDER.txt
  cat /var/lib/ar-sync/topology_cache_$RUN_DATE_UNDER.txt >> /var/lib/ar-sync/sites_$RUN_DATE_UNDER.out.clean
  hadoop fs -put /var/lib/ar-sync/sites_$RUN_DATE_UNDER.out.clean topology_range.txt
  rm -f /var/lib/ar-sync/sites_$RUN_DATE_UNDER.out.clean /var/lib/ar-sync/topology_cache_$RUN_DATE_UNDER.txt

  ### run downtimes and upload the downtimes with name: downtimes.txt (remove previous downtimes.txt)
  hadoop fs -ls downtimes_range.txt
  if [ $? -eq 0 ]
  then
    echo "downtimes file exists"
    hadoop fs -rm -skipTrash downtimes_range.txt
  fi

  /usr/libexec/ar-sync/downtime-sync -d $RUN_DATE
  cat /var/lib/ar-sync/downtimes_$RUN_DATE.out | sed 's/\x01/ /g' | grep " SRM " | sed 's/ SRM / SRMv2 /g' | sed 's/ /\x01/g' > /var/lib/ar-sync/downtimes_cache_$RUN_DATE.out
  cat /var/lib/ar-sync/downtimes_cache_$RUN_DATE.out >> /var/lib/ar-sync/downtimes_$RUN_DATE.out
  hadoop fs -put /var/lib/ar-sync/downtimes_$RUN_DATE.out downtimes_range.txt
  rm -f /var/lib/ar-sync/downtimes_cache_$RUN_DATE.out

  ### run calculator.pig
  pig -useHCatalog -param in_date=$RUN_DATE -f /usr/libexec/ar-compute/pig/calculator.pig

  ### if everything worked out copy results back onto hard drive
  if [ $? -eq 0 ]
  then
      hadoop fs -get /user/hive/warehouse/sitereports/year=$YEAR/month=$MONTH/day=$DAY/part-r-00000 /var/lib/ar-compute/results-${RUN_DATE}.out
  fi

  currentdate=$(/bin/date --date "$currentdate 1 day" +%Y-%m-%d)

done

hadoop fs -rm -skipTrash topology_range.txt
hadoop fs -rm -skipTrash poem_profiles_range.txt
hadoop fs -rm -skipTrash downtimes_range.txt
