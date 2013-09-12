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
 ### run prefilter  (Input: date e.g. 2013-07-05) (Output: /var/lib/ar-sync/prefilter_%Y_%m_%d.out)
 /usr/libexec/ar-sync/prefilter -d $RUN_DATE

 ### upload prefilter's output on hdfs with name prefilter_%s_%s_%s.out
 ### note that on hdfs the file will reside under /user/$username (i.e. /user/root if run as root)
 hadoop fs -put /var/lib/ar-sync/prefilter_${RUN_DATE_UNDER}.out

 ### use pig importer to get data from hdfs to database
 ### processed data will reside under /user/hive/warehouse/raw_data on hdfs
 pig -useHCatalog -param in_date=${RUN_DATE_UNDER} -f /usr/libexec/ar-compute/pig/InputHandler.pig
 
 ### remove prefilter_ file from hdfs
 hadoop fs -rm -skipTrash prefilter_${RUN_DATE_UNDER}.out
 
 currentdate=$(/bin/date --date "$currentdate 1 day" +%Y-%m-%d)

done
