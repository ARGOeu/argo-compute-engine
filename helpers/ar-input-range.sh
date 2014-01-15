#!/bin/bash

currentdate=$1
loopenddate=$(/bin/date --date "$2 1 day" +%Y-%m-%d)

until [ "$currentdate" == "$loopenddate" ]
do

  RUN_DATE=$currentdate
  RUN_DATE_UNDER=`echo $RUN_DATE | sed 's/-/_/g'`

  PARTITIONDATE=`echo $RUN_DATE | sed 's/-//g'`

 ### run prefilter  (Input: date e.g. 2013-07-05) (Output: /var/lib/ar-sync/prefilter_%Y_%m_%d.out)
 /usr/libexec/ar-sync/prefilter -d $RUN_DATE

 hive -e "LOAD DATA LOCAL INPATH \"/var/lib/ar-sync/prefilter_${RUN_DATE_UNDER}.out\" OVERWRITE INTO TABLE raw_data PARTITION (dates=${PARTITIONDATE})"
  
 currentdate=$(/bin/date --date "$currentdate 1 day" +%Y-%m-%d)

done

