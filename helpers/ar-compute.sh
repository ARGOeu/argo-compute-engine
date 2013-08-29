#!/bin/bash

RUN_DATE=`date  --date="2 day ago" +'%Y-%m-%d'`
RUN_DATE_UNDER=`date  --date="2 days ago" +'%Y_%m_%d'`

YEAR=`echo $RUN_DATE_UNDER | awk -F'_' '{print $1}'`
MONTH=`echo $RUN_DATE_UNDER | awk -F'_' '{print $2}'`
DAY=`echo $RUN_DATE_UNDER | awk -F'_' '{print $3}'`
### run prefilter  (Input: date e.g. 2013-07-05) (Output: /var/lib/ar-sync/prefilter_%Y_%m_%d.out)
/usr/libexec/ar-sync/prefilter -d $RUN_DATE

### upload prefilter's output on hdfs with name prefilter-%s-%s-%s.out
### note that on hdfs the file will reside under /user/$username (i.e. /user/root if run as root)
hadoop fs -put /var/lib/ar-sync/prefilter_${RUN_DATE_UNDER}.out

### use pig importer to get data from hdfs to database
### processed data will reside under /user/hive/warehouse/raw_data on hdfs
pig -useHCatalog -param in_date=${RUN_DATE_UNDER} -f /usr/libexec/ar-compute/pig/InputHandler.pig

### remove prefilter_ file from hdfs
hadoop fs -rm -skipTrash prefilter-${RUN_DATE_UNDER}.out

### run poem_sync and upload the profiles with name: poem_profiles.txt (remove previous poem_profiles.txt file if found)
hadoop fs -ls poem_profiles.txt
if [ $? -eq 0 ]
then
    echo "Poem file exists: Removing it now from hdfs"
    hadoop fs -rm -skipTrash poem_profiles.txt
fi

### Get poem file from /var/lib/ar-sync and put on hdfs the file /user/$username/poem_profiles.txt
hadoop fs -put /var/lib/ar-sync/poem_sync_${RUN_DATE_UNDER}.out poem_profiles.txt

### run topology and upload the topology with name: topology.txt (remove previous topology.txt file if found)
hadoop fs -ls topology.txt
if [ $? -eq 0 ]
then
    echo "Topology file exists: Removing it now from hdfs"
    hadoop fs -rm -skipTrash topology.txt
fi

### Get topology file from /var/lib/ar-sync and put on hdfs the file /user/$username/topology.txt
cat /var/lib/ar-sync/sites_${RUN_DATE_UNDER}.out | sort -u > /var/lib/ar-sync/sites_${RUN_DATE_UNDER}.out.clean
hadoop fs -put /var/lib/ar-sync/sites_${RUN_DATE_UNDER}.out.clean topology.txt

### run downtimes and upload the downtimes with name: downtimes.txt (remove previous downtimes.txt file id found)
hadoop fs -ls downtimes.txt
if [ $? -eq 0 ]
then
    echo "Downtimes file exists: Removing it now from hdfs"
    hadoop fs -rm -skipTrash downtimes.txt
fi

### Get downtimes dynamically and put on hdfs the file /user/$username/downtimes.txt
/usr/libexec/ar-sync/downtime-sync -d ${RUN_DATE}
hadoop fs -put /var/lib/ar-sync/downtimes_${RUN_DATE}.out downtimes.txt

### run calculator.pig
pig -useHCatalog -param in_date=$RUN_DATE -f /usr/libexec/ar-compute/pig/calculator.pig

### if everything worked out copy results back onto hard drive
if [ $? -eq 0 ]
then
    hadoop fs -get /user/hive/warehouse/reports/year=$YEAR/month=$MONTH/day=$DAY/part-r-00000 /var/lib/ar-compute/results-${RUN_DATE}.out
fi

### send mail
### feature commented by pkoro pn 29-Aug. Will be implemented on okeanos later on
###if [ $? -eq 0 ]
###then
###    hadoop fs -get /user/hive/warehouse/reports/year=$YEAR/month=$MONTH/day=$DAY/part-r-00000
###    cat part-r-00000 | cut -d `echo -e '\x01'` --output-delimiter=', ' -f "1 2 3 4 5 6" | mail -s "AR all ok" andronat@lab.grid.auth.gr
###    cat part-r-00000 | cut -d `echo -e '\x01'` --output-delimiter=', ' -f "1 2 3 4 5 6" | mail -s "AR all ok" pkoro@lab.grid.auth.gr
###    cat part-r-00000 | cut -d `echo -e '\x01'` --output-delimiter=', ' -f "1 2 3 4 5 6" | mail -s "AR all ok" skanct@admin.grnet.gr
###    rm -f part-r-00000
###else
###    sendmail -v pkoro@grid.auth.gr < /groups/arstats/pig/error.mail
###    sendmail -v skanct@admin.grnet.gr < /groups/arstats/pig/error.mail
###    sendmail -v andronat@lab.grid.auth.gr < /groups/arstats/pig/error.mail    
###fi
