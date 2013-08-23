#!/bin/bash

RUN_DATE=`date  --date="2 day ago" +'%Y-%m-%d'`
RUN_DATE_UNDER=`date  --date="2 days ago" +'%Y_%m_%d'`

YEAR=`echo $RUN_DATE_UNDER | awk -F'_' '{print $1}'`
MONTH=`echo $RUN_DATE_UNDER | awk -F'_' '{print $2}'`
DAY=`echo $RUN_DATE_UNDER | awk -F'_' '{print $3}'`
### run prefilter  (Input: date e.g. 2013-07-05) (Output: prefilter-%Y-%m-%d.out)
cd /groups/arstats/prefilter/
python /groups/arstats/prefilter/prefilter.py -d $RUN_DATE

### upload prefilter's output on hdfs with name prefilter-%s-%s-%s.out
hadoop fs -put /groups/arstats/prefilter/prefilter-$RUN_DATE.out

### use pig importer to get data from hdfs to database
pig -useHCatalog -param in_date=$RUN_DATE -f /groups/arstats/pig/inputHandler.pig

### remove prefilter_ file from hdfs
hadoop fs -rm -skipTrash prefilter-$RUN_DATE.out

### run poem_sync kai upload the profiles with name: poem_profiles.txt (remove previews poem_profiles.txt)
hadoop fs -ls poem_profiles.txt
if [ $? -eq 0 ]
then
    echo "poem file exists"
    hadoop fs -rm -skipTrash poem_profiles.txt
fi

# /groups/arstats/poem_sync/poem-sync
hadoop fs -put /groups/arstats/poem_sync/poem_sync_$RUN_DATE_UNDER.out poem_profiles.txt

### run topology kai upload the topology with name: topology.txt (remove previews topology.txt)
hadoop fs -ls topology.txt
if [ $? -eq 0 ]
then
    echo "topology file exists"
    hadoop fs -rm -skipTrash topology.txt
fi

### groups/arstats/topology-sync/topology-sync
cat /groups/arstats/topology-sync/sites_$RUN_DATE_UNDER.out | sort -u > /groups/arstats/topology-sync/sites_$RUN_DATE_UNDER.out.clean
hadoop fs -put /groups/arstats/topology-sync/sites_$RUN_DATE_UNDER.out.clean topology.txt

### run downtimes kai upload the downtimes with name: downtimes.txt (remove previews downtimes.txt)
hadoop fs -ls downtimes.txt
if [ $? -eq 0 ]
then
    echo "downtimes file exists"
    hadoop fs -rm -skipTrash downtimes.txt
fi

python /groups/arstats/downtime/downtime_sync.py -d $RUN_DATE
hadoop fs -put /groups/arstats/downtime/downtimes_$RUN_DATE.out downtimes.txt

### run calculator.pig
cd /groups/arstats/pig/
pig -useHCatalog -param in_date=$RUN_DATE -f calculator.pig

### send mail
if [ $? -eq 0 ]
then
    hadoop fs -get /user/hive/warehouse/reports/year=$YEAR/month=$MONTH/day=$DAY/part-r-00000
    cat part-r-00000 | cut -d `echo -e '\x01'` --output-delimiter=', ' -f "1 2 3 4 5 6" | mail -s "AR all ok" andronat@lab.grid.auth.gr
    cat part-r-00000 | cut -d `echo -e '\x01'` --output-delimiter=', ' -f "1 2 3 4 5 6" | mail -s "AR all ok" pkoro@lab.grid.auth.gr
    cat part-r-00000 | cut -d `echo -e '\x01'` --output-delimiter=', ' -f "1 2 3 4 5 6" | mail -s "AR all ok" skanct@admin.grnet.gr
    rm -f part-r-00000
else
    sendmail -v pkoro@grid.auth.gr < /groups/arstats/pig/error.mail
    sendmail -v skanct@admin.grnet.gr < /groups/arstats/pig/error.mail
    sendmail -v andronat@lab.grid.auth.gr < /groups/arstats/pig/error.mail    
fi
