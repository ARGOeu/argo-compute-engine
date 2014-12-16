#!/usr/bin/env bash
# Create or ensure empty /tmp/avro directory
set -x
echo "clean /tmp/avro"
mkdir -p /tmp/avro
rm -f /tmp/avro/*
# Extract target day from argument and calculate one day before
DAY_TARGET=$1
DAY_UNDER=`echo $DAY_TARGET | sed 's/-/_/g'`
echo "Target day will be $DAY_TARGET"
DAY_BEFORE=$(date -d "$DAY_TARGET -1 day" +%Y-%m-%d)
DAY_BEFORE_UNDER=`echo $DAY_BEFORE | sed 's/-/_/g'`
DAY_BEFORE_INT=`echo $DAY_BEFORE | sed 's/-//g'`
echo "Day before will be $DAY_BEFORE"
# Remove duplicates for target day and day before consumer files
echo "Remove consumer data duplicates for day $DAY_BEFORE"
cat /var/lib/ar-consumer/ar-consumer_log_details_$DAY_BEFORE.txt | sort -u > /tmp/avro/ar-consumer_log_$DAY_BEFORE.min
echo "Remove consumer data duplicates for day $DAY_TARGET"
cat /var/lib/ar-consumer/ar-consumer_log_details_$DAY_TARGET.txt | sort -u > /tmp/avro/ar-consumer_log_$DAY_TARGET.min
# Encode in avro format
echo "Encode avro file for day $DAY_BEFORE"
java -jar avro_encoder.jar consumer_detail.avsc /tmp/avro/ar-consumer_log_$DAY_BEFORE.min /tmp/avro/ar-consumer_log_$DAY_BEFORE.avro
echo "Encode avro file for day $DAY_TARGET"
java -jar avro_encoder.jar consumer_detail.avsc /tmp/avro/ar-consumer_log_$DAY_TARGET.min /tmp/avro/ar-consumer_log_$DAY_TARGET.avro
# Clean HDFS directory 
echo "clean HDFS ../tmp/avro"
hadoop fs -mkdir -p ./tmp/avro
hadoop fs -rm -r -f ./tmp/avro/*
# Put site file to HDFS
echo "Put site info to hdfs"
hadoop fs -put /var/lib/ar-sync/sites_$DAY_BEFORE_UNDER.out ./tmp/avro
# Put files to hdfs
#echo "Put status detailed info to hdfs"
hadoop fs -put /tmp/avro/ar-consumer_log_$DAY_BEFORE.avro ./tmp/avro
hadoop fs -put /tmp/avro/ar-consumer_log_$DAY_TARGET.avro ./tmp/avro
# Clean MongoDB from data
echo "MongoDB: Delete status_metric data for day $DAY_TARGET"
/usr/libexec/ar-compute/lib/mongo-date-delete.py $DAY_TARGET
# Pig launch for sites
echo "Launch site info upload"

pig \
-param site_date=./tmp/avro/sites_$DAY_BEFORE_UNDER.out \
-param mongo_host="192.168.0.99" \
-param mongo_port=27017 \
-f /usr/libexec/ar-compute/pig/sites.pig

# Pig launch for statuses
echo "Launching Pig Script for metric status upload"

pig  \
-param prev_file=./tmp/avro/ar-consumer_log_$DAY_BEFORE.avro \
-param target_file=./tmp/avro/ar-consumer_log_$DAY_TARGET.avro \
-param prev_date=$DAY_BEFORE_INT \
-param mongo_host="192.168.0.99" \
-param mongo_port=27017 \
-f /usr/libexec/ar-compute/pig/status_detailed.pig

