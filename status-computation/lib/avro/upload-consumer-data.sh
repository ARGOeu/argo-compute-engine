#!/usr/bin/env bash
set -x
# Gather two arguments given: tenant and date
TENANT=$1
DAY_TARGET=$2
DAY_UNDER=`echo $DAY_TARGET | sed 's/-/_/g'`
# Data Locations
CONSUMER_DIR=/var/lib/ar-consumer
# Gather Metric Data Filename
FN_CONSUMER=ar-consumer_log_$DAY_TARGET.avro
# Create Hadoop Data Directory if not
hadoop fs -mkdir -p /user/root/$TENANT/mdata/
# Remove previous metric data file 
hadoop fs -rm -f /user/root/$TENANT/mdata/$FN_CONSUMER
# Upload new one
hadoop fs -put $CONSUMER_DIR/$FN_CONSUMER /user/root/$TENANT/mdata/


