#!/usr/bin/env bash
set -x
# Check the customer argument
if [ -z "$1" ]
  then
    echo "No Customer Argument supplied"
    exit
fi

# Check the date argument
if [ -z "$2" ]
  then
    echo "No Date Argument supplied"
    exit
fi

# Gather two arguments given: tenant and date
TENANT=$1
DAY_TARGET=$2
DAY_UNDER=`echo $DAY_TARGET | sed 's/-/_/g'`
# Data Locations
CONSUMER_DIR=/var/lib/ar-consumer
# Gather Metric Data Filename
FN_CONSUMER=ar-consumer_log_$DAY_TARGET.avro
# Create Hadoop Data Directory if not
hadoop fs -mkdir -p /user/$(whoami)/$TENANT/mdata/
# Remove previous metric data file 
hadoop fs -rm -f /user/$(whoami)/$TENANT/mdata/$FN_CONSUMER
# Upload new one
hadoop fs -put $CONSUMER_DIR/$FN_CONSUMER /user/$(whoami)/$TENANT/mdata/


