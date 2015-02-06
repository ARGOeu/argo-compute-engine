#!/usr/bin/env bash
set -x
# Check the customer argument
if [ -z "$1" ]
  then
    echo "No Customer Argument supplied"
    exit
fi

# Check the job argument
if [ -z "$2" ]
  then
    echo "No Job Argument supplied"
    exit
fi

# Check the date argument
if [ -z "$3" ]
  then
    echo "No Date Argument supplied"
    exit
fi

# Gather two arguments given: tenant and date
TENANT=$1
JOB=$2
DAY_TARGET=$3
DAY_UNDER=`echo $DAY_TARGET | sed 's/-/_/g'`
# Data Locations
PREFILTER_CONF=/etc/ar-sync/prefilter-avro.$TENANT.$JOB.conf
# Call the prefilter
/usr/libexec/ar-sync/prefilter-avro $PREFILTER_CONF -d $DAY_TARGET

FN_PREFILTER=prefilter_$DAY_UNDER.avro
# Create Hadoop Data Directory if not
hadoop fs -mkdir -p /user/$(whoami)/$TENANT/mdata/
# Remove previous metric data file 
hadoop fs -rm -f /user/$(whoami)/$TENANT/mdata/$FN_PREFILTER
# Upload new one
hadoop fs -put /var/lib/ar-sync/$TENTANT/$JOB/$FN_PREFILTER /user/$(whoami)/$TENANT/mdata/
# Clean prefilter in localfs 
rm -f /var/lib/ar-sync/$TENANT/$JOB/$FN_PREFILTER

