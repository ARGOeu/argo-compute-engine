#!/usr/bin/env bash
set -x

# Check the customer argument
if [ -z "$1" ]
  then
    echo "No Customer Argument supplied"
    exit
fi

# Check the Job argument
if [ -z "$2" ]
  then
    echo "No Job Argument supplied"
    exit
fi

# Check the customer argument
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
SYNC_DIR=/var/lib/ar-sync
# Gather Sync Data Filenames
FN_WEIGHTS=weights_sync_$DAY_UNDER.avro
FN_DOWNTIMES=downtimes_$DAY_TARGET.avro
FN_GP_ENDPOINTS=group_endpoints_$DAY_UNDER.avro
FN_GP_GROUPS=group_groups_$DAY_UNDER.avro
FN_PROFILE=poem_sync_$DAY_UNDER.avro
# Profile Dir
PROFILE_DIR=$SYNC_DIR/$TENANT/$JOB

# Create Hadoop Data Directory if not
hadoop fs -mkdir -p /user/$(whoami)/$TENANT/sync/$JOB/
# Remove previous sync files 
hadoop fs -rm -f /user/$(whoami)/$TENANT/sync/$JOB/$FN_WEIGHTS
hadoop fs -rm -f /user/$(whoami)/$TENANT/sync/$JOB/$FN_DOWNTIMES
hadoop fs -rm -f /user/$(whoami)/$TENANT/sync/$JOB/$FN_GP_ENDPOINTS
hadoop fs -rm -f /user/$(whoami)/$TENANT/sync/$JOB/$FN_GP_GROUPS
hadoop fs -rm -f /user/$(whoami)/$TENANT/sync/$JOB/$FN_PROFILE
# Upload new files
hadoop fs -put $SYNC_DIR/$FN_WEIGHTS /user/$(whoami)/$TENANT/sync/$JOB/
hadoop fs -put $SYNC_DIR/$FN_DOWNTIMES /user/$(whoami)/$TENANT/sync/$JOB/
hadoop fs -put $SYNC_DIR/$FN_GP_ENDPOINTS /user/$(whoami)/$TENANT/sync/$JOB/
hadoop fs -put $SYNC_DIR/$FN_GP_GROUPS /user/$(whoami)/$TENANT/sync/$JOB/
hadoop fs -put $PROFILE_DIR/$FN_PROFILE /user/$(whoami)/$TENANT/sync/$JOB/

