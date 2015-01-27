#!/usr/bin/env bash
set -x
# Gather two arguments given: tenant and date
TENANT=$1
JOB=$2
DAY_TARGET=$3
DAY_UNDER=`echo $DAY_TARGET | sed 's/-/_/g'`
# Data Locations
SYNC_DIR=/var/lib/ar-sync
# Gather Sync Data Filenames
FN_WEIGHTS=weights_sync_$DAY_UNDER
FN_DOWNTIMES=downtimes_$DAY_TARGET
FN_GP_ENDPOINTS=group_endpoints_$DAY_UNDER
FN_GP_GROUPS=group_groups_$DAY_UNDER
FN_PROFILE=poem_sync_$DAY_UNDER
# Profile Dir
PROFILE_DIR=$SYNC_DIR/$TENANT$/$JOB

# Create Hadoop Data Directory if not
hadoop fs -mkdir -p /user/root/$TENANT/sync/$DAY_TARGET
# Remove previous metric data file 
hadoop fs -rm -f /user/root/$TENANT/sync/$DAY_TARGET/*
# Upload new files
hadoop fs -put $CONSUMER_DIR/$FN_WEIGHTS /user/root/$TENANT/sync/$DAY_TARGET/
hadoop fs -put $CONSUMER_DIR/$FN_DOWNTIMES /user/root/$TENANT/sync/$DAY_TARGET/
hadoop fs -put $CONSUMER_DIR/$FN_GP_ENDPOINTS /user/root/$TENANT/sync/$DAY_TARGET/
hadoop fs -put $CONSUMER_DIR/$FN_GP_GROUPS /user/root/$TENANT/sync/$DAY_TARGET/
hadoop fs -put $PROFILE_DIR/$FN_PROFILE /user/root/$TENANT/sync/$DAY_TARGET/

