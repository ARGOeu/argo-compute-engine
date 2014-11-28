#!/bin/sh

pig -x $1 \
-param prev_status=$2 \
-param today_status=$3 \
-param last_date=$4 \
-param mongo_host=$5 \
-param mongo_port=$6 \
-f /usr/libexec/ar-compute/pig/status_detailed.pig
