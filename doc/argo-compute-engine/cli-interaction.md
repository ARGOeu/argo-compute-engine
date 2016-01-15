---
title: Compute Engine documentation | ARGO
page_title: Compute - Cli interaction
font_title: 'fa fa-cog'
description: This document describes the Executable Scripts for cli interaction with the compute engine
---

## Executable Scripts for cli interaction with the compute engine

In the folder `/usr/libexec/ar-compute/bin/` reside executable scripts that can be used for uploading metric data and sync data to the hadoop cluster (HDFS Filesystem). . 

| Script | Description | Shortcut 
|--------|-------------|----------
|upload_metric.py |The specific script is used in order to upload daily metric data (relative to a tenant) to HDFS. | [Description](#metric) |
|upload_sync.py |The specific script is used in order to upload daily sync data (relative to a tenant and a job) to HDFS. |[Description](#sync)|
|mongo_clean_ar.py |The specific script is used if necessary to clean a/r data from the datastore regarding for specified tenant,report and date.  |[Description](#ar)|
|mongo_clean_status.py |The specific script is used if necessary to clean status detail data from the datastore for specified tenant,report and date. | [Description](#status)|


<a id="metric"></a>

### upload_metric.py

This utility is used in order to upload the daily metric data for a specified date and tenant to the ARGO Compute Engine.

#### Full path

	/usr/libexec/ar-compute/bin/upload_metric.py


#### Parameters

|Name|Description|Required|
|`-d --date {YYYY-MM-DD}` |specifies the date of the metric data we want to upload  (e.g. MY-SITE-A)|`YES`|
|`-t --tenant {STRING}`| a case-sensitive string specifying the name of the tenant |`YES`|

<a id="sync"></a>

### upload_sync.py

This utility is used in order to upload the daily sync data for a specified date and tenant to the ARGO Compute Engine.

#### Full path

	/usr/libexec/ar-compute/bin/upload_sync.py


#### Parameters

|Name|Description|Required|
|`-d --date {YYYY-MM-DD}` | the date of the daily sync data we want to upload |`YES`|
|`-t --tenant {STRING}`|  the name of the tenant. Case sensitive. |`YES`|
|`-j --job {STRING}`|  the name of the job. Case sensitive |`YES`|


<a id="ar"></a>

### mongo_clean_ar.py

This utility is used in order to delete availability and reliability data from the datastore, for a specified tenant,report and date. It is called automatically before each A/R computation, but can be ran also manually. The script reports back the number of records and from which collections these records are removed.

#### Full path

	/usr/libexec/ar-compute/bin/mongo_clean_ar.py


#### Parameters

|Name|Description|Required|
|`-d --date {YYYY-MM-DD}` | the date (day) for which to delete the availability and reliability data |`YES`|
|`-t --tenant {STRING}`|  the name of the tenant. Case sensitive. |`YES`|
|`-j --report {STRING}`|  the id (uuid format) of the report that results belong to. Case sensitive |`YES`|

<a id="status"></a>

### mongo_clean_status.py

This utility can be used in order to delete the status detail data from the datastore, for a specified tenant,report and date.

#### Full path

	/usr/libexec/ar-compute/bin/mongo_clean_status.py

#### Parameters

|Name|Description|Required|
|`-d --date {YYYY-MM-DD}` | the date (day) for which to delete the availability and reliability data |`YES`|
|`-t --tenant {STRING}`|  the name of the tenant. Case sensitive. |`YES`|
|`-j --report {STRING}`|  the id (uuid format) of the report that results belong to. Case sensitive |`YES`|

