# ARGO Compute Engine CLI

## Executable Scripts for cli interaction with the compute engine
In the folder `/usr/libexec/ar-compute/bin/` reside executable scripts that can be used for uploading metric data and sync data to the hadoop cluster (HDFS Filesystem).

|Script | Description | Shortcut|
|upload_metric.py |The specific script is used in order to upload daily metric data (relative to a tenant) to HDFS. | <a href="#metric">Description</a>|
|upload_sync.py |The specific script is used in order to upload daily sync data (relative to a tenant and a job) to HDFS. | <a href="#sync">Description</a>|
|mongo_clean_ar.py |The specific script is used if necessary to clean a/r data from the datastore regarding a specific day full path.  | <a href="#ar">Description</a>|
|mongo_clean_status.py |The specific script is used if necessary to clean status detail data from the datastore regarding a specific day. | <a href="#status">Description</a>|

<a id="metric"></a>

### upload_metric.py
The specific script is used in order to upload daily metric data (relative to a tenant) to HDFS.

#### Full path

	/usr/libexec/ar-compute/bin/upload_metric.py

#### Parameters

| Type | Description | Required|
|`-d --date {YYYY-MM-DD}`| specifies the date of the metric data we want to upload | `YES` |
|`-t --tenant {STRING}` | a case-sensitive string specifing the name of the tenant | `YES` |


The upload_metric script will push the latest clean metric data to the hadoop cluster for a specific date

<a id="sync"></a>

### upload_sync.py
The specific script is used in order to upload daily sync data (relative to a tenant and a job) to HDFS.


#### Full path

	/usr/libexec/ar-compute/bin/upload_sync.py

#### Parameters

| Type | Description | Required|
|`-d --date {YYYY-MM-DD}`| specifies the date of the sync data we want to upload | `YES` |
|`-t --tenant {STRING}` | a case-sensitive string specifing the name of the tenant | `YES` |
|`-j --job {STRING}` | a case-sensitive string specifing the name of the job | `YES` |

 upload_sync script will push the sync data for a specific date,tenant and job to the hadoop cluster before computations.

<a id="ar"></a>

### mongo_clean_ar.py
The specific script is used if necessary to clean a/r data from the datastore regarding a specific day
full path:

#### Full path

	/usr/libexec/ar-compute/bin/mongo_clean_ar.py

#### Parameters

| Type | Description | Required|
|`-d --date {YYYY-MM-DD}`| specifies the date (day) to clear the data | `YES` |
|`-p --profile {STRING}` | specify the name of an availability profile. If specified, only a/r data regarding the specified profile will be cleared. | NO |


The mongo_clean_ar script will clean a/r results from the mongo datastore for a specific date and/or metric profile. It's been called automatically before a/r computations but can be ran also manually. The script will report on the number of records and from which collections will be removed.

<a id="status"></a>

### mongo_clean_status.py
The specific script is used if necessary to clean status detail data from the datastore regarding a specific day

#### Full path

	/usr/libexec/ar-compute/bin/mongo_clean_status.py

#### Parameters

| Type | Description | Required|
|`-d --date {YYYY-MM-DD}`| date to clean status detail data | `YES` |
