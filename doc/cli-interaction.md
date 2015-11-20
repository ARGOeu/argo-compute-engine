# ARGO Compute Engine CLI

## Executable Scripts for cli interaction with the compute engine

In the folder `/usr/libexec/ar-compute/bin/` reside executable scripts that can be used for uploading metric data and sync data to the hadoop cluster (HDFS Filesystem).

| Script | Description | Shortcut |
|--------|-------------|----------|
|upload_metric.py |The specific script is used in order to upload daily metric data (relative to a tenant) to HDFS. | [Description](#metric) |
|upload_sync.py |The specific script is used in order to upload daily sync data (relative to a tenant and a job) to HDFS. |[Description](#sync)|
|mongo_clean_ar.py |The specific script is used if necessary to clean a/r data from the datastore regarding for specified tenant,report and date.  |[Description](#ar)|
|mongo_clean_status.py |The specific script is used if necessary to clean status detail data from the datastore for specified tenant,report and date. | [Description](#status)|
-->

<a id="metric"></a>

### upload_metric.py

This utility is used in order to upload the daily metric data for a specified date and tenant to the ARGO Compute Engine.

#### Full path

```
/usr/libexec/ar-compute/bin/upload_metric.py
```

#### Parameters

- `-d --date {YYYY-MM-DD}` specifies the date of the metric data we want to upload (Required)
- `-t --tenant {STRING}` a case-sensitive string specifying the name of the tenant (Required)

<a id="sync"></a>

### upload_sync.py

This utility is used in order to upload the daily sync data for a specified date and tenant to the ARGO Compute Engine.

#### Full path

```
/usr/libexec/ar-compute/bin/upload_sync.py
```

#### Parameters

- `-d --date {YYYY-MM-DD}` the date of the daily sync data we want to upload (Required)
- `-t --tenant {STRING}` the name of the tenant. Case sensitive. (Required)
- `-j --job {STRING}` the name of the job. Case sensitive (Required)


<a id="ar"></a>
### mongo_clean_ar.py

This utility is used in order to delete availability and reliability data from the datastore, for a specified tenant,report and date. It is called automatically before each A/R computation, but can be ran also manually. The script reports back the number of records and from which collections these records are removed.

#### Full path

```
/usr/libexec/ar-compute/bin/mongo_clean_ar.py
```

#### Parameters

- `-d --date {YYYY-MM-DD}` the date (day) for which to delete the availability and reliability data (Required)
- `-t --tenant {STRING}` the name of the tenant. Case sensitive. (Required)
- `-r --report {STRING}` the id (uuid format) of the report that results belong to. Case sensitive (Required)

<a id="status"></a>

### mongo_clean_status.py

This utility can be used in order to delete the status detail data from the datastore, for a specified tenant,report and date.

#### Full path
```
/usr/libexec/ar-compute/bin/mongo_clean_status.py
```
#### Parameters

- `-d --date {YYYY-MM-DD}` the date for which to delete the status detail data from the datastore (Required)
- `-t --tenant {STRING}` the name of the tenant. Case sensitive. (Required)
- `-r --report {STRING}` the id (uuid format) of the  report that results belong to. Case sensitive (Required)
