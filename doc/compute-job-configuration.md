# ARGO Compute Engine configuration

## Overview  

|Job Configuration |  Description | Shortcut |
|------------------|--------------|----------|
| `/etc/ar-compute-engine.conf` | This file includes various global parameters used by the engine which are organized in sections, as described next:|[Description](#compute-engine-conf)|
| `{TENANT_NAME}_ops.json` | The ops files are json filetypes that are used to describe the available status types encountered in the monitoring environment of a tenant and also the available algorithmic operations available to use in status aggregations. |[Description](#tenant-ops-conf)|
|`{TENANT_NAME}_{JOB_ID}_cfg.json` | A job config file is a json file that contains specific information needed during the job run such as grouping parameters, the name of the availability profile used and many more. |[Description](#tenant-jobid-conf)|
|`{TENANT_NAME}_{JOB_NAME}_ap.json`| The availability profile is a json file used per specific job that describes the operations that must take place during aggregating statuses up to endpoint group level.|[Description](#tenant-jobid-ap-conf)|

## Tenant and Report configuration

The ARGO Compute Engine is multi-tenant, meaning that a single installation of the ARGO Compute Engine can support multiple tenants (customers). Each tenant must be configured properly before the ARGO Compute Engine can start computing availability and reliability reports for that tenant.

For each tenant there should be at least one _Job Configuration_.  A _Job Configuration_ defines the topology, metric and availability profiles that will be used by the ARGO Compute Engine in order to perform the computations using as input the monitoring metric results received.

For example regarding a hypothetical tenant, we might have two _Job Configurations_ based on two different metric profiles: _Critical_ and _All_. The ARGO Compute Engine, using the _Critical_  job configuration, will compute the availability and reliability report by taking into account the most critical metrics for each service. On the other hand, the _All_ job configuration is more versatile as it takes into account all metrics for each service.

Each _Job Configuration_ includes the following data:

- Topology
- Metric Profile
- Availability Profile
- Weights _(optional)_
- Downtimes _(optional)_

## Hadoop client configuration

In order for the engine to be able to connect and submit jobs successfully in a hadoop cluster, proper hadoop client configuration files must be present the installed node (`/etc/hadoop/conf/`)

## ARGO Compute Engine configuration files

The main configuration file of the ARGO Compute Engine component is installed by default at `/etc/ar-compute-engine.conf`. In addition, a directory with supplementary secondary configuration files is created in `/etc/ar-compute/`

<a id="compute-engine-conf"></a>

### /etc/ar-compute-engine.conf

The main configuration files includes various global parameters used by the engine which are organized in sections, as described next:

#### `[default]`

| Name | Type | Description | Required|
|------|------|-------------|---------|
|`mongo_host`| String | Specify the ip address of the datastore node (running mongodb) | `YES` |
|`mongo_port`| String |Specify the port number of the datastore node (running mongodb) | `YES` |
| `mode` | String| The mode the engine runs. There are two available options: _cluster_ and _local_: `_cluster_`: If the mode is specified as _cluster_, the engine runs connecting to an existing hadoop cluster. It expects that the hadoop client is properly installed and configured. `_local_` : If the mode is specified as _local_, the engine runs local node. | `YES`|
| `serialization`| String|  The serialization type used. There are two available options _avro_ and _none_: `_avro_` : If specified as _avro_, the engine expects metric and sync data in avro format.  `_none_` :If specified as _none_, the engine expects to find metric and sync data in simple text file delimited format. | `YES`|
| `prefilter_clean`| Boolean | Controls whether the local prefilter file will be automatically removed after it has been uploaded to the Compute Engine. If set to _true_, the local prefilter file will be automatically removed after it is  uploaded. |`YES`|
| `sync_clean`| Boolean | Controls whether the uploaded sync files will be automatically removed after a job completion. If set to _true_ the uploaded sync files will be automatically removed after the job completion | `YES`|


#### `[logging]`

In this section we declare the specific logging options for the compute engine

| Name | Type | Description | Required|
|------|------|-------------|---------|
|`log_mode`| String | This parameter specifies the log_mode used by compute engine. Possible values: `syslog` (default), `file`, `none`.
|`syslog`| String | The compute engine is configured to use the syslog facility, a) `file`: the compute engine can write directly to a file defined by `log_file`,  b) `none`: the compute engine does not output any logs| `YES`|
|`log_file`| String | This parameter must be specified if `log_mode=file`. The file which the compute engine will use in order to write logging information |`NO`|
|`log_level`| String | Possible values: `DEBUG` (default), `INFO`, `WARNING`, `ERROR`, `CRITICAL`. Defines the log level that is used by the  compute engine.|`YES`|
|`hadoop_log_root` | String | Hadoop clients log level and log appender. If the user wants the hadoop components to log via SYSLOG must make sure to define an appropriate appender  in hadoop log4j.properties file. The name of this appender must be added in this parameter.|`YES`|

  For example at `hadoop_log_root` if the available appenders in the log4j.properties file are SYSLOG and console the above line will be:
   ```
   hadoop_log_root=SYSLOG,console
   ```
   
#### `[reports]`

In this section we declare the specific tenant used in the installation and the set of jobs available (as we described them above in the [_"Tenant and Report configuration"_](#tenant-and-report-configuration)).

| Name | Type | Description | Required|
|------|------|-------------|---------|
|`tenant`| String | The name of the tenant. It must be unique within an installation.|`YES`|
| `job_set`| List | A comma-separated list of the report configuration. Names are case-sensitive. For the same tenant multiple report configurations can be defined. Each report configurations is defined by a set of  topologies,metric profiles,weights,etc.| `YES`|

#### `[sampling]`

| Name | Type | Description | Required|
|------|------|-------------|---------|
| `s_period` |  minutes |The sampling period time in minutes | `YES`|
| `s_interval` | minutes |The sampling interval time in minutes | `YES`|


> **Note**
>
> the number of samples used in a/r calculations is determined by the s_period/s_interval value. Default values used
> - `s_period = 1440`
> - `s_interval = 5`
>
> so number of samples = 1440/5 = 288

#### `[datastore-mapping]`

This section contains various optional parameters used for correctly mapping results to expected datastore collections and fields

| Name | Description | Required|
|------|-------------|---------|
|`service_dest={db_name}.{collection_name}`| Destination for storing service a/r reports E.g: AR.sfreports | `NO`|
|`egroup_dest={db_name}.{collection_name}` | Destination for storing endpoint grouped a/r reports. E.g: AR.sites | `NO`|
|`sdetail_dest={db_name}.{collection_name}`| Destination for storing status detailed results | `NO` |

> **Note**
>
> Due to nature of field/value storage in the default datastore (mongodb) a specific schema is used with abbreviated field names for storage optimization. For example:
>
>```
>date->dt
>availability profile->ap
>```

The following options define a set of mappings to shorter datastore field names and is recommended not to be changed. Will be removed in further editions.


| Name | Description | Required|
|------|-------------|---------|
|`e_map={fieldname1}, {fieldname2}..., {fieldnameN}`| When storing endpoint a/r results in mongodb compute engine uses the above field map to store the results using abbreviated fields. the default value is `e_map=dt,ap,p,s,n,hs,a,r,up,u,d,m,pr,ss,cs,i,sc`, where dt->date,a->availability etc... (recommended not to be changed) | `NO`|
| `s_map={fieldname1}, {fieldname2}..., {fieldnameN}`| When storing service a/r results in mongodb compute engine uses the above field map to store the results using abbreviated fields. the default value is `s_map=dt,ap,p,s,n,sf,a,r,up,u,d,m,pr,ss,cs,i,sc`, where dt->date,a->availability etc... (recommended not to be changed) | `NO`| 
|`sd_map={fieldname1}, {fieldname2}`| When storing status detailed results in mongodb compute engine uses the above field map to store the results using abbreviated fields. the default value is `sd_map=ts,s,sum,msg,ps,pts,di,ti`, where ts->timestamp, msg->message etc... (recommended not to be changed) | `YES`|
| `n_eg={STRING}`| Endpoint group name type used in status detailed calculations. For e.g. `n_eg=site` if site is used as a group type|`NO`|
| `n_gg={STRING}`| Group of groups name type used in status detailed calculations| `NO`|
| `n_alt={STRING}` | Mapping of alternative grouping parameter used in status detail calc. | `NO`|
| `n_altf={STRING}` | Mapping of alternative grouping parameter used in status detail calc. | `NO`|


### `/etc/ar-compute/`

As mentioned above secondary configuration files used by the compute-engine are stored to the /etc/ar-compute directory. Here are files describing the set of status state types used in the monitoring engine, algorithmic operations of how to combine those states, availability profiles & configuration files for available jobs.

<a id="tenant-ops-conf"></a>

#### `{TENANT_NAME}_ops.json (per tenant)`

These are configuration files expressed in JSON, which describe the available status types encountered in the monitoring environment of a tenant and also the available algorithmic operations to use in status aggregations.

For example if the tenant name is `T1` the corresponding ops filename will be `T1_ops.json`

During computations many operations take place among service statuses which need to be described explicitly. The ARGO Compute Engine gives the flexibility to the end user to declare the available monitoring statuses that are produced by the monitoring infrastructure and to map them to its internal statuses. Then, using _truth tables_ the user can describe the logical operations on these statuses and their results. An ops file contains:

- the list of available status types
- which status type is considered as default in missing circumstances
- which status type is considered as default in downtime circumstances
- which status type is considered as default in unknown circumstances
- a list of available operations between statuses expressed in the form of truth tables

The available status states produced by the Monitoring Engine(s) are expressed in the **"states"** list. For example below is the definition of the status states produced by Nagios compatible Monitoring Engines:

```json
"states": [
    "OK",
    "WARNING",
    "UNKNOWN",
    "MISSING",
    "CRITICAL",
    "DOWNTIME"
]
```

The ARGO Compute Engine requires the user to define a mapping for the default_down, default_missing and default_unknown. For example:

```json
"default_down": "DOWNTIME",
"default_missing": "MISSING",
"default_unknown": "UNKNOWN",
```

> **Note: The importance of the default states**
>
> Since compute engine gives the ability to define completely custom states based on your monitoring infrastructure output we must also tag some custom states with specific meaning. These states might not be present in the monitoring messages but are produced during computations by the compute engine according to a specific logic. So we need to "tie" some of the custom status we declare to a specific default state of service.

| Name | Description |
|------|-------------|
| `"default_down": "DOWNTIME"` | means that whenever compute engine needs to produce a status for a scheduled downtime will mark it using the "DOWNTIME" state. |
| `"default_missing": "MISSING"` | means whenever compute engine decides that a service status must declared missing (because there is no information provided from the metric data) will mark it using the "MISSING" state. |
| `"default_unknown: "UNKNOWN"` | means whenever compute engine decides that must produce a service status to be considered unknown (for e.g. during recomputation requests) will mark it using the "UNKNOWN" state. |

The available operations are declared in the operations list using truth tables as follows:

```json
"operations": {
  "AND":[],
  "OR":[]
}
```

Each operation consists of a JSON array used to describe a _truth table_. An example of such a _truth table_ is presented below:

```json
"operations": {
  "AND": [
    { "A":"OK",       "B":"OK",       "X":"OK"       },
    { "A":"OK",       "B":"WARNING",  "X":"WARNING"  },
    { "A":"OK",       "B":"UNKNOWN",  "X":"UNKNOWN"  },
  ]
}
```

Each element of the JSON array describes a row of the _truth table_ for example:

```json
{ "A":"OK", "B":"WARNING", "X":"WARNING"}
```

declares that in an algorithmic AND operation between two status states of *OK* and *WARNING* the result is *WARNING*

In the ops file the user is able to declare any number of available monitoring states and any number of available custom operations on those states. The ARGO Compute Engine uses this information to create the corresponding _truth tables_ in memory.

<a id="tenant-jobid-conf"></a>

#### `{TENANT_NAME}_{JOB_ID}_cfg.json (per job)`

A _Job Configuration_ file is a JSON file that contains specific information needed during the computations such as grouping parameters, the name of the availability profile used and many more. If the tenant's name is `T1` and the report name is `JobA` then the filename of the config file must be `T1_JobA_cfg.json`

The configuration file of the job contains mandatory and optional fields with rich information describing the parameters of the specific job. Some important fields are:

```json
"tenant": "tenant_name"`
"job": "job_name",
"aprofile": "availability_profile_name",
"egroup": "endpoint_group_type_name",
"ggroup": "group_of_group_type_name",
"weight": "weight_factor_type_name"
```

In the above snippet we have declared the name of the tenant, the name of the job, the name of the specific availability profile used in the job. Also the type of endpoint grouping that will be used is declared here and the type of upper hierarchical grouping. Also if available here is declared the type of weight factor used for upper level A/R aggregations

| Name | Description | 
|------|-------------|
| `"tenant"` | This field is explicitly linked to the value of the tenant declaration of the global ar-compute-engine.conf file [link to description above](/#parameters-for-section-jobs) |
| `"job"` | This field is explicitly linked to the name of a job declared in the job_set variable of the global ar-compute-engine.conf file [link to description above](#parameters-for-section-jobs) |
| `"aprofile"` | This field is explicitly linked to one of the availability profile json files declared in the `/etc/ar_compute/` folder and they are described below [link to description further below](#availability-profile-per-tenant--per-job) |
| `"egroup"` | This field is used to declare the endpoint group that will be used during computation aggregations. The value corresponds to one of the values present in the field `type` of the topology file [group_endpoints.avro](/guides/compute/compute-input/#groupendpointsavro) |
| `"ggroup"` | This field is used to declare the group of groups that will be used during computation aggregations. The value corresponds to one of the values present in the field `type` of the topology file [group_groups.avro](/guides/compute/compute-input/#groupgroupsavro) |
| `"weight"` | This field is used to declare the type of weight that will be used during computation aggregations. The value corresponds to one of the values present in the field `type` of the weight (factors) file [weight_sync.avro](/guides/compute/compute-input/#weights-factors) |

In the configuration file are specified the specific tag values that will be used during the job in order to filter metric data.

For example:

```json
"egroup_tags": {
  "scope":"scope_type",
  "production":"Y",
  "monitored":"Y"
}
```

In the egroup_tag list are declared values for available tag fields that will be encountered in the endpoint group topology sync file (produced by ar-sync components).These tag fields are explicitly linked to the description of the schema of the [group_endpoints.avro file](/guides/compute/compute-input/#groupendpointsavro)

<a id="tenant-jobid-ap-conf"></a>

### `{TENANT_NAME}_{JOB_ID}_ap.json (per report)`

The availability profile is a json file used per specific job that describes the operations that must take place during aggregating statuses up to endpoint group level. For example if the tenant name is `T1` and the report name `JobA` the corresponding availability profile name must be `T1_ReportA_ap.json`

The information in the availability profile JSON file is automatically picked up by the compute-engine during computations.

| Name | Type | Description | 
|------|------|-------------|
| `"name"` | string | the name of the availability profile |
| `"namespace"` | string | the name of the namespace used by the profile |
| `"metric_profile"` | string | the name of the metric profile linked to this availability profile |
| `"metric_ops"` | string | the default operation to be used when aggregating low level metric statuses |
| `"group_type"` | string | the default endpoint group type used in aggregation |

In the availability profile JSON file also are declared custom grouping of services to be used in the aggregation. The grouping of services are expressed in the JSON "groups" list see example below:

```json
"groups": {
  "my_group_of_services_1": {
    "services":{
      "service_type_A":"OR",
      "service_type_B":"OR"
    },
    "operation":"OR"
  },
  "my_group_of_services_2": {
    "services":{
      "service_type_C":"OR",
      "service_type_D":"OR"
    },
    "operation":"OR"
  },
  "operation":"AND"
}
```

In the above example the service types are grouped in two groups:

- **my_group_of_services_1** and
- **my_group_of_services_2**.

Each group contains a "service" list containing service types included in the group as fields and the operation values in order to choose who to aggregate the various instances of a specific service. For example if for ***"service_type_A"*** are 3 service endpoints available, they are going to be aggregated using the OR operation. The ***"operation"*** field under each group of services is used to declare the operation that will be used to aggregate the service types under that group. The outer ***"operation"*** field in the root of the json document is used to declare the operation used to aggregate the various groups in order to produce the final endpoint aggregation result.
