# ARGO Compute Engine configuration

## Main Job Configurations  

|Job Configuration |  Description - Section of | Shortcut |
|Main configuration file | This file includes various global parameters used by the engine which are organized in sections, as described next:|<a href="#etcar-compute-engineconf-main-configuration-file">Description</a>|
|Parameters for section `[default]` |Main configuration file |<a href="#parameters-for-section-default">Description</a>|
|Parameters for section `[logging]` | Main configuration file  |<a href="#parameters-for-section-logging">Description</a>|
|Parameters for section `[jobs]` |  Main configuration file |<a href="#parameters-for-section-jobs">Description</a>|
|Parameters for section `[sampling]` |  Main configuration file |<a href="#parameters-for-section-sampling">Description</a>|
|Parameters for section `[datastore-mapping]` |  Main configuration file |<a href="#parameters-for-section-datastore-mapping">Description</a>|
|Ops Files (per tenant) | The ops files are json filetypes that are used to describe the available status types encountered in the monitoring enviroment of a tenant and also the availabe algorithmic operations available to use in status aggregations. |<a href="#ops-files-per-tenant">Description</a>|
|Config File (per tenant/ per job) | A job config file is a json file that contains specific information needed during the job run such as grouping parameters, the name of the availability profile used and many more. |<a href="#config-file-per-tenant-per-job">Description</a>|
|Availability Profile (per tenant / per job)| The availability profile is a json file used per specific job that describes the operations that must take place during aggregating statuses up to endpoint group level.|<a href="#availability-profile-per-tenant--per-job">Description</a>|

## Tenants and Job definitions
Compute engine can be tenant and job aware. Each tenant must be configured properly both in the argo-connector configuration files and then in the argo-compute-engine configuration.

For each tenant there is at least one or more computational jobs declared. These jobs allow to perform different calculations on the same metric data in order to produce different a/r results based on topology and metric profiles.

For example regarding a specific tenant we might have two jobs defined based on two different metric profiles: __Job_Critical__, __Job_All__

The Job_Critical configuration for example will compute a/r results by taking into account the most critical metrics for each service. The __Job_All__ configuration for example will be more strict by using a profile that takes into account all metrics for each service.

Each Job configuration includes its own supplementary data: _topology_, _metric profile_, _availability profile_, _weights_, _downtimes_ etc.

Each tenant has its own folder which contains job subfolders. Each job subfolder contains daily supplementary data files from argo-connectors. The directory structure resembles the following:

- path_to_argo_connector_data
  + tenantA
    - Job_Critical
    - Job_All

## Hadoop client configuration

In order for the engine to be able to connect and submit jobs successfully in a hadoop cluster, proper hadoop client configuration files must be present the installed node (`/etc/hadoop/conf/`)

## Configuration files of Argo-compute-engine

With the installation of Argo-compute-engine component a main configuration file is deployed in `/etc/ar-compute-engine.conf`. In addition, a directory with supplementary secondary configuration files is created in `/etc/ar-compute/`

### /etc/ar-compute-engine.conf (Main configuration file)

This file includes various global parameters used by the engine which are organized in sections, as described next:

#### Parameters for section `[default]`

| Name | Description | Options| Required |
|`mongo_host={IP ADDRESS}`| specify the ip address of the datastore node (running mongodb) |  | `YES` |
|`mongo_port={PORT_NUMBER}`| specify the port number of the datastore node (running mongodb) |  | `YES` |
|`mode={cluster|local}`| The mode the engine runs |two available options= **cluster, local** .  If specified as **cluster** the engine runs connecting to an available hadoop cluster. If specified as **local**, the engine runs calculating using processes in the local node.| `YES` |
| `serialization={avro|none}` | The serializatin type used |two available options= **avro, none**. If specified as **avro**, the engine expects metric and sync data in avro format. If specified as **none**, the engine expects to find metric and sync data in simple text file delimited format|`YES` |
| `prefilter_clean={true|false}`| If the local prefilter file will be automatically cleaned after hdfs upload or not | If set to **true** local prefilter file will be automatically cleaned after hdfs upload|
|`sync_clean={true|false}`| If the uploaded sync files will be automatically cleaned after a job completion or not | If set to **true** uploaded sync files will be automatically cleaned after a job completion| `YES` |


#### Parameters for section `[logging]`
In this section we declare the specific logging options for the compute engine

| Name | Description | Options| Default value | Required |
|`log_mode=syslog|file|none`| specifies the log_mode used by compute engine. If set to `file` compute engine can write directy to a file path defined in the next parameter. If set to `none` no logging is outputted.|`syslog|file|none` | Default value is `syslog`| `YES`|
| `log_file=path_to_log_file` |  The logfile file path. The compute engine will output log messages to the file path specified as value.|||NO Must be specified if `log_mode=file`.|
| `log_level=DEBUG|INFO|WARNING|ERROR|CRITICAL` | Specify the log level status to be used. |`DEBUG|INFO|WARNING|ERROR|CRITICAL`| `DEBUG`|`YES`|
| `hadoop_log_root={log4j.appender},{log4j.appender}` |Hadoop clients log level and log appender. If the user for example want the hadoop components to log via SYSLOG must make sure to define an appropriate appender  in hadoop log4j.properties file. Then the name of this appender must be added in the above line. For example if the available appenders in the log4j.properties file are SYSLOG and console the above line will be `hadoop_log_root=SYSLOG,console`.| `{log4j.appender},{log4j.appender}` | `YES`|

#### Parameters for section `[jobs]`
In this section we declare the specific tenant used in the installation and the set of jobs available (as we described them above in the [_"Tenants and jobs definitions"_](#tenants-and-job-definitions)).

| Name | Description | Required |
|`tenant={TENANT_NAME}`|specify the name of the tenant used in this installation|`YES`|
|`job_set={JobName1},{JobName2},...{JobNameN}` | A list already establihed jobs (initially specified in ar-connector components). Names are case-sensitive. For the same tenant multiple jobs can be present. Each job is defined by a set of different argo-connector files (different topologies,metric profiles,weights,etc...). Each job gives the opportunity to calculate a different view base|`YES`|

#### Parameters for section `[sampling]`

| Name | Description | Required |
|`s_period={INTEGER}` | specify the sampling period time in minutes | `YES`|
|`s_interval={INTEGER}` |specify the sampling interval time in minutes  | `YES`|

***Note:*** the number of samples used in a/r calculations is determined by the s_period/s_interval value. Default values used: **s_period = 1440** (mins) , **s_interval=5** (mins). so number of sample = 1440/5=288 samples.


#### Parameters for section `[datastore-mapping]`
This section contains various optional parameters used for correctly mapping results to expected datastore collections and fields

| Name | Description | Required |
|`service_dest={db_name}.{collection_name}` | destination for storing service a/r reports E.g: AR.sfreports | `YES`|
|`egroup_dest={db_name}.{collection_name}` | destination for storing endpoint grouped a/r reports. E.g: AR.sites | `YES`|
| `sdetail_dest={db_name}.{collection_name}` | destination for storing status detailed results | `YES`|

Due to nature of field/value storage in the default datastore (mongodb) a specific schema is used with abbreviated field names for storage optimization.
For example date->dt, availability profile->ap.
The following options define some mapping to shorter datastore fieldnames (mongo related) and is recommended not to be changed. Will be removed in further editions.

| Name | Description | Required |
| `e_map={fieldname1},{fieldname2}...,{fieldnameN}` | When storing endpoint a/r results in mongodb compute engine uses the above field map to store the results using abbreviated fields. the default value is
`e_map=dt,ap,p,s,n,hs,a,r,up,u,d,m,pr,ss,cs,i,sc`, where dt->date,a->availability etc... (recommended not to be changed) | `YES`|
|`s_map={fieldname1},{fieldname2}...,{fieldnameN}` | When storing service a/r results in mongodb compute engine uses the above field map to store the results using abbreviated fields. the default value is
`s_map=dt,ap,p,s,n,sf,a,r,up,u,d,m,pr,ss,cs,i,sc`, where dt->date,a->availability etc... (recommended not to be changed) | `YES`|
|`sd_map={fieldname1},{fieldname2}` | When storing status detailed results in mongodb compute engine uses the above field map to store the results using abbreviated fields. the default value is
`sd_map=ts,s,sum,msg,ps,pts,di,ti`, where ts->timestamp, msg->message etc... (recommended not to be changed) | `YES`|
| `n_eg={STRING}` | endpoint group name type used in status detailed calculations. For e.g. `n_eg=site` if site is used as a group type | `YES`|
| `n_gg={STRING}` | group of groups name type used in status detailed calculations | `YES`|
| `n_alt={STRING}` | mapping of alternative grouping parameter used in status detail calc. | `YES`|
| `n_altf={STRING}` | mapping of alternative grouping parameter used in status detail calc.| `YES`|


#### Contents of /etc/ar-compute directory
As mentioned above secondary configuration files used by the compute-engine are stored to the /etc/ar-compute directory. Here are files describing the set of status state types used in the monitoring engine, algorithmic operations of how to combine those states, availability profiles & configuration files for available jobs.

### Ops Files (per tenant)
The ops files are json filetypes that are used to describe the available status types encountered in the monitoring enviroment of a tenant and also the availabe algorithmic operations available to use in status aggregations. The filename template used is very specific and is the following:

	{TENANT_NAME}_ops.json

for eg. if the tenant name is T1 the corresponding ops filename will be

	T1_ops.json

#### Contents of an ops file (operations on monitoring statuses)
During calculations many operations take place among service statuses which need to be described explicitly. Compute engine gives the flexibility to the end user to declare the available monitoring statuses that are produced by the monitoring infrastructure. Then, using the form of simple truth tables the user can describe operations on these statuses and the results of those operations. The available statuses and the available operations are described using json format in ops files:

An ops file contains:
- the list of available status types
- which status type is considered as default in missing circumstances
- which status type is considered as default in downtime circumstances
- which status type is considered as default in unknown circumstances
- a list of available operations between statuses expressed in the form of truth tables

The available status states in the metric enviroment are expressed in the **"states"** list as described below

	"states":
	  [
	    "OK",
	    "WARNING",
	    "UNKNOWN",
	    "MISSING",
	    "CRITICAL",
	    "DOWNTIME"
	  ]


Based on the availabe states declared in the "states" list are then declared default missing,unknown and downtime states for eg:


	 "default_down": "DOWNTIME",
	 "default_missing": "MISSING",
	 "default_unknown": "UNKNOWN",


___Note: The importance of the default states___
_Since compute engine gives the ability to define completely custom states based on your monitoring infrastructure output we must also tag some custom states with specific meaning. These states might not be present in the monitoring messages but are produced during computations by the compute engine according to a specific logic. So we need to "tie" some of the custom status we declare to a specific default state of service._

|Name | Description |
| `"default_down": "DOWNTIME"`| _means that whenever compute engine needs to produce a status for a scheduled downtime will mark it using the "DOWNTIME" state._ |
| `"default_missing": "MISSING"`| _means whenever compute engine decides that a service status must declared missing (because there is no information provided from the metric data) will mark it using the "MISSING" state._ |
| `"default_unknown": "UNKNOWN"`| _means whenever compute engine decides that must produce a service status to be considered unknown (for e.g. during recomputation requests) will mark it using the "UNKNOWN" state._ |

The available operations are declared in the operations list using truth tables as follows:

	"operations":
	  {
	    "AND":[]
	    "OR":[]
	  }


Each operation consists of a json array used to describe a truth table. For example expanding the above AND operation we see that it consists of a list of dictionary elements


	"operations":
	  {
	    "AND":
	    [
	      { "A":"OK",       "B":"OK",       "X":"OK"       },
	      { "A":"OK",       "B":"WARNING",  "X":"WARNING"  },
	      { "A":"OK",       "B":"UNKNOWN",  "X":"UNKNOWN"  },
	      ...
	      ...
	      ...
	     ]
	}

Each element of the json array describes a row of the truth table for eg:

	{ "A":"OK", "B":"WARNING", "X":"WARNING"}

declares that in an algorithmic AND operation between two status states of *OK* and *WARNING* the result is *WARNING*

In the ops file the user is able to declare any number of availabe monitoring states and any number of available custom operations on those states. The compute-engine picks up this information as a sync file for a job and creates in memory the corresponding truth tables.

### Config File (per tenant/ per job)
A job config file is a json file that contains specific information needed during the job run such as grouping parameters, the name of the availability profile used and many more.

The file name template of each job config file is the following:

	{TENANT_NAME}_{JOB_ID}_cfg.json

If the tenant's name is T1 and the jobs name is JobA then the filename of the config file must be:

	T1_JobA_cfg.json

#### Contents of an job config file
The configuration file of the job contains mandatory and optional fields with rich information describing the paramaters of the specific job. Some important fields are:

	"tenant":"tenant_name"`
	"job":"job_name",
	"aprofile":"availability_profile_name",
	"egroup":"endpoint_group_type_name",
	"ggroup":"group_of_group_type_name",
	"weight":"weight_factor_type_name",


In the above snippet are declared the name of the tenant, the name of the job, the name of the specific availability profile used in the job. Also the type of endpoint grouping that will be used is declared here also and the type of upper hierarchical grouping. Also if available here is declared the type of weight factor used for upper level a/r aggregations

|Name | Description |
|`"tenant"`| field is explicitly linked to the value of the tenant declaration of the global ar-compute-engine.conf file [link to description above](/#parameters-for-section-jobs)
|`"job"` | field is explicitly linked to the name of a job declared in the job_set variable of the global ar-compute-engine.conf file [link to description above](#parameters-for-section-jobs)
|`"aprofile"` | field is explicitly linked to one of the availability profile json files declared in the `/etc/ar_compute/` folder and they are described below [link to description further below](#availability-profile-per-tenant--per-job)
|`"egroup"` |field is used to declare the endpoint group that will be used during computation aggregations. The value corresponds to one of the values present in the field `type` of the topology file [group_endpoints.avro](/guides/compute/compute-input/#groupendpointsavro)
|`"ggroup"` | field is used to declare the group of groups that will be used during computation aggregations. The value corresponds to one of the values present in the field `type` of the topology file [group_groups.avro](/guides/compute/compute-input/#groupgroupsavro)
|`"weight"` | field is used to declare the type of weight that will be used during computation aggregations. The value corresponds to one of the values present in the field `type` of the weight (factors) file [weight_sync.avro](/guides/compute/compute-input/#weights-factors)

In the configuration file are specified the specific tag values that will be used during the job in order to filter metric data.

For eg.

	"egroup_tags":{
	    "scope":"scope_type",
	    "production":"Y",
	    "monitored":"Y"
	    }

In the egroup_tag list are declared values for available tag fields that will be encountered in the endpoint group topology sync file (produced by ar-sync components).These tag fields are explicitly linked to the description of the schema of the [group_endpoints.avro file](/guides/compute/compute-input/#groupendpointsavro)

### Availability Profile (per tenant / per job)
The availability profile is a json file used per specific job that describes the operations that must take place during aggregating statuses up to endpoint group level. The filename template is specific:

	{TENANT_NAME}_{JOB_NAME}_ap.json


For eg. if the tenant name = T1 and the job name = JobA the corresponding availability profile name must be:

	T1_JobA_ap.json

The information in the availability profile json file is automatically picked up by the compute-engine during computations.

#### Contents of an availability profile json file

|Name | Type | Description|
|`"name"` | `"string"` | the name of the availability profile|
|`"namespace"` | `"string"`| the name of the namespace used by the profile
|`"metric_profile"` | `"string"`| the name of the metric profile linked to this availability profile
|`"metric_ops"` | ` "string"`| the default operation to be used when aggregating low level metric statuses
|`"group_type"` | `"string"`| the default endpoint group type used in aggregation

In the availability profile json file also are declared custom grouping of services to be used in the aggregation. The grouping of services are expressed in the json "groups" list see example below:


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

In the above example the service types are grouped in two groups: ***my_group_of_services_1*** and ***my_group_of_services_2***. Each group contains a "service" list containing service types included in the group as fields and the operation values in orded to choose who to aggregate the various instances of a specific service. For example if for ***"service_type_A"*** are 3 service endpoints available, they are going to be aggregated using the OR operation. The ***"operation"*** field under each group of services is used to declare the operation that will be used to aggregate the service types under that group.
The outer ***"operation"*** field in the root of the json document is used to declare the operation used to aggregate the various groups in order to produce the final endpoint aggregation result.
