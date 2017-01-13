# ARGO Compute Engine input

## Input Data

| Input Data |  Description | Shortcut |
|------------|--------------|----------|
|Metric Data | Metric data come in the form of avro files and contain timestamped status information about the hostname,service and specific checks (metrics) that are being monitored|[Description](#metric)|
|Topology Files | Topology information is provided by two files: a) groups of endpoints, and b)groups of groups.|[Description](#topology)|
|Metric Profiles | Every service type contains a number of metrics that are being checked from the monitoring mechanism.|[Description](#profiles)|
|Weights (factors) | Some group items have an associated weight information (factors) on how they contribute when are being aggregated on higher level groups. |[Description](#weights)|
|Downtimes | Downtime information: the period (start_time –> end_time) in which a specific service endpoint was in scheduled downtime. |[Description](#downtimes)|

<a id="metric"></a>

## Metric data

The main engine's input is the metric data collected from the argo-consumer component. These files (according to the default configuration of ar-consumer component) reside to the `/var/lib/ar-consumer/` folder.

Metric data come in the form of avro files and contain timestamped status information about the hostname,service and specific checks (metrics) that are being monitored. A typical item of information in the metric data avro file contains the following fields:

| Name | Description | Required|
|------|-------------|---------|
|`hostname`| The fqdn address of the host being monitored | `YES` |
|`service` | The name of the specific service being monitored | `YES` |
|`metric` | The name of the specific metric (check) of the service that is being monitored | `YES` |
|`timestamp` | Time of the monitoring check | `YES` |
|`status` | Status of the metric during the monitoring check | `YES` |
|`monitoring_host` | The fqdn of the monitoring agent  | `NO` |
|`summary` | Text containing a summary of the monitoring check | `NO` |
|`message`| Text containing the detailed system output message of the monitoring check probe| `NO`|
|`tags`| Array containing optional user defined tags| `NO`|

The current **raw avro schema file** for the metric data is the following:

```json
{
  "namespace": "argo.avro",
  "type": "record",
  "name": "metric_data",
  "fields": [
    { "name": "timestamp", "type": "string"},
    { "name": "service", "type": "string"},
    { "name": "hostname", "type": "string"},
    { "name": "metric", "type": "string"},
    { "name": "status", "type": "string"},
    { "name": "monitoring_host", "type": ["null", "string"]},
    { "name": "summary", "type": ["null", "string"]},
    { "name": "message", "type": ["null", "string"]},
    { "name": "tags", "type" : [
      "null", { "name" : "Tags",
                "type" : "map",
                "values" : ["int", "string"]
              }
      ]
    }
  ]
}
```

## Additional input: topology, profiles, factors

This core metric data set is processed and transformed with additional information provided by the argo-connector components. Additional information includes topology, grouping of services, weight factors, lists relevant metrics to be considered, etc. This information is provided per-tenant/per-job in the following path

```
/var/lib/ar-sync/{tenant-name}/{job-name}
```

for example for `tenant-name=T1` and `job-name=JobA` the correct path with the sync files is as follows

```
/var/lib/ar-sync/T1/JobA
```

Some sync files that concern the whole enviroment such as the downtime information are provided once in the root ar-sync folder `/var/lib/ar-sync/`

<a id="topology"></a>

### Topology files

Topology information is provided by two files:

- groups of endpoints,
- groups of groups.

A service endpoint is considered by the engine the simplest item of topology. Service endpoint combines the information of `hostname + service_name`. Service endpoints can be grouped together forming upper level entities named endpoint groups. For example an organization's geographical IT site that is being monitored can be considered a group of service endpoints. Information for available endpoint groups is contained in the file group_endpoints.

#### group_endpoints.avro

The file uses avro format and contains the following fields:

| Name | Description | Required |
|------|-------------|----------|
|`group`|The name of the group (e.g. MY-SITE-A)|`YES`|
|`type`| The type of the grouping (e.g. sites)|`YES`|
|`hostname`| The hostname fqdn part info of the specific endpoint contained in the group|`YES`|
|`service`| The service name part info of the specific endpoint contained in the group|`YES`|
|`tags`| User defined tags providing description metadata| `NO`|

Below is the full description of the group_endpoints.avro specification

```json
{
  "namespace": "argo.avro",
  "type": "record",
  "name": "group_of_service_endpoints",
    "fields": [
      { "name": "type", "type": "string" },
      { "name": "group", "type": "string" },
      { "name": "service", "type": "string" },
      { "name": "hostname", "type": "string" },
      { "name": "tags", "type" : [
        "null", { "name" : "Tags",
                  "type" : "map",
                  "values" : ["int", "string"]
                }
        ]
      }
    ]
}
```

#### group_groups.avro

Service endpoint groups can be further grouped in higher-level entities such as for example nation-wide groups of sites etc. The topology information regarding higher-level groups is contained to the group_groups.avro file.

The file uses avro format and contains the following fields:

| Name | Description | Required |
|------|-------------|----------|
|`profile` | Profile name |`YES`|
| `group`| The name of the group (e.g. MY-SITE-A)|`YES`|
| `type`| The type of the grouping (e.g. sites)|`YES`|
| `hostname`| The hostname fqdn part info of the specific endpoint contained in the group|`YES`|
| `service`| The service name part info of the specific endpoint contained in the group|`YES`|
| `tags`| User defined tags providing description metadata|`NO`|

Below is the full description of the avro specification:

| Name | Description | Required |
|------|-------------|----------|
|`group`|The name of the group (e.g. MY-NATIONAL-GROUP)|`YES`|
|`type`| The type of the grouping (e.g. national entities)|`YES`|
|`subgroup`| The name of the lower level group contained (e.g. MY-SITE-A)|`YES`|
|`tags`| User defined tags providing description metadata|`NO`|

The structure of the specific file gives the ability to define recursively group entities that can be contained as subgroups on other group entities.

An abstract example using cities, nations, continents

```
group: 'Athens', type: 'cities', subgroup:'location-1'
group: 'Athens', type: 'cities', subgroup:'location-2'
group: 'Athens', type: 'cities', subgroup:'location-3'

group: 'Thessaloniki', type: 'cities', subgroup:'location-5'
group: 'Thessaloniki', type: 'cities', subgroup:'location-6'
group: 'Thessaloniki', type: 'cities', subgroup:'location-7'

group: 'Greece', type: 'countries', subgroup: 'Athens'
group: 'Greece', type: 'countries', subgroup: 'Thessaloniki'

group: 'Europe', type: 'continents' subgroup: 'Greece'
group: 'Europe', type: 'continents' subgroup: 'Croatia'
group: 'Europe', type: 'continents' subgroup: 'France'
```

Below is the full avro specification of the group_groups.avro file:

```json
{
  "namespace": "argo.avro",
  "type": "record",
  "name": "group_groups",
  "fields": [
    { "name": "type", "type": "string" },
    { "name": "group", "type": "string" },
    { "name": "subgroup", "type": "string" },
    { "name": "tags", "type" : [
      "null", { "name" : "Tags",
                "type" : "map",
                "values" : ["int", "string"]
              }
      ]
    }
  ]
}
```

<a id="profiles"></a>

### Metric Profiles

Every service type contains a number of metrics that are being checked from the monitoring mechanism. Each metric equals to a specific monitoring check that takes place periodically on the host and has to do with a specific facet of the service's operation (processes, memory, load, files, settings, network etc)

When wanting to look on the whole status information of the service for a given time it is possible to take into account any number of the metrics available (for e.g. the most critical ones) and compose a view of the service based on those specific metrics selected. This view is dictated by a profile, actually a metric profile which contains information about the service and which metrics are relevant. The metric profile is provided as an avro type file containing the following fields:

| Name | Description | Required |
|------|-------------|----------|
|`profile`|Name of the profile|`YES`|
|`service`| Name of the specific service|`YES`|
|`metric`| Name of the metric to be taken into account|`YES`|
|`tags`| User defined tags |`NO`|

and the full avro specification:

```json
{
  "namespace": "argo.avro",
  "type": "record",
  "name": "metric_profiles",
  "fields": [
    { "name": "profile", "type": "string" },
    { "name": "service", "type": "string" },
    { "name": "metric", "type": "string" },
    { "name": "tags", "type" : [
      "null", { "name" : "Tags",
                "type" : "map",
                "values" : ["int", "string"]
              }
      ]
    }
  ]
}
```

<a id="weights"></a>

### Weights (factors)

Some group items have an associated weight information (factors) on how they contribute when are being aggregated on higher level groups. For example hepspec weights for specific sites when they are aggregated on their contribution on national level groups. The weight information is provided in an avro file format containing the following fields:

| Name | Description | Required |
|------|-------------|----------|
|`type`| Type of the weight (for e.g. hepspec)|`YES`|
|`site`| Name of the specific site|`YES`|
|`weight`| Number value of the weight|`YES`|

The full avro specification of the weight file:

```json
{
  "namespace": "argo.avro",
  "type": "record",
  "name": "weight_sites",
  "fields": [
    { "name": "type", "type": "string" },
    { "name": "site", "type": "string" },
    { "name": "weight", "type": "string" }
  ]
}
```

<a id="downtimes"></a>

### Downtimes

Downtime information: the period (start_time --> end_time) in which a specific service endpoint was in scheduled downtime. This information resided in the corresponding downtime avro file. The file has the following fields:

|Name|Description|Required|
|----|-----------|--------|
|`hostname`| The hostname fqdn info part of the specific service endpoint|`YES`|
|`service` | The service name info part of the specific service endpoint|`YES`|
|`start_time` | WC3 date/time when the period begins |`YES`|
|`end_time`| WC3 date/time when the period ends|`YES`|

The full avro specification of the file is provided below

```json
{
  "namespace": "argo.avro",
  "type": "record",
  "name": "downtimes",
  "fields": [
    { "name": "hostname", "type": "string" },
    { "name": "service", "type": "string" },
    { "name": "start_time", "type": "string" },
    { "name": "end_time", "type": "string" }
  ]
}
```
