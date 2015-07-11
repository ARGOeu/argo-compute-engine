# ARGO Compute Engine documentation

The ARGO Compute Engine is responsible for computing the availability and reliability of services using:

- the metric results that are collected from monitoring engine(s)
- information about the topology of the infrastructure
- information about scheduled downtimes (optional)
- information about the importance of each entity in the infrastructure (optional)

The availability and reliability results produced by the ARGO Compute Engine can be retrieved through the ARGO Web API.

##  How the ARGO Compute Engine works

The ARGO Compute Engine is designed to be multi-tenant, meaning that each installation of the ARGO Compute Engine can support multiple tenants - customers. Each tenant must have at least one _Report Configuration_,  which includes the topology of the monitored infrastructure, information about the monitored services and their metrics and the metric and availability profiles that will be applied in order to calculate the status, the availability and the reliability.

From the ARGO Compute Engine point of view, a monitored infrastructure is comprised by service instances of specific _Service Types_ running on specific endpoints, which are called _Service Endpoints_. For example a _Service Type_ of  web server listening on port 443 on the host www.example.com, is a _Service Endpoint_. So a _Service Endpoint_ is defined as the combination of an FQDN and _Service Type_. Each _Service Type_ can have a defined set of _metrics_, which are explicit tests that we check in order to asses the status of a _Service Endpoint_.  The ARGO Compute Engine receives as input the results from each metric tested against the monitored _Service Endpoints_.

The ARGO Compute Engine allows the users to model their infrastructure by defining multiple level of _Groups_, starting from groups of _Service Endpoints_ and moving to higher level groups of groups. In this way it is easy to model different infrastructure architectures.

For example a user with an infrastructure on top of AWS using VPC can model the infrastructure in the following way:

```
- Global Group                  # Level 5
  - Regional Group              # Level 4
    - VPC                       # Level 3
      - Availability Group      # Level 2
        - Service Types         # Level 1
          -  Service Endpoints  # Level 0
```

Another example comes from the European Grid Infrastructure (EGI), in which the infrastructure model is the following:

```
- EGI # Level 4                       |  - VO # Level 2
  - NGI # Level 3                     |    - Service Type # Level 1
    - Site # Level 2                  |      - Service Endpoints # Level 0
      - Service Types # Level 1       |
        - Service Endpoints # Level 0 |
```
> _More information about [European Grid Infrastructure](http://www.egi.eu/infrastructure/), the [NGIs](http://www.egi.eu/community/ngis/) and the [VOs](http://www.egi.eu/community/vos/) can be found on the website of [EGI](http://www.egi.eu)_

### Status computations

The ARGO Compute Engine expects to receive a stream metric results produced by a monitoring engine. A metric result is the output of a specific test that was run at a specific time against a specific service endpoint. A metric result includes at least:

- a timestamp showing when the given monitoring probe was executed
- the name of the service type (e.g. HTTPS Web Server)
- the name of the hostname on which the service is running (e.g. www.example.com)
- the name of the metric that was tested (e.g. TCP_CHECK)
- the status result that was produced by the monitoring probe (e.g. OK)

An example metric result in is shown below:

```json
{
  "timestamp": "2013-05-02T10:53:38Z",
  "metric": "org.bdii.Freshness",
  "service_type": "Site-BDII",
  "hostname": "bdii.afroditi.hellasgrid.gr",
  "status": "OK"
}
```

The ARGO Compute Engine receives this stream of metric results and creates a set of status timelines for each service endpoint and metric tuple. The ARGO Compute Engine computes the status of the _Service Endpoints_  based on the results from each defined _metric_ for the _Service Type_ of the _Service Endpoints_, which have been checked within a time frame that matches the frequency with which the probe is executed. The ARGO Compute Engine requires at least three states to be defined:

- A `DOWNTIME` state, which is used in order to fill the timelines when downtime information is applied.
- A `MISSING` state, which is used in order to fill the timelines when a metric isn't present in the consumer data for a period of time
- An `UNKNOWN` state, which is used in order fill the timelines when a re-computation exclusion is applied

A default ARGO Compute Engine installation comes with seven statuses predefined, in alignment with the statuses produced by a Nagios compatible monitoring engine: ``"OK"``, ``"WARNING"``, ``"UNKNOWN"``, ``"MISSING"``, ``"CRITICAL"`` and ``"DOWNTIME"``. The computed service statuses generate status timelines for each service endpoint.

So for example, let's assume that the service we are interested in is the website https://www.example.com and that there are two metrics defined for a secure website, the ``TCP_CHECK`` and ``CHECK_CERTIFICATE_VALIDITY``. In order for the website to be considered as OK, the results for both the tcp check and the check for the certificate validity must be OK. How the individual results of each _metric_ for a _Service Type_ are combined in order to compute the status of the _Service Endpoint_, is defined in what we call *truth tables*. More information about the truth tables can be found in the ARGO Compute Engine configuration document.

### Availability and reliability computations

In order to understand how service availability and reliability is computed, it is important to understand what availability and reliability mean:

- Service Availability is the fraction of time a service was in the UP Period during the known interval in a given period
- Service Reliability is the ratio of the time interval a service was UP over the time interval it was supposed (scheduled) to be UP in the given period.

Before computing the availability and reliability of a given service or group, the ARGO Compute Engine computes the status timelines for that service or group by aggregating the individual status results of the lower level groups or service endpoints. How these aggregations are taking place at each group level, is defined by user defined algorithms called _metric profiles_ for the status computations and _availability profiles_ for the availability and reliability computations. More information about the definition of the _metric profiles_  and the _availability profiles_ can be found in the ARGO Compute Engine configuration document.

The ARGO Compute Engine computes hourly availability and reliability for  _Service Endpoints_ based on the individual statuses of each _Service Endpoint_ for a specific time period. It computes the percentage of the time the _Service Endpoint_ was available within that period (Availability). In order to compute the Reliability of a _Service Endpoint_, it takes into account the declared _Downtimes_ for the given _Service Endpoint_.

The availability and reliability of the _Group_ of _Service Endpoints_ and any other higher level grouping are computed by using the aggregated status timelines. In these computations, the ARGO Compute Engine can take into account custom factors such as the importance of each _Service Endpoint_ and _Group_. These custom factors are used as weights when the aggregated percentages are computed. More information about the custom factors can be found in the ARGO Compute Engine configuration document.
