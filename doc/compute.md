---
title: Compute Engine documentation | ARGO
layout: compute
page_title: Compute Engine documentation
font_title: 'fa fa-cog'
description: This document describes the Compute engine which is the argo component responsible for performing various transformations and computations on the collected metric data to provide availability and reliability metrics.
row_font: [ fa-cog,fa-cog, fa-cog,fa-cog, fa-cog]
row_title: [  Compute Engine Input, Compute Engine job configuration, Compute Engine CLI]
row_description: [' This document describes the compute engine input data (metrics, topology, profiles, factors)', ' This document describes the job configurations', 'This document describes the Executable Scripts for cli interaction with the compute engine']
row_link: ['/guides/compute/compute-input/', '/guides/compute/compute-job-configuration/', '/guides/compute/cli-interaction/']
---

Argo-compute-engine is the argo component responsible for performing various transformations and computations on the collected metric data in order to provide availability and reliability metrics. The results produced by the compute-engine are forwarded and stored to the configured datastore (MongoDB).

Argo-compute-engine uses the hadoop software stack for performing calculations on the metric data as map-reduce jobs. These jobs can also be run locally (single node mode) in the absence of a proper hadoop cluster. Under the hood the engine uses Apache Pig for map-reduce job submission and execution.
