# ARGO Compute Engine documentation

Argo-compute-engine is the argo component responsible for performing various transformations and computations on the collected metric data in order to provide availability and reliability metrics. The results produced by the compute-engine are forwarded and stored to the configured datastore (MongoDB).

Argo-compute-engine uses the hadoop software stack for performing calculations on the metric data as map-reduce jobs. These jobs can also be run locally (single node mode) in the absence of a proper hadoop cluster. Under the hood the engine uses Apache Pig for map-reduce job submission and execution.
