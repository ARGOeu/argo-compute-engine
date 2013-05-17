logs = load '/groups/arstats/data/consumer_log_2013-05-02.txt' using PigStorage('\\u001') as (time_stamp, metric:chararray, service_type:chararray, hostname:chararray, status:chararray, vo:chararray);

per_metric = GROUP logs BY (hostname, service_type, metric);

each_metric_timeline = FOREACH per_metric {
    timeline = FOREACH logs GENERATE status, time_stamp;
    GENERATE FLATTEN(group), timeline;
};

same_metrics = GROUP each_metric_timeline BY (hostname, service_type);

fused_metrics_per_host = FOREACH same_metrics {
  metric_timeline = FOREACH each_metric_timeline GENERATE metric, timeline;
  GENERATE FLATTEN(group), metric_timeline AS metrics;
};

dump fused_metrics_per_host;
