ALTER TABLE nav.hbase_events_parquet RECOVER PARTITIONS;
ALTER TABLE nav.hdfs_events_parquet RECOVER PARTITIONS;
ALTER TABLE nav.hive_events_parquet RECOVER PARTITIONS;
ALTER TABLE nav.hue_events_parquet RECOVER PARTITIONS;
ALTER TABLE nav.impala_events_parquet RECOVER PARTITIONS;
ALTER TABLE nav.navms_events_parquet RECOVER PARTITIONS;
ALTER TABLE nav.sentry_events_parquet RECOVER PARTITIONS;
ALTER TABLE nav.solr_events_parquet RECOVER PARTITIONS;

REFRESH nav.hbase_events_parquet;
REFRESH nav.hdfs_events_parquet;
REFRESH nav.hive_events_parquet;
REFRESH nav.hue_events_parquet;
REFRESH nav.impala_events_parquet;
REFRESH nav.navms_events_parquet;
REFRESH nav.sentry_events_parquet;
REFRESH nav.solr_events_parquet;
