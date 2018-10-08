CREATE DATABASE IF NOT EXISTS nav;

-- Kafka offset table;

DROP TABLE IF EXISTS nav.nav_offsets;
CREATE TABLE nav.nav_offsets (
  `group_id` STRING,
  `topic` STRING,
  `partition` INT,
  `offset` BIGINT,
  PRIMARY KEY (`group_id`, `topic`, `partition`)
)
STORED AS KUDU
;

-- Kudu tables;

-- Solr;
DROP TABLE IF EXISTS nav.solr_events;
CREATE TABLE nav.solr_events (
  event_time TIMESTAMP,
  service_name STRING,
  username STRING,
  ip_addr STRING,
  operation STRING,
  allowed STRING,
  impersonator STRING,
  --
  collection_name STRING,
  operation_params STRING,
  solr_version STRING,
  PRIMARY KEY (event_time, service_name, username, ip_addr, operation)
)
PARTITION BY HASH (event_time) PARTITIONS 16,
RANGE (event_time) (
  PARTITION VALUES < '2018-01-01'
)
STORED AS KUDU
;

-- Hue;
DROP TABLE IF EXISTS nav.hue_events;
CREATE TABLE nav.hue_events (
  event_time TIMESTAMP,
  service_name STRING,
  username STRING,
  ip_addr STRING,
  operation STRING,
  allowed STRING,
  impersonator STRING,
  --
  operation_text STRING,
  service STRING,
  url STRING,
  PRIMARY KEY (event_time, service_name, username, ip_addr, operation)
)
PARTITION BY HASH (event_time) PARTITIONS 16,
RANGE (event_time) (
  PARTITION VALUES < '2018-01-01'
)
STORED AS KUDU
;

-- NavMS;
DROP TABLE IF EXISTS nav.navms_events;
CREATE TABLE nav.navms_events (
  event_time TIMESTAMP,
  service_name STRING,
  username STRING,
  ip_addr STRING,
  operation STRING,
  allowed STRING,
  impersonator STRING,
  --
  additional_info STRING,
  entity_id STRING,
  stored_object_name STRING,
  sub_operation STRING,
  PRIMARY KEY (event_time, service_name, username, ip_addr, operation)
)
PARTITION BY HASH (event_time) PARTITIONS 16,
RANGE (event_time) (
  PARTITION VALUES < '2018-01-01'
)
STORED AS KUDU
;

-- Sentry;
DROP TABLE IF EXISTS nav.sentry_events;
CREATE TABLE nav.sentry_events (
  event_time TIMESTAMP,
  service_name STRING,
  username STRING,
  ip_addr STRING,
  operation STRING,
  allowed STRING,
  impersonator STRING,
  --
  sentry_database_name STRING,
  sentry_object_type STRING,
  operation_text STRING,
  resource_path STRING,
  table_name STRING,
  PRIMARY KEY (event_time, service_name, username, ip_addr, operation)
)
PARTITION BY HASH (event_time) PARTITIONS 16,
RANGE (event_time) (
  PARTITION VALUES < '2018-01-01'
)
STORED AS KUDU
;

-- HBase;
DROP TABLE IF EXISTS nav.hbase_events;
CREATE TABLE nav.hbase_events (
  event_time TIMESTAMP,
  service_name STRING,
  username STRING,
  ip_addr STRING,
  operation STRING,
  allowed STRING,
  impersonator STRING,
  --
  table_name STRING,
  family STRING,
  qualifier STRING,
  PRIMARY KEY (event_time, service_name, username, ip_addr, operation)
)
PARTITION BY HASH (event_time) PARTITIONS 16,
RANGE (event_time) (
  PARTITION VALUES < '2018-01-01'
)
STORED AS KUDU
;

-- HDFS;
DROP TABLE IF EXISTS nav.hdfs_events;
CREATE TABLE nav.hdfs_events (
  event_time TIMESTAMP,
  service_name STRING,
  username STRING,
  ip_addr STRING,
  operation STRING,
  allowed STRING,
  impersonator STRING,
  --
  src STRING,
  dest STRING,
  permissions STRING,
  delegation_token_id STRING,
  PRIMARY KEY (event_time, service_name, username, ip_addr, operation)
)
PARTITION BY HASH (event_time) PARTITIONS 16,
RANGE (event_time) (
  PARTITION VALUES < '2018-01-01'
)
STORED AS KUDU
;

-- Hive;
DROP TABLE IF EXISTS nav.hive_events;
CREATE TABLE nav.hive_events (
  event_time TIMESTAMP,
  service_name STRING,
  username STRING,
  ip_addr STRING,
  operation STRING,
  allowed STRING,
  impersonator STRING,
  --
  operation_text STRING,
  database_name STRING,
  table_name STRING,
  resource_path STRING,
  object_type STRING,
  object_usage_type STRING,
  PRIMARY KEY (event_time, service_name, username, ip_addr, operation)
)
PARTITION BY HASH (event_time) PARTITIONS 16,
RANGE (event_time) (
  PARTITION VALUES < '2018-01-01'
)
STORED AS KUDU
;

-- Impala;
DROP TABLE IF EXISTS nav.impala_events;
CREATE TABLE nav.impala_events (
  event_time TIMESTAMP,
  service_name STRING,
  username STRING,
  ip_addr STRING,
  operation STRING,
  allowed STRING,
  impersonator STRING,
  --
  operation_text STRING,
  status STRING,
  database_name STRING,
  table_name STRING,
  privilege STRING,
  object_type STRING,
  query_id STRING,
  session_id STRING,
  PRIMARY KEY (event_time, service_name, username, ip_addr, operation)
)
PARTITION BY HASH (event_time) PARTITIONS 16,
RANGE (event_time) (
  PARTITION VALUES < '2018-01-01'
)
STORED AS KUDU
;

-- Parquet tables;

-- Solr;
DROP TABLE IF EXISTS nav.solr_events_parquet;
CREATE EXTERNAL TABLE nav.solr_events_parquet (
  service_name STRING,
  allowed STRING,
  username STRING,
  impersonator STRING,
  ip_addr STRING,
  operation STRING,
  event_time TIMESTAMP,
  --
  collection_name STRING,
  operation_params STRING,
  solr_version STRING
)
PARTITIONED BY (
  day STRING
)
STORED AS PARQUET
LOCATION '${VAR:hdfs_base_dir}/solr_events_parquet';

-- Hue;
DROP TABLE IF EXISTS nav.hue_events_parquet;
CREATE EXTERNAL TABLE nav.hue_events_parquet (
  service_name STRING,
  allowed STRING,
  username STRING,
  impersonator STRING,
  ip_addr STRING,
  operation STRING,
  event_time TIMESTAMP,
  --
  operation_text STRING,
  service STRING,
  url STRING
)
PARTITIONED BY (
  day STRING
)
STORED AS PARQUET
LOCATION '${VAR:hdfs_base_dir}/hue_events_parquet';

-- NavMS;
DROP TABLE IF EXISTS nav.navms_events_parquet;
CREATE EXTERNAL TABLE nav.navms_events_parquet (
  service_name STRING,
  allowed STRING,
  username STRING,
  impersonator STRING,
  ip_addr STRING,
  operation STRING,
  event_time TIMESTAMP,
  --
  additional_info STRING,
  entity_id STRING,
  stored_object_name STRING,
  sub_operation STRING
)
PARTITIONED BY (
  day STRING
)
STORED AS PARQUET
LOCATION '${VAR:hdfs_base_dir}/navms_events_parquet';

-- Sentry;
DROP TABLE IF EXISTS nav.sentry_events_parquet;
CREATE EXTERNAL TABLE nav.sentry_events_parquet (
  service_name STRING,
  allowed STRING,
  username STRING,
  impersonator STRING,
  ip_addr STRING,
  operation STRING,
  event_time TIMESTAMP,
  --
  sentry_database_name STRING,
  sentry_object_type STRING,
  operation_text STRING,
  resource_path STRING,
  table_name STRING
)
PARTITIONED BY (
  day STRING
)
STORED AS PARQUET
LOCATION '${VAR:hdfs_base_dir}/sentry_events_parquet';

-- HBase;
DROP TABLE IF EXISTS nav.hbase_events_parquet;
CREATE EXTERNAL TABLE nav.hbase_events_parquet (
  service_name STRING,
  allowed STRING,
  username STRING,
  impersonator STRING,
  ip_addr STRING,
  operation STRING,
  event_time TIMESTAMP,
  --
  table_name STRING,
  family STRING,
  qualifier STRING
)
PARTITIONED BY (
  day STRING
)
STORED AS PARQUET
LOCATION '${VAR:hdfs_base_dir}/hbase_events_parquet';

-- HDFS;
DROP TABLE IF EXISTS nav.hdfs_events_parquet;
CREATE EXTERNAL TABLE nav.hdfs_events_parquet (
  service_name STRING,
  allowed STRING,
  username STRING,
  impersonator STRING,
  ip_addr STRING,
  operation STRING,
  event_time TIMESTAMP,
  --
  src STRING,
  dest STRING,
  permissions STRING,
  delegation_token_id STRING
)
PARTITIONED BY (
  day STRING
)
STORED AS PARQUET
LOCATION '${VAR:hdfs_base_dir}/hdfs_events_parquet';

-- Hive;
DROP TABLE IF EXISTS nav.hive_events_parquet;
CREATE EXTERNAL TABLE nav.hive_events_parquet (
  service_name STRING,
  allowed STRING,
  username STRING,
  impersonator STRING,
  ip_addr STRING,
  operation STRING,
  event_time TIMESTAMP,
  --
  operation_text STRING,
  database_name STRING,
  table_name STRING,
  resource_path STRING,
  object_type STRING,
  object_usage_type STRING
)
PARTITIONED BY (
  day STRING
)
STORED AS PARQUET
LOCATION '${VAR:hdfs_base_dir}/hive_events_parquet';

-- Impala;
DROP TABLE IF EXISTS nav.impala_events_parquet;
CREATE EXTERNAL TABLE nav.impala_events_parquet (
  service_name STRING,
  allowed STRING,
  username STRING,
  impersonator STRING,
  ip_addr STRING,
  operation STRING,
  event_time TIMESTAMP,
  --
  operation_text STRING,
  status STRING,
  database_name STRING,
  table_name STRING,
  privilege STRING,
  object_type STRING,
  query_id STRING,
  session_id STRING
)
PARTITIONED BY (
  day STRING
)
STORED AS PARQUET
LOCATION '${VAR:hdfs_base_dir}/impala_events_parquet';
