DROP TABLE IF EXISTS fix_orders;

CREATE TABLE fix_orders
(
    clordid STRING
  , start_ts BIGINT
  , end_ts BIGINT

  , handlinst INT
  , `symbol` STRING
  , side INT
  , transacttime BIGINT
  , ordtype INT
  , orderqty INT

  , current_flag STRING
  , last_updated STRING
)
TBLPROPERTIES
(
    'storage_handler' = 'com.cloudera.kudu.hive.KuduStorageHandler'
  , 'kudu.table_name' = 'fix_orders'
  , 'kudu.master_addresses' = 'vm1:7051'
  , 'kudu.key_columns' = 'clordid,start_ts'
);