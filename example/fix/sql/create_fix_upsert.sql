DROP TABLE IF EXISTS fix_upsert;

CREATE TABLE fix_upsert
(
    clordid STRING

  , msgtype STRING
  , handlinst INT
  , `symbol` STRING
  , side INT
  , transacttime BIGINT
  , ordtype INT
  , orderqty INT
  , checksum STRING

  , lastupdated STRING
)
TBLPROPERTIES
(
    'storage_handler' = 'com.cloudera.kudu.hive.KuduStorageHandler'
  , 'kudu.table_name' = 'fix_upsert'
  , 'kudu.master_addresses' = 'vm1:7051'
  , 'kudu.key_columns' = 'clordid'
);