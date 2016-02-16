DROP TABLE IF EXISTS fix_insertonly;

CREATE TABLE fix_insertonly
(
    keycol STRING
  , clordid STRING
  , msgtype STRING
  , handlinst INT
  , `symbol` STRING
  , side INT
  , transacttime BIGINT
  , ordtype INT
  , orderqty INT
  , checksum STRING
)
TBLPROPERTIES
(
    'storage_handler' = 'com.cloudera.kudu.hive.KuduStorageHandler'
  , 'kudu.table_name' = 'fix_insertonly'
  , 'kudu.master_addresses' = 'vm1:7051'
  , 'kudu.key_columns' = 'keycol'
);