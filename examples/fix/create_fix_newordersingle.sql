DROP TABLE IF EXISTS fix_newordersingle;

CREATE TABLE fix_newordersingle
(
    clordid STRING
  , msgtype STRING
  , msgtypedesc STRING
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
  , 'kudu.table_name' = 'fix_newordersingle'
  , 'kudu.master_addresses' = 'vm1:7051'
  , 'kudu.key_columns' = 'clordid'
);
