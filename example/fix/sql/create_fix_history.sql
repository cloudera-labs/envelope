DROP TABLE IF EXISTS fix_history;

CREATE TABLE fix_history
(
    clordid STRING
  , startdate BIGINT

  , msgtype STRING
  , handlinst INT
  , `symbol` STRING
  , side INT
  , transacttime BIGINT
  , ordtype INT
  , orderqty INT
  , checksum STRING

  , enddate BIGINT
  , currentflag STRING
  , lastupdated STRING
)
TBLPROPERTIES
(
    'storage_handler' = 'com.cloudera.kudu.hive.KuduStorageHandler'
  , 'kudu.table_name' = 'fix_history'
  , 'kudu.master_addresses' = 'vm1:7051'
  , 'kudu.key_columns' = 'clordid,startdate'
);