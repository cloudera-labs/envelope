DROP TABLE IF EXISTS fix_orderhistory;

CREATE TABLE fix_orderhistory
(
    clordid STRING
  , startdate BIGINT

  , msgtype STRING
  , `symbol` STRING
  , transacttime BIGINT
  , orderqty INT
  , leavesqty INT
  , cumqty INT
  , avgpx DOUBLE

  , enddate BIGINT
  , currentflag STRING
  , lastupdated STRING
)
TBLPROPERTIES
(
    'storage_handler' = 'com.cloudera.kudu.hive.KuduStorageHandler'
  , 'kudu.table_name' = 'fix_orderhistory'
  , 'kudu.master_addresses' = 'vm1:7051'
  , 'kudu.key_columns' = 'clordid,startdate'
);
