DROP TABLE IF EXISTS fix_execrpt;

CREATE TABLE fix_execrpt
(
    execid STRING
    
  , msgtype STRING
  , msgtypedesc STRING
  , orderid STRING
  , clordid STRING
  , exectranstype INT
  , exectype INT
  , ordstatus INT  
  , `symbol` STRING
  , side INT
  , leavesqty INT
  , cumqty INT
  , avgpx DOUBLE
  , transacttime BIGINT
  , checksum STRING
  
  , lastupdated STRING
)
TBLPROPERTIES
(
    'storage_handler' = 'com.cloudera.kudu.hive.KuduStorageHandler'
  , 'kudu.table_name' = 'fix_execrpt'
  , 'kudu.master_addresses' = 'vm1:7051'
  , 'kudu.key_columns' = 'execid'
);
