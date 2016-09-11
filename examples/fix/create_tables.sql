DROP TABLE IF EXISTS fix_messagetypes;
CREATE TABLE fix_messagetypes
(
    msgtype STRING
  , msgtypedesc STRING
)
DISTRIBUTE BY HASH(msgtype) INTO 4 BUCKETS
TBLPROPERTIES
(
    'storage_handler' = 'com.cloudera.kudu.hive.KuduStorageHandler'
  , 'kudu.table_name' = 'fix_messagetypes'
  , 'kudu.master_addresses' = 'vm1:7051'
  , 'kudu.key_columns' = 'msgtype'
);
INSERT INTO fix_messagetypes VALUES ('D', 'Order Single'), ('8', 'Execution Report');

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
DISTRIBUTE BY HASH(clordid) INTO 4 BUCKETS
TBLPROPERTIES
(
    'storage_handler' = 'com.cloudera.kudu.hive.KuduStorageHandler'
  , 'kudu.table_name' = 'fix_newordersingle'
  , 'kudu.master_addresses' = 'vm1:7051'
  , 'kudu.key_columns' = 'clordid'
);

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
DISTRIBUTE BY HASH(execid) INTO 4 BUCKETS
TBLPROPERTIES
(
    'storage_handler' = 'com.cloudera.kudu.hive.KuduStorageHandler'
  , 'kudu.table_name' = 'fix_execrpt'
  , 'kudu.master_addresses' = 'vm1:7051'
  , 'kudu.key_columns' = 'execid'
);

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
DISTRIBUTE BY HASH(clordid, startdate) INTO 4 BUCKETS
TBLPROPERTIES
(
    'storage_handler' = 'com.cloudera.kudu.hive.KuduStorageHandler'
  , 'kudu.table_name' = 'fix_orderhistory'
  , 'kudu.master_addresses' = 'vm1:7051'
  , 'kudu.key_columns' = 'clordid,startdate'
);
