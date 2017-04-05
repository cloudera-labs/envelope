DROP TABLE IF EXISTS fix_messagetypes;
CREATE TABLE fix_messagetypes
(
    msgtype STRING
  , msgtypedesc STRING
  , PRIMARY KEY (msgtype)
)
PARTITION BY HASH(msgtype) PARTITIONS 2
STORED AS KUDU;

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
  , PRIMARY KEY (clordid)
)
PARTITION BY HASH(clordid) PARTITIONS 4
STORED AS KUDU;

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
  , PRIMARY KEY (execid)
)
PARTITION BY HASH(execid) PARTITIONS 4
STORED AS KUDU;

DROP TABLE IF EXISTS fix_orderhistory;
CREATE TABLE fix_orderhistory
(
    clordid STRING
  , startdate BIGINT
  , `symbol` STRING
  , transacttime BIGINT
  , orderqty INT
  , leavesqty INT
  , cumqty INT
  , avgpx DOUBLE
  , enddate BIGINT
  , currentflag STRING
  , lastupdated STRING
  , PRIMARY KEY (clordid, startdate)
)
PARTITION BY HASH(clordid, startdate) PARTITIONS 4
STORED AS KUDU;
