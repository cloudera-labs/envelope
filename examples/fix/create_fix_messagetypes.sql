DROP TABLE IF EXISTS fix_messagetypes;

CREATE TABLE fix_messagetypes
(
    msgtype STRING
  , msgtypedesc STRING
)
TBLPROPERTIES
(
    'storage_handler' = 'com.cloudera.kudu.hive.KuduStorageHandler'
  , 'kudu.table_name' = 'fix_messagetypes'
  , 'kudu.master_addresses' = 'vm1:7051'
  , 'kudu.key_columns' = 'msgtype'
);