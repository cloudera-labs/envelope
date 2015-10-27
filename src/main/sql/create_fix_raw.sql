DROP TABLE IF EXISTS fix_raw;

CREATE TABLE fix_raw 
(
    message STRING
  , last_updated STRING
)
TBLPROPERTIES
(
    'storage_handler' = 'com.cloudera.kudu.hive.KuduStorageHandler'
  , 'kudu.table_name' = 'fix_raw'
  , 'kudu.master_addresses' = 'vm1:7051'
  , 'kudu.key_columns' = 'message'
);
