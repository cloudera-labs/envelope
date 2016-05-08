DROP TABLE IF EXISTS traffic_conditions;

CREATE TABLE traffic_conditions
(
    as_of_time BIGINT
  , avg_num_veh DOUBLE
  , min_num_veh INT
  , max_num_veh INT
  , first_meas_time BIGINT
  , last_meas_time BIGINT
)
TBLPROPERTIES
(
    'storage_handler' = 'com.cloudera.kudu.hive.KuduStorageHandler'
  , 'kudu.table_name' = 'traffic_conditions'
  , 'kudu.master_addresses' = 'vm1:7051'
  , 'kudu.key_columns' = 'as_of_time'
);
