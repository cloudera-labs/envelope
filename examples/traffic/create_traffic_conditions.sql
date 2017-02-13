DROP TABLE IF EXISTS traffic_conditions;

-- This is the syntax for Impala 2.7.0(CDH 5.10.0), if you use a previous CDH version, please refer to documents for CREATE TABLE syntax.
CREATE TABLE traffic_conditions
(
    as_of_time BIGINT
  , avg_num_veh DOUBLE
  , min_num_veh INT
  , max_num_veh INT
  , first_meas_time BIGINT
  , last_meas_time BIGINT
  , PRIMARY KEY(as_of_time)
)
PARTITION BY HASH PARTITIONS 4
STORED AS KUDU
TBLPROPERTIES
(
    'kudu.master_addresses' = 'REPLACEME:7051'
);
