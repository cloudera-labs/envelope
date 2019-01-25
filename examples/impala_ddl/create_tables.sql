DROP TABLE IF EXISTS example_output;
CREATE EXTERNAL TABLE example_output
(
    id BIGINT
  , foo STRING
  , blah STRING
  , ymd BIGINT
)
STORED AS PARQUET
LOCATION '/tmp/example-output-unpartitioned';

DROP TABLE IF EXISTS example_output_part;
CREATE EXTERNAL TABLE example_output_part
(
    id BIGINT
  , foo STRING
  , blah STRING
)
PARTITIONED BY (ymd BIGINT)
STORED AS PARQUET
LOCATION '/tmp/example-output-partitioned';

DROP TABLE IF EXISTS example_output_kudu;
CREATE TABLE example_output_kudu
(
    ymd BIGINT
  , id BIGINT
  , foo STRING
  , blah STRING
  , PRIMARY KEY (ymd, id)
)
PARTITION BY RANGE (ymd) (
  PARTITION VALUE = 20190101
)
STORED AS KUDU;

