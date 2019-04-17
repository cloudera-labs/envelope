# Simple Filesystem Example

This example demonstrates a simple HDFS-based data processing pipeline. 

**Build Envelope**

    mvn package

**Upload the JSON example data to home directory**

    hdfs dfs -put examples/filesystem/example-input.json

**Run the Envelope job**

    spark-submit build/envelope/target/envelope-*.jar examples/filesystem/filesystem.conf

Note: CDH5 uses `spark2-submit` instead of `spark-submit` for Spark 2 applications such as Envelope.

**Grab the results**

    hdfs dfs -get example-output/*.parquet

**Look at the local results**

    parquet-tools cat *.parquet
