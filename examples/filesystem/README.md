# Simple Filesystem Example

This example demonstrates a simple HDFS-based data processing pipeline. 

**Build Envelope**

    mvn package

**Upload the JSON example data to home directory**

    hdfs dfs -put examples/filesystem/example-input.json

**Run the Envelope job**

    spark2-submit build/envelope/target/envelope-*.jar examples/filesystem/filesystem.conf

**Grab the results**

    hdfs dfs -get example-output/*.parquet

**Look at the local results**

    parquet-tools cat *.parquet
