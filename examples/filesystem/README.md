# Simple Filesystem Example

This example demonstrates a simple HDFS-based data processing pipeline. 

**Build Envelope**

    mvn package

**Upload the JSON example data**

    hadoop fs -copyFromLocal examples/filesystem/example-input.json .

**Run the Envelope job**

    spark-submit target/envelope-*.jar examples/filesystem/filesystem.conf

**Concatenate the resulting partitions**

This is a quick 'n dirty way to glob the partitions.

    IN=$(hadoop fs -ls hdfs://<your NameNode>:8020/user/<username>/example-output/part* | awk '{printf "%s ", $NF}')
    avro-tools concat ${IN} results.avro

**Look at the local results**

    avro-tools tojson --pretty results.avro
