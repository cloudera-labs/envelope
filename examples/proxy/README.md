## Bluecoat Proxy

The Bluecoat Proxy example is an Envelope pipeline that shows how to use the Morphline translator to parse 
incoming logs and convert them to Avro records.  It doesn't store the generated records; the output records are 
printed to stdout via a debugging/testing store. This example demonstrates use cases that need to do more complex 
extraction and manipulation of inbound messages without needing to resort to Spark or SparkSQL.  The use of the 
`grok` command allows for easy parsing development while the `toAvro` command provides a high degree of schema 
mapping and control.

To use the Morphline translator, the Spark executor needs to have the Kite Morphline Core JAR in its classpath, which
 is done via the `--jars` parameter to `spark-submit`. The `toAvro` command requires the Kite Morphline Avro JAR for 
the Spark executor as well. 

The `grok` command requires that the syntax dictionaries be located either in the classpath (likely an included 
JAR), on the local file system, or inline in the morphline configuration file.  In this example, the dictionaries -- 
there are two of them, as one depends on the other -- are loaded into the executor's file system via the `--files` 
parameter.  

### Execution

To run this example, first edit the `bluecoat.properties` file to update the Kafka brokers for your 
cluster. Then execute the following command: 

    spark-submit \
        --driver-java-options "-Dlog4j.configuration=file:///log4j.properties" \
        --files log4j.properties,bluecoat.conf,bluecoat.avro,bluecoat.grok,grok-patterns.grok \
        --jars kite-morphlines-avro.jar,kite-morphlines-core.jar \
        envelope-0.1.0.jar \
        bluecoat.properties

_NOTE: You will have to adjust the paths to these resources according to your environment._

For sample data, there is a extremely rudimentary Apache Kafka producer to generate sample messages, which pushes out
 at a 10ms interval the same fake log entry to the specified topic (`bluecoat` in this example). Run the generator as:

    spark-submit \
        --class com.cloudera.labs.envelope.examples.ProxyGenerator \
        envelope-0.1.0.jar \
        <your_kafka_broker_host>:9092 \
        bluecoat

To see the results, kill both jobs (generator then envelope), and then view the YARN application logs, which is where
 the StdoutStorageTable will write.

Enjoy!       
 