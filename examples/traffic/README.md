## Traffic

The traffic example is an Envelope pipeline that retrieves measurements of traffic congestion and stores an aggregated view of the traffic congestion at a point in time using the current measurement and all of those in the previous 60 seconds. Within Envelope this uses the Spark Streaming window operations functionality. This example demonstrates use cases that need to do live aggregations of recently received messages prior to user querying.

The configuration for this example is found [here](http://github.mtv.cloudera.com/jeremy/envelope/tree/master/examples/traffic/traffic.properties). After creating the required Kudu tables using the provided Impala scripts, the example can be run as:

    spark-submit envelope-0.1.0.jar traffic.properties

A Kafka producer to generate sample messages for the example, and push them in to the "traffic" topic, can be run as:

    spark-submit --class com.cloudera.fce.envelope.examples.TrafficGenerator envelope-0.1.0.jar kafkabrokerhost:9092 traffic