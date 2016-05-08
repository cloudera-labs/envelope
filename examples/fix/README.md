## FIX

The FIX example is an Envelope pipeline that receives [FIX financial messages](https://en.wikipedia.org/wiki/Financial_Information_eXchange) of order fulfillment and updates the representation of the order in Kudu. This use case would allow near-real-time analytics of order history.

The configuration for this example is found [here](http://github.mtv.cloudera.com/jeremy/envelope/blob/master/examples/fix/fix.properties). The messages do not conform to the real FIX protocol but should be sufficient to demonstrate how a complete implementation could be developed.

After creating the required Kudu tables using the provided Impala scripts, the example can be run as:

    spark-submit envelope-0.1.0.jar fix.properties

A Kafka producer to generate sample messages for the example, and push them in to the "fix" topic, can be run as:

    spark-submit --class com.cloudera.fce.envelope.examples.FIXGenerator envelope-0.1.0.jar kafkabrokerhost:9092 fix