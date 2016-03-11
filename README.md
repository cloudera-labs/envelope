## Envelope

Envelope is a Spark Streaming application that can be configured to easily implement streaming data pipelines on a CDH cluster.

The target use cases for Envelope are pipelines that need to move, and perhaps transform using SQL along the way, data from a queue (such as Kafka) to a storage layer (such as Kudu). The goal of Envelope is to reduce the amount of plumbing code required to develop these pipelines -- sometimes without requiring any code at all.

Running an Envelope pipeline is as simple as submitting the Envelope application to Spark along with the configuration that defines the pipeline being implemented. Configuration is provided as a simple properties file. Where Envelope does not already provide the functionality to develop a specific pipeline there are pluggable points in the data flow that user-provided code can be inserted.

### Examples

Envelope includes example configurations to demonstrate what can be achieved and as a reference for building new Envelope pipelines. All of the examples source from Kafka and store in to Kudu, which in turn allows immediate user access to streaming data via fast Impala queries.

#### FIX

The FIX example is an Envelope pipeline that receives [FIX financial messages](https://en.wikipedia.org/wiki/Financial_Information_eXchange) of order fulfillment and updates the representation of the order in Kudu. This use case would allow near-real-time analytics of order history. More information on this example can be found [here](http://github.mtv.cloudera.com/jeremy/envelope/tree/master/example/fix).

#### Tick data

The tick data example is an Envelope pipeline that retrieves sparse [tick-level](http://www.investopedia.com/terms/t/tick.asp) market data where only the updated values of the security are provided, and stores the ticks into Kudu with all values populated by using the previous state of the security for the values that are not provided. This use case would allow near-real-time analytics of the state of the market for any security, ranging from the most recent state to many years of history.

The configuration for this example is found here. The messages are only representative of real sparsely-populated ticks but should be sufficient to demonstrate how a complete implementation could be developed.

After creating the required Kudu tables using the provided Impala scripts, the example can be run as:

    spark-submit envelope-0.1.0.jar tick.properties

A Kafka producer to generate sample messages for the example, and push them in to the "tick" topic, can be run as:

    spark-submit --class com.cloudera.fce.envelope.example.tick.ExampleTickDataGenerator envelope-0.1.0.jar kafkabrokerhost:9092 tick

#### Traffic

The traffic example is an Envelope pipeline that retrieves measurements of traffic congestion and stores an aggregated view of the traffic congestion at a point in time using the current measurement and all of those in the previous 60 seconds. Within Envelope this uses the Spark Streaming window operations functionality. This example demonstrates use cases that need to do live aggregations of recently received messages prior to user querying.

The configuration for this example is found here. After creating the required Kudu tables using the provided Impala scripts, the example can be run as:

    spark-submit envelope-0.1.0.jar traffic.properties

A Kafka producer to generate sample messages for the example, and push them in to the "traffic" topic, can be run as:

    spark-submit --class com.cloudera.fce.envelope.example.traffic.ExampleTrafficGenerator envelope-0.1.0.jar kafkabrokerhost:9092 traffic

### Functionality

Envelope pipelines run these six stages every Spark Streaming micro-batch:

 1. Queue Sourcing -- retrieve the queued messages
 2. Translation -- translate the queue messages into typed records
 3. Lookup -- optionally retrieve existing storage records
 4. Derivation -- use SQL to transform the stream and lookup records into the storage data model
 5. Planning -- determine the mutations required to update the storage layer
 6. Storing -- apply the planned mutations to the storage layer

Stages 4 to 6 can be defined multiple times per pipeline so that a stream can be fed into multiple storage tables. Each of these is called a flow.

#### Queue Sourcing

A queue source interacts with an instance of a message queueing system. Envelope provides an implementation of this for Kafka via the built-in Spark Streaming integration. User-provided implementations can be referenced if they extend the `QueueSource` class.

#### Translation

A translator interprets the messages from a queue source as typed records. Envelope uses Avro `GenericRecord`s as the in-memory record format between the stages of the pipeline. The translated records are registered as a Spark SQL temporary table named "stream".

Envelope provides translator implementations for delimited messages, key-value pair messages, and serialized Avro records. User-provided implementations can be referenced if they extend the `Translator` class.

#### Lookup

The lookup stage retrieves existing records from a storage layer. This is often useful to provide data for enriching records with reference data from other tables, or for transforming the stream message based on existing records of the destination table. The retrieved records are registered as a Spark SQL temporary table with the same name as the storage table. Currently lookups are limited to existing records that be identified from values on the translated stream record.

#### Derivation

A deriver is used to transform the stream data model into the storage data model. Envelope provides a Spark SQL implementation that allows a SQL SELECT statement to be used to define the transformation. The SQL can be provided either directly into the configuration or as a reference to an HDFS file that contains the SQL statement. User-provided deriver implementations can be referenced if they extend the `Deriver` class.

#### Planning

A planner uses the derived records to determine which mutations are required to apply the records to the storage layer. This may involve referencing the existing records of the same keys. The planned mutations are logical so that a planner can be used across multiple storage systems. Planners are designed to correctly plan for records that arrive out-of-order, based on the specified timestamp field of the record.

Envelope provides three planner implementations:
* Append: all records are planned as inserts. This does not require any reference of existing records and so provides the best performance. A UUID key can be optionally added.
* Upsert: records that have an existing storage record for the same key are planned as updates, and those that do not are planned as inserts. However, no update is planned where the timestamp is before that of an existing record of the same key, or where the timestamp is the same as an existing record of the same key but no values have changed.
* History: plans to store all versions of a key using [Type II modeling](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2). This may require multiple mutations of the storage table for a single arriving record where the start/end dates and current flags of existing versions need to be altered to reflect the new history.

User-provided planner implementations can be referenced if they extend the `Planner` class.

#### Storing

Storage is represented in Envelope as systems that contain mutable tables. A planner is compatible with a storage system if the storage system can apply all of the mutation types (e.g. insert, update, delete) that the planner may produce. Envelope currently provides a storage implementation for Kudu. User-provided storage implementations can be referenced if they extend the `StorageSystem` and `StorageTable` classes.

#### Flows

A flow is a definition of the derivation to planning to storing stages. An Envelope pipeline defines one or more flows. Each flow is automatically executed in parallel by Spark.

