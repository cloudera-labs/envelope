# Envelope

Envelope is a configuration-driven framework for Apache Spark that makes it easy to develop Spark-based data processing pipelines on a Cloudera EDH.

Envelope is simply a pre-made Spark application that implements many of the tasks commonly found in ETL pipelines. In many cases, Envelope allows large pipelines to be developed on Spark with no coding required. When custom code is needed, there are pluggable points in Envelope for core functionality to be extended. Envelope works in batch and streaming modes.

Some examples of what you can easily do with Envelope:
- Run a graph of Spark SQL queries, all in the memory of a single Spark job
- Stream in event data from Apache Kafka, join to reference data, and write to Apache Kudu
- Read in from an RDBMS table and write to Apache Parquet files on HDFS
- Automatically merge into slowly changing dimensions (Type 1 and 2, and bi-temporal)
- Insert custom DataFrame transformation logic for executing complex business rules

## Get started

### Requirements

Envelope requires a CDH5.7+ cluster with:

- Cloudera's distribution of Apache Spark 2.2.0 Release 2 or above
- Cloudera's distribution of Apache Kafka 2.1.0 (based on Apache Kafka 0.10) or above, if using that component
- Cloudera's distribution of Apache Kudu 1.3.0 or above, if using that component

### Compiling Envelope

You can build the Envelope application from the top-level directory of the source code by running the Maven command:

    mvn clean package

This will create `envelope-0.6.0-SNAPSHOT.jar` in the `build/envelope/target` directory.

### Finding examples

Envelope provides three example pipelines that you can run for yourself:

- [FIX](examples/fix/): simulates receiving financial orders and executions and tracking the history of the orders over time.
    - This example includes a [walkthrough](examples/fix/README.adoc#walkthrough) that explains in detail how it meets the requirements.
- [FIX HBase](examples/fix-hbase/): simulates receiving financial orders and executions and tracking the history of the orders over time in HBase.
- [Traffic](examples/traffic/): simulates receiving traffic conditions and calculating an aggregate view of traffic congestion.
- [Filesystem](examples/filesystem/): demonstrates a batch job that reads a JSON file from HDFS and writes the data back to Avro files on HDFS.

### Running Envelope

You can run Envelope by submitting it to Spark with the configuration file for your pipeline:

    spark2-submit envelope-0.6.0-SNAPSHOT.jar yourpipeline.conf

A helpful place to monitor your running pipeline is from the Spark UI for the job. You can find this via the YARN ResourceManager UI, which can be found in Cloudera Manager by navigating to the YARN service and then to the ResourceManager Web UI link.

## More information

If you are ready for more, dive in:

* [User Guide](docs/userguide.adoc) - details on the design, operations, configuration, and usage of Envelope
* [Configuration Specification](docs/configurations.adoc) - a deep-dive into the configuration options of Envelope
* [Inputs Guide](docs/inputs.adoc) - detailed information on each provided input, and how to write custom inputs
* [Derivers Guide](docs/derivers.adoc) - detailed information on each provided deriver, and how to write custom derivers
* [Planners Guide](docs/planners.adoc) - directions and details on when, why, and how to use planners and associated outputs
* [Looping Guide](docs/looping.adoc) - information and an example for defining loops in an Envelope pipeline
* [Decisions Guide](docs/decisions.adoc) - information on using decisions to dynamically choose which parts of the pipeline to run
* [Tasks Guide](docs/tasks.adoc) - how to apply side-effects in an Envelope pipeline that are separate from the data flow