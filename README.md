## nrtkudu
nrtkudu* is a near-real-time ingestion library for Kudu built on Spark Streaming. The goal of the library is to implement common ingestion logic and then only require the user of the library to provide the mappings of the arriving data fields to the Kudu table fields plus some additional configuration.

Key functionality that is currently implemented (but not rigorously tested):
- Ingestion from Kafka
- Common API for defining mappings of Kafka messages to Kudu tables
- Updates of existing records of an arriving key (Type I SCD)
- History tracking for all record versions of an arriving key (Type II SCD)
- Consistent batching of Kudu writes within a Spark Streaming micro-batch
- Derivation of new fields from the arriving record
- Ingestion of a Kafka topic to multiple Kudu tables
- Modularized to allow alternative storage layers, such as HBase
- Example implementation of the library to ingest FIX messages

The user of the library only needs to provide:
- A class that maps the Kafka messages to Avro records
- A class per Kudu table that maps the Avro records to Kudu field values
- No Kafka, Spark Streaming, or Kudu code required

*pending a catchier name...

### Architecture

nrtkudu is built as a Java Spark Streaming job that receives data from the Kafka direct connector and reads/writes to Kudu using the Kudu Java client API.

The library acts on all data by the natural key of the messages. When a micro-batch begins the arrived Kafka messages are grouped by their key across the Spark executors to ensure that there are no race conditions in the case that multiple messages arrive for the same key. Within each resulting Spark RDD partition the keys are iterated over to define which Kudu writes are required to bring that natural key up to date in Kudu at the completion of the micro-batch. This includes doing Kudu scans to identify the current state of the natural key within the Kudu table. After all keys of the partition have been examined then the required writes are applied in small batches to improve throughput.

Avro generic records are used as a common in-memory record format to compare Kafka messages to Kudu scan results. Data does not need to arrive or be persisted as Avro.

The two classes required from the user of the library are the decoder and the encoder:
- The decoder (base class `Decoder`) is provided Kafka messages and returns equivalent Avro records.
- The encoder (base classes `Encoder` and `KuduEncoder`) does the work of reading/writing to Kudu, but the user is only required to provide Kudu field values for the decoded Avro record field values plus some additional metadata. This could be driven from configuration if there are no derived fields.

An execution of the job correlates to one Kafka topic, to one decoder, and to one-to-many encoders.

### Update logic

The Kudu client API write semantics only allows for inserts where the primary key does not already exist, and only allows for updates where the primary key does already exist. nrtkudu reads any existing record(s) of the natural key to determine the required writes, if any, that need to go back to Kudu to include the arrived records of the natural key.

#### Type I

Where history tracking is not enabled on an encoder then nrtkudu will:
- Update an existing record for the key if the arriving timestamp is equal or greater than the existing record and there has been a change in the data.
- Insert a new record if there are no existing records for the key.

#### Type II

Where history tracking is enabled on an encoder then nrtkudu will follow Type II SCD conventions and store a Kudu record for each version of the key. A version is identified by its timestamp. Each record will contain the metadata fields:
- Effective from: when the version first became effective
- Effective to: when the version finally was effective. If this has not yet happened it will be persisted as 9999-12-31.
- Current flag: whether the version is currently effective

The primary key of a Type II table is assumed to be the natural key + effective from.

nrtkudu can handle record versions arriving out of order. All record versions of a key are scanned from Kudu and the arriving records for the key are then applied to that history, including setting metadata fields of existing versions that are found to be no longer effective.

### Example FIX implementation

An example implementation for ingesting FIX messages is included in the `com.cloudera.fce.nrtkudu.fix` package. It only contains a small number of FIX fields and would require more work to be usable for a real-world FIX use case.

The FIX ingest application can be run with following command:

    spark-submit nrtkudu-0.1.0.jar [Kudu master hosts] [Kafka host]:9092 [Kafka topic] FIX [micro-batch duration in seconds]
    
For example:

    spark-submit nrtkudu-0.1.0.jar vm1:7051 vm1:9092 generated7 FIX 3

The FIXGenerator class can be separately run to send example FIX messages to Kafka for testing purposes. It will first create a new order with a UUID key (FIX field ClOrdID) and then send updates for the key with the order quantity reducing by random amounts until it reaches zero. A FIX message is sent every 2 milliseconds. This repeats until the program is killed. It can be run with the following command: (note this does not actually start a Spark application)

    spark-submit --class com.cloudera.fce.nrtkudu.fix.FIXGenerator nrtkudu-0.1.0.jar [Kafka host]:9092 [Kafka topic]

Querying the Kudu table is straight-forward using Impala. There are example DDL scripts provided for creating Impala tables that automatically create corresponding Kudu tables underneath. When querying the Type II fix_orders table the results will be similar to:

    [vm1.jeremybeard.net:21000] > select * from fix_orders where clordid = '02006379-475f-417d-9700-167019361699' order by start_ts;
    Query: select * from fix_orders where clordid = '02006379-475f-417d-9700-167019361699' order by start_ts
    +--------------------------------------+---------------+-----------------+-----------+--------+------+---------------+---------+----------+--------------+------------------------------+
    | clordid                              | start_ts      | end_ts          | handlinst | symbol | side | transacttime  | ordtype | orderqty | current_flag | last_updated                 |
    +--------------------------------------+---------------+-----------------+-----------+--------+------+---------------+---------+----------+--------------+------------------------------+
    | 02006379-475f-417d-9700-167019361699 | 1445977516291 | 1445977516293   | 2         | AAPL   | 2    | 1445977516291 | 2       | 2722     | N            | Tue Oct 27 16:25:18 EDT 2015 |
    | 02006379-475f-417d-9700-167019361699 | 1445977516294 | 1445977516295   | 2         | AAPL   | 2    | 1445977516294 | 2       | 2378     | N            | Tue Oct 27 16:25:18 EDT 2015 |
    | 02006379-475f-417d-9700-167019361699 | 1445977516296 | 1445977516297   | 2         | AAPL   | 2    | 1445977516296 | 2       | 696      | N            | Tue Oct 27 16:25:18 EDT 2015 |
    | 02006379-475f-417d-9700-167019361699 | 1445977516298 | 1445977516299   | 2         | AAPL   | 2    | 1445977516298 | 2       | 199      | N            | Tue Oct 27 16:25:18 EDT 2015 |
    | 02006379-475f-417d-9700-167019361699 | 1445977516300 | 253402214400000 | 2         | AAPL   | 2    | 1445977516300 | 2       | 0        | Y            | Tue Oct 27 16:25:18 EDT 2015 |
    +--------------------------------------+---------------+-----------------+-----------+--------+------+---------------+---------+----------+--------------+------------------------------+
    Fetched 5 row(s) in 0.15s

### To be implemented

The current implementation is very much alpha and requires a lot of work to make it more reliable and performant. Potential areas to be tackled include:

- Comprehensive test cases
- Capture and intelligently act on exceptions
- Mechanism for executing a re-run of original messages
- Store Kudu write errors into a table for manual intervention
- Drive more of the decoder/encoder parts from configuration to reduce coding
- SQL expression of mappings and derivations
- Use a real logging library instead of println
- Derivations that include input from outside the arriving record
- Do change data capture on a hash of the values
- Only scan columns from Kudu table that need to be used
- Enable Spark Streaming checkpointing
- Allow binary Kafka messages instead of just string
- Allow no-scan inserts with surrogate key if there are no updates (e.g. logs)
- Skip group-by at start of micro-batch if keys are known to go to a single Kafka partition
- Add remaining Kudu data types
- Many low-hanging-fruit performance improvements that likely exist