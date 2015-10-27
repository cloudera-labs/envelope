# nrtkudu
nrtkudu* is a near-real-time ingestion library for Kudu built on Spark Streaming. The goal of the library is to implement common ingestion logic and then only require the user of the library to provide the mapping of the arriving data to the Kudu tables.

Key functionality that is currently implemented:
- Ingestion from Kafka
- Common API for defining mappings of Kafka messages to Kudu tables
- Updates of existing records of an arriving key (Type I SCD)
- History tracking for all record versions of an arriving key (Type II SCD)
- Consistent batching of Kudu writes within a Spark Streaming micro-batch
- Derive new fields from the arriving record
- Write a Kafka topic to multiple Kudu tables
- Modularized to allow alternative storage layers, such as HBase
- Example implementation of the library to ingest FIX messages

The user of the library only needs to provide:
- A class that maps the Kafka messages to Avro records
- A class that maps the Avro records to Kudu field values
- No Kafka, Spark Streaming, or Kudu code required

*pending a catchier name...

### Architecture

nrtkudu is built as a Java Spark Streaming job that receives data from the Kafka direct connector and does reads/writes using the Kudu client API.

The library acts on all data by the natural key of the messages. When a micro-batch begins the arrived Kafka messages are grouped across the Spark executors by this key to ensure that there are no race conditions in the case that multiple messages arrive for the same key. Within each resulting Spark RDD partition the keys are iterated over to define which Kudu writes are required to bring that natural key up to date in Kudu at the completion of the micro-batch. This includes doing Kudu scans to identify the current state of the natural key within the Kudu table. After all keys of the partition have been processed then the required writes are applied in small batches to improve throughput.

Avro generic records are used as a common in-memory record format to compare Kafka messages to Kudu scan results. Data does not need to arrive or be persisted as Avro.

The two classes required from the user of the library are the decoder and the encoder:
- The decoder (base class Decoder) is provided Kafka messages and returns equivalent Avro records.
- The encoder (base classes Encoder and KuduEncoder) does the work of reading/writing to Kudu, but the user is only required to provide Kudu field values for the decoded Avro record field values plus some additional metadata. This could be driven from configuration if there are no derived fields.

An execution of the library Spark Streaming job correlates to one Kafka topic, to one decoder, and to one-to-many encoders.

### Update logic

The Kudu client API write semantics only allow for inserts where the primary key does not already exist, and updates where the primary does already exist. nrtkudu reads any existing record(s) of the natural key to determine the correct writes, if any, that need to go back to Kudu to effect the arrived records of the natural key.

#### Type I

Where no history tracking is enabled on an encoder then nrtkudu will overwrite an existing record of the same key if the timestamp is equal or greater than the existing record and there has been a change in the data.

#### Type II

Where history tracking is enabled on an encoder then nrtkudu will follow Type II SCD conventions and store a Kudu record for each version of the key. A version is identified by the timestamp. Each record will contain the fields:
- Effective from timestamp: when the version first became effective
- Effective to timestamp: when the version finally was effective. If this has not arrived it will be persisted as 9999-12-31.
- Current flag: whether the version is currently effective

nrtkudu can handle record versions arriving out of order. All record versions of a key are scanned from Kudu and the arriving records for the key are then applied to that history, including setting metadata fields of existing versions that are found to be no longer effective.

### Example FIX implementation

### To be implemented
