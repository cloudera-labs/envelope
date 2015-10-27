package com.cloudera.fce.nrtkudu;

import java.io.Serializable;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

// Base class for decoding Kafka messages.

@SuppressWarnings("serial")
public abstract class Decoder implements Serializable {
    
    // Convert the Kafka messages to Avro records.
    public abstract List<GenericRecord> decode(Iterable<String> inputs);
    
    // The Avro schema for the messages.
    public abstract Schema getSchema();
    
    // Extract the key from the Kafka message so that all messages of the same key
    // for the micro-batch can be identified and sent to the same RDD partition.
    public abstract Object extractGroupByKey(String input);
    
}