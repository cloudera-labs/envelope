package com.cloudera.fce.nrtkudu;

import java.io.Serializable;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import scala.Tuple2;

// Base class for decoding Kafka messages.

@SuppressWarnings("serial")
public abstract class Decoder implements Serializable {
    
    // Convert the Kafka key+messages to Avro records.
    public abstract List<GenericRecord> decode(Iterable<Tuple2<String, String>> inputs);
    
    // The Avro schema for the messages.
    public abstract Schema getSchema();
    
    // Extract the natural key from the Kafka key+message so that all messages of the same
    // natural key for the micro-batch can be identified and sent to the same RDD partition.
    public abstract Object extractGroupByKey(String key, String message);
    
}