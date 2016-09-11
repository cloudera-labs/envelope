package com.cloudera.labs.envelope.input.stream;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.cloudera.labs.envelope.input.Input;
import com.typesafe.config.Config;

public abstract class StreamInput extends Input {

    public StreamInput(Config config) {
        super(config);
    }

    public abstract JavaDStream<Row> getDStream() throws Exception;
    
    public abstract StructType getSchema() throws Exception;
    
}
