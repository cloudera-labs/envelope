package com.cloudera.labs.envelope.input;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaDStream;

public interface StreamInput extends Input {

  JavaDStream<Row> getDStream() throws Exception;

  StructType getSchema() throws Exception;

}
