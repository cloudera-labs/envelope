/*
 * Copyright (c) 2015-2018, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.labs.envelope.input;

import com.cloudera.labs.envelope.schema.DeclaresProvidingSchema;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.api.java.JavaDStream;

/**
 * Stream inputs read in a JavaDStream of Spark SQL Rows from an external stream source.
 * Custom inputs that point to a streaming data source should implement StreamInput.
 *
 * Stream inputs must declare the schema they provide so that DataFrames can be made
 * from the micro-batches.
 */
public interface StreamInput extends Input, DeclaresProvidingSchema {

  /**
   * Get the raw JavaDStream for the input stream.
   */
  JavaDStream<?> getDStream() throws Exception;

  /**
   * Get the Spark function that encodes the message object from the JavaDStream as
   * a Spark SQL Row where the raw message is the 'value' field and any other associated
   * metadata of the message can optionally be added as other fields. The output Row of this
   * function will be sent to the translator associated with the corresponding streaming step.
   */
  Function<?, Row> getMessageEncoderFunction();

}
