/**
 * Copyright © 2016-2017 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.input;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaDStream;

/**
 * Stream inputs read in a JavaDStream of Spark SQL Rows from an external stream source.
 * Custom inputs that point to a streaming data source should implement StreamingInput.
 */
public interface StreamInput extends Input {

  /**
   * Read the external stream source.
   * @return The Spark distributed stream of Spark SQL Rows.
   * It is the responsibility of the StreamInput to translate its source into Row objects. The
   * Translator interface can assist with this.
   * @throws Exception
   */
  JavaDStream<Row> getDStream() throws Exception;

  /**
   * Get the schema of the input rows. This is used by Envelope to turn the stream
   * micro-batches into DataFrames.
   * @return The Spark SQL schema of the input rows.
   * @throws Exception
   */
  StructType getSchema() throws Exception;

}
