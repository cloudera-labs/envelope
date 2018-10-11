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

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;

/**
 * Stream inputs read in a JavaDStream of Spark SQL Rows from an external stream source.
 * Custom inputs that point to a streaming data source should implement StreamingInput.
 */
public interface StreamInput extends Input {

  /**
   * Provide the JavaDStream for the input stream. If the stream contains a key and message
   * they should be wrapped together in a single object.
   */
  JavaDStream<?> getDStream() throws Exception;
  
  /**
   * Provide a Spark pair function that prepares an object from the JavaDStream for
   * translation by turning it into a key and value.
   * @return
   */
  PairFunction<?, ?, ?> getPrepareFunction();

}
