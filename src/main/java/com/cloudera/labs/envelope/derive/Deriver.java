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
package com.cloudera.labs.envelope.derive;

import java.util.Map;

import org.apache.spark.sql.DataFrame;

import com.typesafe.config.Config;

/**
 * Derivers create new DataFrames derived from DataFrames already loaded into the Spark application.
 * Custom derivers should directly implement this interface.
 */
public interface Deriver {

  /**
   * Configure the deriver.
   * This is called once by Envelope, immediately after deriver instantiation.
   * @param config The configuration of the deriver.
   */
  void configure(Config config);

  /**
   * Derive a new DataFrame from the DataFrames of the dependency steps.
   * @param dependencies The map of step names to step DataFrames.
   * @return The derived DataFrame.
   * @throws Exception
   */
  DataFrame derive(Map<String, DataFrame> dependencies) throws Exception;

}
