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
package com.cloudera.labs.envelope.output;

import com.typesafe.config.Config;

/**
 * Outputs write data out of the Spark application.
 * Custom outputs should not directly implement this interface -- they should implement either
 * BulkOutput, RandomOutput, or both.
 */
public interface Output {

  /**
   * Configure the output.
   * This is called once by Envelope, immediately after output instantiation.
   * @param config The configuration of the output.
   */
  void configure(Config config);

}
