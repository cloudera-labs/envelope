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

import com.cloudera.labs.envelope.load.Loadable;
import com.typesafe.config.Config;

/**
 * Inputs bring data into the Spark application, typically from an external source.
 * Custom inputs should not implement Input directly -- they should implement either
 * BatchInput or StreamInput.
 */
public interface Input extends Loadable {

  /**
   * Configure the input.
   * This is called once by Envelope, immediately after input instantiation.
   * @param config The configuration of the input.
   */
  void configure(Config config);

}
