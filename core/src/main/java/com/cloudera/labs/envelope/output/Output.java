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

package com.cloudera.labs.envelope.output;

import com.cloudera.labs.envelope.component.Component;
import com.typesafe.config.Config;

/**
 * Outputs write data out of the Spark application.
 * Custom outputs should not directly implement this interface -- they should implement either
 * BulkOutput, RandomOutput, or both.
 */
public interface Output extends Component {

  /**
   * Configure the output.
   * This is called once by Envelope, immediately after output instantiation.
   * @param config The configuration of the output.
   */
  void configure(Config config);

}
