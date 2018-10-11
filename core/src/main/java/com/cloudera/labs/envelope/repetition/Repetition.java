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

package com.cloudera.labs.envelope.repetition;

import com.cloudera.labs.envelope.component.Component;
import com.cloudera.labs.envelope.run.BatchStep;
import com.typesafe.config.Config;

/**
 * A class which should contain functionality to repeat a step according to some regularly assessed criteria
 */
public interface Repetition extends Component {

  /**
   * Configure the repetition instance
   * @param config the configuration for the repetition instance
   */
  void configure(BatchStep step, String name, Config config);

}
