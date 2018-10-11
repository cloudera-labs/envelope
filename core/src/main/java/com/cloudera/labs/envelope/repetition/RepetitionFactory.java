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

import com.cloudera.labs.envelope.load.LoadableFactory;
import com.cloudera.labs.envelope.run.BatchStep;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepetitionFactory extends LoadableFactory<Repetition> {

  private static final Logger LOG = LoggerFactory.getLogger(RepetitionFactory.class);

  public static final String TYPE_CONFIG_NAME = "type";

  /**
   * Create a {@link Repetition} instance with the supplied name and associated with the supplied step.
   * The {@code type} parameter is mandatory in the supplied {@code repetitionConfig}.
   * @param step the linked {@link BatchStep} for this repetition
   * @param name the name of the repetition
   * @param repetitionConfig the configuration of the repetition
   * @return the Repetition instance
   */
  public static Repetition create(BatchStep step, String name, Config repetitionConfig, boolean configure) {
    if (!repetitionConfig.hasPath(TYPE_CONFIG_NAME)) {
      throw new RuntimeException("Repetition type not specified");
    }

    LOG.debug("Loaded repetitions from services: {}" + getLoadables(Repetition.class));
    String repetitionType = repetitionConfig.getString(TYPE_CONFIG_NAME);
    Repetition repetition;
    try {
      repetition = loadImplementation(Repetition.class, repetitionType);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    if (configure) {
      repetition.configure(step, name, repetitionConfig);
    }

    return repetition;
  }

}
