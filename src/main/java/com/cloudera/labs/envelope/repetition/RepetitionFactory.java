/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.repetition;

import com.cloudera.labs.envelope.run.BatchStep;
import com.typesafe.config.Config;

import java.lang.reflect.Constructor;

public class RepetitionFactory {

  public static final String TYPE_CONFIG_NAME = "type";

  /**
   * Create a {@link Repetition} instance with the supplied name and associated with the supplied step. Shipped
   * implementations include {@code schedule} (see {@link ScheduledRepetition} and {@code flagfile} (see {@link FlagFileRepetition})
   * but arbitrary implementations can be selected using the fully-qualified class name. The {@code type} parameter
   * is mandatory in the supplied {@code repetitionConfig}.
   * @param step the linked {@link BatchStep} for this repetition
   * @param name the name of the repetition
   * @param repetitionConfig the configuration of the repetition
   * @return the Repetition instance
   */
  public static Repetition create(BatchStep step, String name, Config repetitionConfig) {
    if (!repetitionConfig.hasPath(TYPE_CONFIG_NAME)) {
      throw new RuntimeException("Repetition type not specified");
    }

    String repetitionType = repetitionConfig.getString(TYPE_CONFIG_NAME);

    Repetition repetition;

    switch(repetitionType) {
      case "schedule":
        repetition = new ScheduledRepetition();
        break;
      case "flagfile":
        repetition = new FlagFileRepetition();
        break;
      // TODO: hdfs directory pattern, Kafka topic, others...
      default:
        try {
          Class<?> clazz = Class.forName(repetitionType);
          Constructor<?> constructor = clazz.getConstructor();
          repetition = (Repetition)constructor.newInstance();
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
    }

    repetition.configure(step, name, repetitionConfig);

    return repetition;
  }

}
