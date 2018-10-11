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

import com.cloudera.labs.envelope.run.BatchStep;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.typesafe.config.Config;

import java.util.concurrent.TimeUnit;

public abstract class AbstractRepetition implements Repetition, ProvidesValidations {

  private static final String MIN_REPEAT_INTERVAL = "min-repeat-interval";
  private static final long DEFAULT_MIN_INTERVAL_MS = 60000;

  protected BatchStep step;
  protected String name;

  private long minimumIntervalMs = DEFAULT_MIN_INTERVAL_MS;
  private long lastRepeat = 0;

  @Override
  public void configure(BatchStep step, String name, Config config) {
    this.step = step;
    this.name = name;
    if (config.hasPath(MIN_REPEAT_INTERVAL)) {
      minimumIntervalMs = config.getDuration(MIN_REPEAT_INTERVAL, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Submit the step associated with this {@code Repetition} to be repeated
   */
  protected void repeatStep() {
    long currentTime = System.currentTimeMillis();
    if (currentTime - lastRepeat > minimumIntervalMs) {
      Repetitions.get().addRepeatingStep(step);
      lastRepeat = currentTime;
    }
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .optionalPath(MIN_REPEAT_INTERVAL)
        .build();
  }

}
