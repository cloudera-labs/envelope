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

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.run.BatchStep;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Regularly run a repetition according to the configured frequency as defined by the {@code every} parameter.
 */
public class ScheduledRepetition
    extends AbstractRepetition implements Runnable, ProvidesAlias, ProvidesValidations {

  private static final Logger LOG = LoggerFactory.getLogger(ScheduledRepetition.class);

  private static final String FREQUENCY_CONFIG = "every";

  @Override
  public void configure(BatchStep step, String name, Config config) {
    super.configure(step, name, config);
    Repetitions.get().submitRegularTask(this, config.getDuration(FREQUENCY_CONFIG, TimeUnit.MILLISECONDS));
  }

  @Override
  public void run() {
    LOG.info("Triggering repetition [" + name + "] for step [" +
        step.getName() + "] at [" + System.currentTimeMillis() + "]");
    repeatStep();
  }

  @Override
  public String getAlias() {
    return "schedule";
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .addAll(super.getValidations())
        .mandatoryPath(FREQUENCY_CONFIG)
        .build();
  }
  
}
