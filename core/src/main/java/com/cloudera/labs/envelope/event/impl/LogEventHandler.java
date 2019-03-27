/*
 * Copyright (c) 2015-2019, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.labs.envelope.event.impl;

import com.cloudera.labs.envelope.event.CoreEventTypes;
import com.cloudera.labs.envelope.event.Event;
import com.cloudera.labs.envelope.event.EventHandler;
import com.cloudera.labs.envelope.component.ProvidesAlias;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class LogEventHandler implements EventHandler, ProvidesAlias, ProvidesValidations {

  public static final String LOG_ALL_EVENTS_CONFIG = "log-all-events";

  private Set<String> handledEventTypes;

  @Override
  public void configure(Config config) {
    boolean allEventsEnabled = ConfigUtils.getOrElse(config, LOG_ALL_EVENTS_CONFIG, false);

    handledEventTypes = CoreEventTypes.getAllCoreEventTypes();
    if (!allEventsEnabled) {
      handledEventTypes.removeAll(CoreEventTypes.getHighPerformanceImpactCoreEventTypes());
    }
  }

  @Override
  public void handle(Event event) {
    String notifier = event.getNotifier();

    switch (event.getEventType()) {
      case CoreEventTypes.PIPELINE_STARTED:
      case CoreEventTypes.PIPELINE_FINISHED:
      case CoreEventTypes.STEPS_EXTRACTED:
      case CoreEventTypes.EXECUTION_MODE_DETERMINED:
      case CoreEventTypes.DATA_STEP_WRITTEN_TO_OUTPUT:
      case CoreEventTypes.DATA_STEP_DATA_GENERATED:
        logInfo(event, notifier);
        break;
      case CoreEventTypes.PIPELINE_EXCEPTION_OCCURRED:
        logError(event, notifier);
        break;
    }
  }

  private void logTrace(Event event, String notifier) {
    LoggerFactory.getLogger(notifier).trace(event.getMessage());
  }

  private void logDebug(Event event, String notifier) {
    LoggerFactory.getLogger(notifier).debug(event.getMessage());
  }

  private void logInfo(Event event, String notifier) {
    LoggerFactory.getLogger(notifier).info(event.getMessage());
  }

  private void logWarn(Event event, String notifier) {
    LoggerFactory.getLogger(notifier).warn(event.getMessage());
  }

  private void logError(Event event, String notifier) {
    LoggerFactory.getLogger(notifier).error(event.getMessage());
  }

  @Override
  public boolean canHandleEventType(String eventType) {
    return handledEventTypes.contains(eventType);
  }

  @Override
  public String getAlias() {
    return "log";
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .optionalPath(LOG_ALL_EVENTS_CONFIG, ConfigValueType.BOOLEAN)
        .build();
  }

}
