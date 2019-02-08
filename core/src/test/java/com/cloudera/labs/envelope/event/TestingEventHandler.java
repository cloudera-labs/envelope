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

package com.cloudera.labs.envelope.event;

import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class TestingEventHandler implements EventHandler {

  // Used to identify the events from a single test. After a Runner#run call this class instance is
  // not visible, so we need to access the events captured by this class statically, but we might be
  // running tests in parallel and so we can't have a single list of events or the
  // events of the parallel pipelines will be mixed together. We can't use thread-locals because
  // events for a single pipeline can be submitted from multiple threads.
  private String executionKey;

  private static Map<String, Set<String>> handledEventTypes = Maps.newHashMap();
  private static Map<String, List<Event>> handledEvents = Maps.newHashMap();

  public TestingEventHandler() {
    // Initialize to a random value. Can be overridden by setExecutionKey for when the events
    // need to be retrieved statically by a known value.
    setExecutionKey(UUID.randomUUID().toString());
  }

  @Override
  public void configure(Config config) {
    this.setExecutionKey(config.getString("execution-key"));

    if (ConfigUtils.getOrElse(config, "handle-all-core-events", false)) {
      this.setHandledEventTypes(CoreEventTypes.getAllCoreEventTypes());
    }
  }

  @Override
  public void handle(Event event) {
    handledEvents.get(executionKey).add(event);
  }

  public List<Event> getHandledEvents() {
    return handledEvents.get(executionKey);
  }

  public static List<Event> getHandledEvents(String executionKey) {
    return handledEvents.get(executionKey);
  }

  @Override
  public boolean canHandleEventType(String eventType) {
    return handledEventTypes.get(executionKey).contains(eventType);
  }

  public void setHandledEventTypes(Set<String> newHandledEventTypes) {
    handledEventTypes.put(executionKey, newHandledEventTypes);
  }

  public void setExecutionKey(String executionKey) {
    this.executionKey = executionKey;
    handledEvents.put(executionKey, Lists.<Event>newArrayList());
  }

}
