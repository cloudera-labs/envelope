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

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class TestEventManager {

  @Before
  public void before() {
    EventManager.reset();
  }

  @Test
  public void testHandled() {
    TestingEventHandler eh = new TestingEventHandler();
    eh.setHandledEventTypes(Sets.newHashSet("hello"));
    EventManager.register(Collections.<EventHandler>singleton(eh));
    EventManager.notify(new Event("hello", "world"));

    assertEquals(1, eh.getHandledEvents().size());
  }

  @Test
  public void testMultipleHandlers() {
    TestingEventHandler eh1 = new TestingEventHandler();
    TestingEventHandler eh2 = new TestingEventHandler();
    eh1.setHandledEventTypes(Sets.newHashSet("hello"));
    eh2.setHandledEventTypes(Sets.newHashSet("hello"));
    EventManager.register(Sets.<EventHandler>newHashSet(eh1, eh2));
    EventManager.notify(new Event("hello", "world"));

    assertEquals(1, eh1.getHandledEvents().size());
    assertEquals(1, eh2.getHandledEvents().size());
  }

  @Test
  public void testMultipleHandlersAndMultipleEvents() {
    TestingEventHandler eh1 = new TestingEventHandler();
    TestingEventHandler eh2 = new TestingEventHandler();
    eh1.setHandledEventTypes(Sets.newHashSet("hello"));
    eh2.setHandledEventTypes(Sets.newHashSet("hello"));
    EventManager.register(Sets.<EventHandler>newHashSet(eh1, eh2));
    EventManager.notify(new Event("hello", "world"));
    EventManager.notify(new Event("hello", "world?"));

    assertEquals(2, eh1.getHandledEvents().size());
    assertEquals(2, eh2.getHandledEvents().size());
  }

  @Test
  public void testNotHandled() {
    TestingEventHandler eh = new TestingEventHandler();
    eh.setHandledEventTypes(Sets.newHashSet("hello"));
    EventManager.register(Collections.<EventHandler>singleton(eh));
    EventManager.notify(new Event("hello?", "world"));

    assertEquals(0, eh.getHandledEvents().size());
  }

  @Test
  public void testBuffered() {
    TestingEventHandler eh = new TestingEventHandler();
    eh.setHandledEventTypes(Sets.newHashSet("hello"));

    EventManager.notify(new Event("hello", "world"));
    EventManager.notify(new Event("hello", "world?"));
    EventManager.notify(new Event("hello", "world!"));
    assertEquals(0, eh.getHandledEvents().size());

    EventManager.register(Collections.<EventHandler>singleton(eh));
    assertEquals(3, eh.getHandledEvents().size());

    EventManager.notify(new Event("hello", "world..."));
    assertEquals(4, eh.getHandledEvents().size());
  }

}
