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

import com.google.common.collect.Maps;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestEvent {

  @Test
  public void testEvent() {
    Map<String, Object> metadata = Maps.newHashMap();
    metadata.put("metadata.key", "hello");
    Event event = new Event("event.type", "message", metadata);

    assertTrue(event.getId().length() > 0);
    assertEquals("event.type", event.getEventType());
    assertEquals("message", event.getMessage());
    assertEquals(metadata, event.getMetadata());
    assertEquals("hello", event.getMetadataItem("metadata.key"));
    assertEquals(this.getClass().getName(), event.getNotifier());
    assertNotNull(event.getTimestamp());
  }

  @Test
  public void testNoMetadata() {
    Event event = new Event("event.type", "message");

    assertTrue(event.getId().length() > 0);
    assertEquals("event.type", event.getEventType());
    assertEquals("message", event.getMessage());
    assertEquals(0, event.getMetadata().size());
    assertEquals(this.getClass().getName(), event.getNotifier());
    assertNotNull(event.getTimestamp());
  }

}
