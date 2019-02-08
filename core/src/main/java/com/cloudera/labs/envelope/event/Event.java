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

import java.sql.Timestamp;
import java.util.Map;
import java.util.UUID;

/**
 * An event is the occurrence of a point in the lifecycle of an Envelope application.
 */
public class Event {

  private String id;
  private Timestamp timestamp;
  private String eventType;
  private String message;

  private String notifier;
  private Map<String, Object> metadata;

  /**
   * @param eventType The type of event that has occurred
   * @param message A plain text message that describes the occurrence of the event
   */
  public Event(String eventType, String message) {
    this(eventType, message, Maps.<String, Object>newHashMap());
  }

  /**
   * @param eventType The type of event that has occurred
   * @param message A plain text message that describes the occurrence of the event
   * @param metadata Zero or more metadata items associated with the event. These can be retrieved
   *                 by an event handler that is notified of the event
   */
  public Event(String eventType, String message, Map<String, Object> metadata) {
    this.id = generateId();
    this.timestamp = new Timestamp(System.currentTimeMillis());
    this.eventType = eventType;
    this.message = message;
    this.notifier = EventUtils.getCallingClassName();
    this.metadata = metadata;
  }

  public String getId() {
    return id;
  }

  public Timestamp getTimestamp() {
    return timestamp;
  }

  public String getEventType() {
    return eventType;
  }

  public String getMessage() {
    return message;
  }

  public String getNotifier() {
    return notifier;
  }

  public Map<String, Object> getMetadata() {
    return metadata;
  }

  public <T> T getMetadataItem(String key) {
    return (T) metadata.get(key);
  }

  private String generateId() {
    return UUID.randomUUID().toString();
  }

  @Override
  public String toString() {
    return "(" + getEventType() + ") " + getMessage();
  }

}
