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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Set;

public class EventManager {

  private static Set<Event> bufferedEvents = Sets.newHashSet();
  private static boolean registered = false;
  private static Collection<EventHandler> handlers = Sets.newHashSet();
  private static Logger LOG = LoggerFactory.getLogger(EventManager.class);

  public static void register(Collection<EventHandler> handlersToRegister) {
    for (EventHandler handlerToRegister : handlersToRegister) {
      for (Event bufferedEvent : bufferedEvents) {
        notifyForHandler(handlerToRegister, bufferedEvent);
      }
    }
    bufferedEvents.clear();

    handlers.addAll(handlersToRegister);
    registered = true;
  }

  public static void reset() {
    handlers.clear();
    registered = false;
  }

  public static void notify(Event event) {
    if (!registered) {
      bufferedEvents.add(event);
      return;
    }

    for (EventHandler handler : handlers) {
      notifyForHandler(handler, event);
    }
  }

  private static void notifyForHandler(EventHandler handler, Event event) {
    if (handler.canHandleEventType(event.getEventType())) {
      try {
        handler.handle(event);
      } catch (Exception e) {
        LOG.warn("Event handler " + handler.getClass().getSimpleName() + " failed to " +
            "handle event " + event.getClass().getSimpleName() + ". Stack trace printed at debug level.");
        if (LOG.isDebugEnabled()) {
          e.printStackTrace();
        }
      }
    }
  }

  public static boolean isHandled(String type) {
    for (EventHandler handler : handlers) {
      if (handler.canHandleEventType(type)) {
        return true;
      }
    }

    return false;
  }

}
