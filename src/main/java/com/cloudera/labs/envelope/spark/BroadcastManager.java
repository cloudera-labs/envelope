/**
 * Copyright © 2016-2017 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.spark;

import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.spark.broadcast.Broadcast;

/**
 * Allows users to store and retrieve broadcasts made throughout a envelope's
 * lifecycle.
 *
 * This management service is useful for storing a broadcast that you want viewable
 * by a subsequent step.
 *
 *  Example:
 *     BroadcastManager<ComplexRecord> recordManager = new BroadcastManager<>();
 *     Broadcast<ComplexRecord> complexRecordBroadcast = sparkContext.broadcast(complexRecord);
 *     recordManager.put("abc", complexRecordBroadcast);
 *
 *     // In a later step...
 *     BroadcastManager<ComplexRecord> recordManager = new BroadcastManager<>();
 *     Broadcast<ComplexRecord> complexRecordBroadcast = recordManager.getBroadcast("abc");
 *
 * @param <T>
 */
public class BroadcastManager<T> {
  private static Map<String, Broadcast<?>> broadcastLookup = Maps.newConcurrentMap();


  /**
   * Registers a broadcast returned by the Spark context using a lookup name.
   * @param name
   *  Name for later lookup
   * @param broadcast
   *  The broadcast created using Spark
   */
  public void registerBroadcast(String name,Broadcast<T> broadcast) {
    broadcastLookup.put(name, broadcast);
  }

  /**
   * Looks up a stored broadcast using the lookup name.
   * @param name
   *  Name of stored broadcast
   * @return
   */
  public Broadcast<T> getBroadcast(String name) {
    if (broadcastLookup.containsKey(name)) {
      return (Broadcast<T>) broadcastLookup.get(name);
    } else {
      return null;
    }
  }
}
