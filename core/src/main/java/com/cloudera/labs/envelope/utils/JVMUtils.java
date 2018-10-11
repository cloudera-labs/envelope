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

package com.cloudera.labs.envelope.utils;

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Objects;

/**
 *
 */
public class JVMUtils {

  private static final ShutdownHook SHUTDOWN_HOOK = new ShutdownHook();

  private JVMUtils() {}

  /**
   * Adds a shutdown hook that tries to call {@link Closeable#close()} on the given argument
   * at JVM shutdown.
   *
   * @see <a href="https://github.com/OryxProject/oryx/blob/a16230a9697f1f62b10dce93491703ceb221669c/framework/oryx
   * -common/src/main/java/com/cloudera/oryx/common/lang/JVMUtils.java">Cloudera Oryx's JVMUtils</a>
   * @param closeable thing to close
   */
  public static void closeAtShutdown(Closeable closeable) {
    if (SHUTDOWN_HOOK.addCloseable(closeable)) {
      try {
        Runtime.getRuntime().addShutdownHook(new Thread(SHUTDOWN_HOOK, "OryxShutdownHookThread"));
      } catch (IllegalStateException ise) {
        System.err.println(String.format("Can't close %s at shutdown since shutdown is in progress", closeable));
      }
    }
  }
}

/**
 * Intended for use with {@link Runtime#addShutdownHook(Thread)} or similar mechanisms, this is a
 * {@link Runnable} that is configured with a list of {@link Closeable}s that are to be closed
 * at shutdown, when its {@link #run()} is called.
 */
final class ShutdownHook implements Runnable {

  private final Deque<Closeable> closeAtShutdown = new LinkedList<>();
  private volatile boolean triggered;

  @Override
  public void run() {
    triggered = true;
    synchronized (closeAtShutdown) {
      for (Closeable c : closeAtShutdown) {
        if (c != null) {
          try {
            c.close();
          } catch (IOException e) {
            System.err.println("Unable to close:" + e);
          }
        }
      }
    }
  }

  /**
   * @param closeable object to close at shutdown
   * @return {@code true} iff this is the first object to be registered
   * @throws IllegalStateException if already shutting down
   */
  boolean addCloseable(Closeable closeable) {
    Objects.requireNonNull(closeable);
    Preconditions.checkState(!triggered, "Can't add closeable %s; already shutting down", closeable);
    synchronized (closeAtShutdown) {
      boolean wasFirst = closeAtShutdown.isEmpty();
      closeAtShutdown.push(closeable);
      return wasFirst;
    }
  }

}


