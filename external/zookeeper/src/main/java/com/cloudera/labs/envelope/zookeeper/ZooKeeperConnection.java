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

package com.cloudera.labs.envelope.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ZooKeeperConnection implements Watcher {

  private ZooKeeper zk;
  private CountDownLatch latch;

  private String connection;
  private int sessionTimeoutMs;
  private int connectionTimeoutMs;

  public static final String CONNECTION_CONFIG = "connection";

  private static final int DEFAULT_SESSION_TIMEOUT_MS = 1000;
  private static final int DEFAULT_CONNECTION_TIMEOUT_MS = 10000;

  public ZooKeeperConnection(String connection) {
    this.connection = connection;
    this.sessionTimeoutMs = DEFAULT_SESSION_TIMEOUT_MS;
    this.connectionTimeoutMs = DEFAULT_CONNECTION_TIMEOUT_MS;
  }

  public void setSessionTimeoutMs(int sessionTimeoutMs) {
    this.sessionTimeoutMs = sessionTimeoutMs;
  }

  public void setConnectionTimeoutMs(int connectionTimeoutMs) {
    this.connectionTimeoutMs = connectionTimeoutMs;
  }

  @Override
  public void process(WatchedEvent event) {
    if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
      latch.countDown();
    }
  }

  public synchronized ZooKeeper getZooKeeper() throws IOException, InterruptedException {
    if (zk == null || zk.getState() != ZooKeeper.States.CONNECTED) {
      latch = new CountDownLatch(1);
      zk = new ZooKeeper(connection, sessionTimeoutMs, this);

      boolean done = latch.await(connectionTimeoutMs, TimeUnit.MILLISECONDS);
      if (!done) {
        throw new InterruptedException("Did not connect to ZooKeeper within " +
            connectionTimeoutMs + " seconds");
      }
    }

    return zk;
  }

}
