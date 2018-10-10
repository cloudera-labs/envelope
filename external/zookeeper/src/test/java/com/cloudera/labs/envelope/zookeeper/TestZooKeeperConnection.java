/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.zookeeper;

import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestZooKeeperConnection {

  private static TestingServer zk;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void before() throws Exception {
    zk = new TestingServer(2181, true);
  }

  @Test
  public void testGoodConnection() throws Exception {
    new ZooKeeperConnection("localhost:2181").getZooKeeper();
  }

  @Test
  public void testBadConnection() throws Exception {
    thrown.expect(InterruptedException.class);
    new ZooKeeperConnection("localhost:2182").getZooKeeper();
  }

  @AfterClass
  public static void after() throws Exception {
    zk.close();
  }

}
