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

package com.cloudera.labs.envelope.kudu;

import static com.cloudera.labs.envelope.kudu.KuduOutput.CONNECTION_CONFIG_NAME;

import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import java.util.Map;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.SessionConfiguration;

class KuduConnection {

  private KuduClient client;
  private KuduSession session;
  private Map<String, KuduTable> tables = Maps.newHashMap();
  private long lastUsed;

  KuduConnection(Config config, byte[] token) {
    client = new KuduClient.KuduClientBuilder(config.getString(CONNECTION_CONFIG_NAME)).build();
    if (token != null) {
      client.importAuthenticationCredentials(token);
    }
    session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
    session.setMutationBufferSpace(10000);
    session.setIgnoreAllDuplicateRows(KuduUtils.doesInsertIgnoreDuplicates(config));
  }

  KuduClient getClient() {
    return client;
  }

  KuduSession getSession() {
    return session;
  }

  KuduTable getTable(String tableName) throws KuduException {
    if (tables.containsKey(tableName)) {
      return tables.get(tableName);
    }
    else {
      KuduTable table = client.openTable(tableName);
      tables.put(tableName, table);
      return table;
    }
  }

  void close() throws KuduException {
    session.close();
    client.close();
  }

  long getLastUsed() {
    return lastUsed;
  }

  void setLastUsed(long lastUsed) {
    this.lastUsed = lastUsed;
  }

}
