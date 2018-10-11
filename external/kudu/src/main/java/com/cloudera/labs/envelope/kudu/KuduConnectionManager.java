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

import com.cloudera.labs.envelope.security.TokenStoreListener;
import com.typesafe.config.Config;
import org.apache.kudu.client.KuduException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KuduConnectionManager {

  private static final Logger LOG = LoggerFactory.getLogger(KuduConnectionManager.class);
  private static KuduConnectionManager INSTANCE;

  private Config config;
  private String tokenAlias;
  private ThreadLocal<KuduConnection> connectionThreadLocal = new ThreadLocal<>();
  private long lastChanged = -1;

  private KuduConnectionManager(Config config) {
    tokenAlias = KuduUtils.buildCredentialAlias(config.getString(CONNECTION_CONFIG_NAME));
    this.config = config;
  }

  KuduConnection getConnection() throws KuduException {
    // get the thread-local connection or open a new one if expired or null
    if (connectionThreadLocal.get() == null) {
      LOG.debug("Creating new KuduConnection for thread");
      setNewConnection(KuduUtils.isSecure(config));
    } else if (TokenStoreListener.get().hasChanged(tokenAlias)) {
      LOG.debug("Kudu token has changed, refreshing KuduConnection for thread");
      lastChanged = System.currentTimeMillis();
      connectionThreadLocal.get().close();
      setNewConnection(KuduUtils.isSecure(config));
    } else if (connectionThreadLocal.get().getLastUsed() < lastChanged) {
      LOG.debug("Kudu token change detected in another thread, refreshing KuduConnection for this thread");
      connectionThreadLocal.get().close();
      setNewConnection(KuduUtils.isSecure(config));
    }

    return connectionThreadLocal.get();
  }

  private void setNewConnection(boolean secure) {
    byte[] token = null;
    if (secure) {
      token = TokenStoreListener.get().getToken(tokenAlias).getPassword();
    }
    connectionThreadLocal.set(new KuduConnection(config, token));
    connectionThreadLocal.get().setLastUsed(System.currentTimeMillis());
  }

  static synchronized KuduConnectionManager getKuduConnectionManager(Config config) {
    if (INSTANCE == null) {
      LOG.debug("Creating new KuduConnectionManager");
      INSTANCE = new KuduConnectionManager(config);
    }
    return INSTANCE;
  }

}
