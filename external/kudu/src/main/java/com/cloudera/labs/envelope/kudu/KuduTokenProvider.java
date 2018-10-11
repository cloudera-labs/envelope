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

import static com.cloudera.labs.envelope.security.SecurityUtils.RENEW_INTERVAL;
import static com.cloudera.labs.envelope.security.SecurityUtils.SECURITY_PREFIX;
import static com.cloudera.labs.envelope.security.TokenStoreManager.ONE_DAY_MILLIS;

import com.cloudera.labs.envelope.security.SecurityUtils;
import com.cloudera.labs.envelope.security.TokenProvider;
import com.typesafe.config.Config;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.kudu.client.KuduClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KuduTokenProvider implements TokenProvider {

  private static final Logger LOG = LoggerFactory.getLogger(KuduTokenProvider.class);

  private String kuduMasterAddresses;
  private long renewalIntervalMillis = ONE_DAY_MILLIS;

  KuduTokenProvider(String kuduMasterAddresses, Config config) {
    this.kuduMasterAddresses = kuduMasterAddresses;
    if (config.hasPath(SECURITY_PREFIX + "." + RENEW_INTERVAL)) {
      this.renewalIntervalMillis = config.getDuration(SECURITY_PREFIX + "." + RENEW_INTERVAL,
          TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public Token obtainToken() throws Exception {
    try {
      if (UserGroupInformation.getCurrentUser().isFromKeytab()) {
        // If we're using a keytab, we're probably in cluster mode
        // can we rely on Spark already having logged in at least once?
        UserGroupInformation.getCurrentUser().reloginFromKeytab();
      }
      KuduClient client = new KuduClient.KuduClientBuilder(kuduMasterAddresses).build();
      byte[] token = client.exportAuthenticationCredentials();
      client.close();
      LOG.debug("Obtained new Kudu token for {}", kuduMasterAddresses);
      return SecurityUtils.createToken(token);
    } catch (Exception e) {
      LOG.error("Could not obtain new security token from {}", kuduMasterAddresses);
      throw e;
    }
  }

  @Override
  public long getRenewalIntervalMillis() {
    return renewalIntervalMillis;
  }

  @Override
  public String getAlias() {
    return KuduUtils.buildCredentialAlias(kuduMasterAddresses);
  }

}
