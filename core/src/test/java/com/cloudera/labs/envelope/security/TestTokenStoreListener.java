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

package com.cloudera.labs.envelope.security;

import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.validate.Validation;
import com.cloudera.labs.envelope.validate.ValidationAssert;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.token.Token;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static com.cloudera.labs.envelope.security.SecurityUtils.SECURITY_PREFIX;
import static com.cloudera.labs.envelope.security.SecurityUtils.TOKENS_FILE;
import static com.cloudera.labs.envelope.spark.Contexts.APPLICATION_SECTION_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestTokenStoreListener {

  private static Configuration hadoopConf = new Configuration();
  private static Config config = ConfigUtils.configFromResource("/security/security_listener_test.conf");

  private static Config securityConfig;

  @BeforeClass
  public static void before() throws IOException {
    securityConfig = ConfigUtils.getOrElse(config,
        APPLICATION_SECTION_PREFIX + "." + SECURITY_PREFIX, ConfigFactory.empty());
    List<Path> files = SecurityUtils.getExistingTokenStoreFiles(securityConfig, hadoopConf, true);
    SecurityUtils.deleteTokenStoreFiles(files, 0, hadoopConf);
    Contexts.closeSparkSession(true);
    Contexts.initialize(config, Contexts.ExecutionMode.UNIT_TEST);
    Contexts.getSparkSession();
  }

  @AfterClass
  public static void after() throws IOException {
    List<Path> files = SecurityUtils.getExistingTokenStoreFiles(securityConfig, hadoopConf, true);
    SecurityUtils.deleteTokenStoreFiles(files, 0, hadoopConf);
    Contexts.closeSparkSession(true);
  }

  @Test
  public void testRenew() {
    try {
      // Lay a creds file down
      TokenStore tokenStore = new TokenStore();
      TestTokenProvider provider = new TestTokenProvider();
      tokenStore.addToken("test-provider", provider.obtainToken());
      tokenStore.write(securityConfig.getString(TOKENS_FILE), new Configuration());

      // Create listener
      TokenStoreListener listener = TokenStoreListener.get();
      assertNotNull(listener);

      // Get the token
      Token token = listener.getToken("test-provider");
      assertNotNull(token);
      int generation = ByteBuffer.wrap(token.getIdentifier()).getInt();
      assertEquals(0, generation);
      assertFalse(listener.hasChanged("test-provider"));

      // Write a new token
      tokenStore.addToken("test-provider", provider.obtainToken());
      tokenStore.write(securityConfig.getString(TOKENS_FILE) + ".1", new Configuration());

      // Get the new token
      Thread.sleep(1000);
      assertTrue(listener.hasChanged("test-provider"));
      token = listener.getToken("test-provider");
      assertNotNull(token);
      generation = ByteBuffer.wrap(token.getIdentifier()).getInt();
      assertEquals(1, generation);
      assertFalse(listener.hasChanged("test-provider"));

    } catch (IOException | InterruptedException e) {
      fail(e.getMessage());
    }
  }

}
