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

import static com.cloudera.labs.envelope.security.SecurityUtils.SECURITY_PREFIX;
import static com.cloudera.labs.envelope.security.SecurityUtils.TOKENS_FILE;
import static com.cloudera.labs.envelope.spark.Contexts.APPLICATION_SECTION_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.token.Token;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTokenStoreManager {

  @BeforeClass
  public static void before() throws IOException {
    Configuration hadoopConf = new Configuration();
    Config config = ConfigUtils.configFromResource("/security/security_manager_basic.conf");
    Config securityConfig = ConfigUtils.getOrElse(config,
        APPLICATION_SECTION_PREFIX + "." + SECURITY_PREFIX, ConfigFactory.empty());
    List<Path> files = SecurityUtils.getExistingTokenStoreFiles(securityConfig, hadoopConf, true);
    SecurityUtils.deleteTokenStoreFiles(files, 0, hadoopConf);
  }

  @Test
  public void testCreateAndInitialize() {
    Config config = ConfigUtils.configFromResource("/security/security_manager_basic.conf");
    Contexts.initialize(config, Contexts.ExecutionMode.UNIT_TEST);
    TokenStoreManager manager = new TokenStoreManager(ConfigUtils.getOrElse(config,
        APPLICATION_SECTION_PREFIX + "." + SECURITY_PREFIX, ConfigFactory.empty()));

    assertNotNull(manager);
  }

  @Test
  public void testGenerateRuntimeConfigsWithCredsFile() {
    Config config = ConfigUtils.configFromResource("/security/security_manager_basic.conf");
    Contexts.initialize(config, Contexts.ExecutionMode.UNIT_TEST);

    assertTrue(config.hasPath(APPLICATION_SECTION_PREFIX + "." + SECURITY_PREFIX + "." + TOKENS_FILE));
    assertEquals("tokenfile", config.getConfig(Contexts.APPLICATION_SECTION_PREFIX +
        "." + SECURITY_PREFIX).getString(TOKENS_FILE));
  }

  @Test
  public void testAddProvider() {
    try {
      Configuration hadoopConf = new Configuration();

      Config config = ConfigUtils.configFromResource("/security/security_manager_testrenewal.conf");
      Contexts.initialize(config, Contexts.ExecutionMode.UNIT_TEST);
      TokenStoreManager manager = new TokenStoreManager(ConfigUtils.getOrElse(config,
          APPLICATION_SECTION_PREFIX + "." + SECURITY_PREFIX, ConfigFactory.empty()));
      manager.addTokenProvider(new TestTokenProvider());
      manager.start();

      List<Path> files = SecurityUtils.getExistingTokenStoreFiles(ConfigUtils.getOrElse(config,
          APPLICATION_SECTION_PREFIX + "." + SECURITY_PREFIX, ConfigFactory.empty()),
          hadoopConf, true);
      assertEquals(1, files.size());
      TokenStore wrapper = new TokenStore();
      wrapper.read(files.get(0).toString(), hadoopConf);
      assertTrue(wrapper.getTokenAliases().contains("test-provider"));

      manager.stop();
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testCredsRenew() {
    try {
      Configuration hadoopConf = new Configuration();
      FileSystem fs = FileSystem.get(hadoopConf);

      Config config = ConfigUtils.configFromResource("/security/security_manager_testrenewal.conf");
      Contexts.initialize(config, Contexts.ExecutionMode.UNIT_TEST);
      TokenStoreManager manager = new TokenStoreManager(ConfigUtils.getOrElse(config,
          APPLICATION_SECTION_PREFIX + "." + SECURITY_PREFIX, ConfigFactory.empty()));
      TestTokenProvider provider = new TestTokenProvider();
      manager.addTokenProvider(provider);
      manager.start();

      List<Path> files = SecurityUtils.getExistingTokenStoreFiles(ConfigUtils.getOrElse(config,
          APPLICATION_SECTION_PREFIX + "." + SECURITY_PREFIX, ConfigFactory.empty()),
          hadoopConf, true);
      assertEquals(1, files.size());

      assertTrue(fs.exists(files.get(0)));

      TokenStore creds = new TokenStore();
      creds.read(files.get(0).toString(), fs.getConf());
      Token token = creds.getToken("test-provider");
      assertNotNull(token);
      int generation = ByteBuffer.wrap(token.getIdentifier()).getInt();
      assertEquals(0, generation);

      Thread.sleep(provider.getRenewalIntervalMillis() * 3);

      assertTrue(provider.getGeneration() > 0);
      files = SecurityUtils.getExistingTokenStoreFiles(ConfigUtils.getOrElse(config,
          APPLICATION_SECTION_PREFIX + "." + SECURITY_PREFIX, ConfigFactory.empty()),
          hadoopConf, true);
      assertTrue(files.size() > 0);

      int[] generations = new int[files.size()];
      int idx = 0;
      for (Path p : files) {
        if (fs.exists(new Path(p.toString() + ".READY"))) {
          creds.read(p.toString(), fs.getConf());
          token = creds.getToken("test-provider");
          assertNotNull(token);
          generations[idx++] = ByteBuffer.wrap(token.getIdentifier()).getInt();
        }
      }

      int[] validGenerations = Arrays.copyOfRange(generations, 0, idx);
      Arrays.sort(validGenerations);
      for (int i : validGenerations) {
        assertEquals(i, validGenerations[i]);
      }

      manager.stop();
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

}
