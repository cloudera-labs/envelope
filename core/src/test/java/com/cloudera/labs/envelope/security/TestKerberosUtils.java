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

package com.cloudera.labs.envelope.security;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kerby.kerberos.kerb.KrbException;
import org.apache.kerby.kerberos.kerb.admin.kadmin.Kadmin;
import org.apache.kerby.kerberos.kerb.admin.kadmin.local.LocalKadminImpl;
import org.apache.kerby.kerberos.kerb.client.KrbConfig;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;
import org.apache.kerby.util.NetworkUtil;
import org.apache.spark.SparkConf;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import static com.cloudera.labs.envelope.security.SecurityConfig.KEYTAB_CONFIG;
import static com.cloudera.labs.envelope.security.SecurityConfig.REALM_CONFIG;
import static com.cloudera.labs.envelope.security.SecurityConfig.USER_PRINC_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestKerberosUtils {

  private static String oldKrb5 = System.getProperty("java.security.krb5.conf", null);
  private static SimpleKdcServer kdcServer;

  @ClassRule
  public static TemporaryFolder testFolder = new TemporaryFolder();

  @BeforeClass
  public static void setup() throws Exception {
    // Create KDC
    kdcServer = new SimpleKdcServer();
    kdcServer.setKdcHost("localhost");
    kdcServer.setWorkDir(testFolder.newFolder("kdc"));
    kdcServer.setAllowUdp(false);
    kdcServer.setAllowTcp(true);
    kdcServer.setKdcRealm("ENVELOPE.LOCAL");
    int serverPort = NetworkUtil.getServerPort();
    kdcServer.setKdcTcpPort(serverPort);
    kdcServer.init();
    kdcServer.start();

    Kadmin kadmin = new LocalKadminImpl(kdcServer.getKdcSetting(), kdcServer.getIdentityService());
    kadmin.addPrincipal("testuser@ENVELOPE.LOCAL");
    kadmin.exportKeytab(new File(testFolder.getRoot(), "kt"),"testuser@ENVELOPE.LOCAL");

    // Create krb5.conf
    String configString = Files.toString(
        new File(ClassLoader.getSystemResource("security/krb5.conf.template").getPath()),
        Charsets.UTF_8
    ).replaceAll("%PORT%", Integer.toString(serverPort));
    try (Writer writer = new FileWriter(new File(testFolder.getRoot(), "krb5.conf"))) {
      writer.write(configString);
      writer.flush();
    }
  }

  @Before
  public void before() {
    KerberosUtils.setSparkConf(null);
  }

  @AfterClass
  public static void cleanup() throws KrbException {
    if (oldKrb5 != null) {
      System.setProperty("java.security.krb5.conf", oldKrb5);
    } else {
      System.clearProperty("java.security.krb5.conf");
    }

    if (kdcServer != null) {
      kdcServer.stop();
    }
  }

  @Test
  public void testGetKrb5Config() throws KrbException {
    System.setProperty("java.security.krb5.conf",
        testFolder.getRoot().getAbsolutePath() + File.separator + "krb5.conf");
    KrbConfig config = KerberosUtils.getKrb5config();
    assertNotNull(config);
    assertEquals("ENVELOPE.LOCAL", config.getDefaultRealm());
  }

  @Test
  public void testGetKerberosRealm() throws KrbException {
    Config config = ConfigFactory.parseString(String.format("%s = foo.kt", KEYTAB_CONFIG));

    System.setProperty("java.security.krb5.conf",
        testFolder.getRoot().getAbsolutePath() + File.separator + "krb5.conf");
    String realm = KerberosUtils.getKerberosRealm(config);
    assertEquals("ENVELOPE.LOCAL", realm);

    config = ConfigFactory.parseString(String.format("%s = foo.kt\n%s = CORP.GLOBAL", KEYTAB_CONFIG, REALM_CONFIG));
    realm = KerberosUtils.getKerberosRealm(config);
    assertEquals("CORP.GLOBAL", realm);
  }

  @Test
  public void testGetRenewInterval() throws KrbException {
    Config config = ConfigFactory.empty();

    System.setProperty("java.security.krb5.conf",
        testFolder.getRoot().getAbsolutePath() + File.separator + "krb5.conf");
    long renewInterval = KerberosUtils.getRenewInterval(config);

    assertEquals(10 * 3600, renewInterval);
  }

  @Test
  public void testParseLifetime() {
    String dhms1 = " 2d 4h 30m 10s ";
    String dhms2 = "3d";
    String hms1 = "10:30";
    String hms2 = "12:45:30";
    String sec1 = "2750";
    String sec2 = " 3450 ";

    assertEquals(189010, KerberosUtils.parseLifetime(dhms1));
    assertEquals(86400 * 3, KerberosUtils.parseLifetime(dhms2));
    assertEquals(10 * 3600 + 30 * 60, KerberosUtils.parseLifetime(hms1));
    assertEquals(12 * 3600 + 45 * 60 + 30, KerberosUtils.parseLifetime(hms2));
    assertEquals(2750, KerberosUtils.parseLifetime(sec1));
    assertEquals(3450, KerberosUtils.parseLifetime(sec2));
  }

  @Test
  public void testKerberosLogin() throws IOException {
    System.setProperty("java.security.krb5.conf",
        testFolder.getRoot().getAbsolutePath() + File.separator + "krb5.conf");

    // Generate config file
    String configString = Files.toString(
        new File(ClassLoader.getSystemResource("security/krb-app.conf.template").getPath()),
        Charsets.UTF_8
    ).replaceAll("%KEYTAB%",testFolder.getRoot() + File.separator + "kt");
    Config config = ConfigFactory.parseString(configString);
    try {
      KerberosLoginContext kerberosLoginContext = KerberosUtils.createKerberosLoginContext("test-context",
          config);
      KerberosUtils.loginIfRequired(kerberosLoginContext, config);
      assertTrue(kerberosLoginContext.getLastLoggedIn() > 0);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testGetKerberosKeytabFromSpark() {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set("spark.yarn.keytab", "boom-oo-ya-ta-ta-ta.kt");
    KerberosUtils.setSparkConf(sparkConf);
    Map<String, Object> configMap = new HashMap<>();
    Config config = ConfigFactory.parseMap(configMap);

    String keytab = KerberosUtils.getKerberosKeytab(config);

    assertEquals("boom-oo-ya-ta-ta-ta.kt", keytab);
  }

  @Test
  public void testGetKerberosKeytabFromConfig() {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set("spark.yarn.keytab", "boom-oo-ya-ta-ta-ta.kt");
    KerberosUtils.setSparkConf(sparkConf);
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(KEYTAB_CONFIG, "foo.kt");
    Config config = ConfigFactory.parseMap(configMap);

    String keytab = KerberosUtils.getKerberosKeytab(config);

    assertEquals("foo.kt", keytab);
  }

  @Test
  public void testGetKerberosPrincFromSpark() {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set("spark.yarn.principal", "boom-oo-ya-ta-ta-ta");
    KerberosUtils.setSparkConf(sparkConf);
    Map<String, Object> configMap = new HashMap<>();
    Config config = ConfigFactory.parseMap(configMap);

    String principal = KerberosUtils.getKerberosPrincipal(config);

    assertEquals("boom-oo-ya-ta-ta-ta", principal);
  }

  @Test
  public void testGetKerberosPrincFromConfig() {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set("spark.yarn.principal", "boom-oo-ya-ta-ta-ta");
    KerberosUtils.setSparkConf(sparkConf);
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(USER_PRINC_CONFIG, "foo");
    Config config = ConfigFactory.parseMap(configMap);

    String principal = KerberosUtils.getKerberosPrincipal(config);

    assertEquals("foo", principal);
  }

}
