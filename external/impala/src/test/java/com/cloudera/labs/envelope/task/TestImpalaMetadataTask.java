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

package com.cloudera.labs.envelope.task;

import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.validate.ValidationResult;
import com.cloudera.labs.envelope.validate.ValidationUtils;
import com.cloudera.labs.envelope.validate.Validator;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.cloudera.labs.envelope.task.ImpalaMetadataTask.AUTH_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTask.HOST_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTask.KEYTAB_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTask.PASSWORD_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTask.PORT_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTask.QUERY_LOCATION_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTask.QUERY_PART_SPEC_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTask.QUERY_PART_RANGE_END_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTask.QUERY_PART_RANGE_START_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTask.QUERY_PART_RANGE_VAL_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTask.QUERY_TABLE_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTask.QUERY_TYPE_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTask.REALM_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTask.SSL_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTask.SSL_TRUSTSTORE_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTask.SSL_TRUSTSTORE_PASSWORD_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTask.USERNAME_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTask.USER_PRINC_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestImpalaMetadataTask {

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
        new File(ClassLoader.getSystemResource("krb5.conf.template").getPath()),
        Charsets.UTF_8
    ).replaceAll("%PORT%", Integer.toString(serverPort));
    try (Writer writer = new FileWriter(new File(testFolder.getRoot(), "krb5.conf"))) {
      writer.write(configString);
      writer.flush();
    }
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
  public void testBuildNoAuthConnectionString() {
    Properties properties = new Properties();
    properties.setProperty(HOST_CONFIG, "testhost");
    properties.setProperty(QUERY_TYPE_CONFIG, "refresh");
    properties.setProperty(QUERY_TABLE_CONFIG, "testtable");
    Config config = ConfigFactory.parseProperties(properties);
    ImpalaMetadataTask metadataTask = new ImpalaMetadataTask();
    metadataTask.configure(config);

    String connectionString = metadataTask.buildConnectionString();
    assertEquals("jdbc:hive2://testhost:21050/;auth=noSasl", connectionString);
  }

  @Test
  public void testBuildNoAuthConnectionStringCustomPort() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(HOST_CONFIG, "testhost");
    configMap.put(QUERY_TYPE_CONFIG, "refresh");
    configMap.put(QUERY_TABLE_CONFIG, "testtable");
    configMap.put(PORT_CONFIG, 23050);
    Config config = ConfigFactory.parseMap(configMap);
    ImpalaMetadataTask metadataTask = new ImpalaMetadataTask();
    metadataTask.configure(config);

    String connectionString = metadataTask.buildConnectionString();
    assertEquals("jdbc:hive2://testhost:23050/;auth=noSasl", connectionString);
  }

  @Test
  public void testBuildLdapAuthConnectionString() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(HOST_CONFIG, "testhost");
    configMap.put(QUERY_TYPE_CONFIG, "refresh");
    configMap.put(QUERY_TABLE_CONFIG, "testtable");
    configMap.put(AUTH_CONFIG, "ldap");
    configMap.put(USERNAME_CONFIG, "user");
    configMap.put(PASSWORD_CONFIG, "password");
    Config config = ConfigFactory.parseMap(configMap);
    ImpalaMetadataTask metadataTask = new ImpalaMetadataTask();
    metadataTask.configure(config);

    String connectionString = metadataTask.buildConnectionString();
    assertEquals("jdbc:hive2://testhost:21050/", connectionString);
  }

  @Test
  public void testBuildLdapAuthConnectionStringWithSsl() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(HOST_CONFIG, "testhost");
    configMap.put(QUERY_TYPE_CONFIG, "refresh");
    configMap.put(QUERY_TABLE_CONFIG, "testtable");
    configMap.put(AUTH_CONFIG, "ldap");
    configMap.put(USERNAME_CONFIG, "user");
    configMap.put(PASSWORD_CONFIG, "password");
    configMap.put(SSL_CONFIG, true);
    Config config = ConfigFactory.parseMap(configMap);
    ImpalaMetadataTask metadataTask = new ImpalaMetadataTask();
    metadataTask.configure(config);

    String connectionString = metadataTask.buildConnectionString();
    assertEquals("jdbc:hive2://testhost:21050/;ssl=true", connectionString);
  }

  @Test
  public void testBuildLdapAuthConnectionStringWithSslWithTrustStore() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(HOST_CONFIG, "testhost");
    configMap.put(QUERY_TYPE_CONFIG, "refresh");
    configMap.put(QUERY_TABLE_CONFIG, "testtable");
    configMap.put(AUTH_CONFIG, "ldap");
    configMap.put(USERNAME_CONFIG, "user");
    configMap.put(PASSWORD_CONFIG, "password");
    configMap.put(SSL_CONFIG, true);
    configMap.put(SSL_TRUSTSTORE_CONFIG, "/my/truststore.jks");
    Config config = ConfigFactory.parseMap(configMap);
    ImpalaMetadataTask metadataTask = new ImpalaMetadataTask();
    metadataTask.configure(config);

    String connectionString = metadataTask.buildConnectionString();
    assertEquals("jdbc:hive2://testhost:21050/;ssl=true;sslTrustStore=/my/truststore.jks", connectionString);
  }

  @Test
  public void testBuildLdapAuthConnectionStringWithSslWithTrustStoreWithPassword() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(HOST_CONFIG, "testhost");
    configMap.put(QUERY_TYPE_CONFIG, "refresh");
    configMap.put(QUERY_TABLE_CONFIG, "testtable");
    configMap.put(AUTH_CONFIG, "ldap");
    configMap.put(USERNAME_CONFIG, "user");
    configMap.put(PASSWORD_CONFIG, "password");
    configMap.put(SSL_CONFIG, true);
    configMap.put(SSL_TRUSTSTORE_CONFIG, "/my/truststore.jks");
    configMap.put(SSL_TRUSTSTORE_PASSWORD_CONFIG, "public");
    Config config = ConfigFactory.parseMap(configMap);
    ImpalaMetadataTask metadataTask = new ImpalaMetadataTask();
    metadataTask.configure(config);

    String connectionString = metadataTask.buildConnectionString();
    assertEquals("jdbc:hive2://testhost:21050/;ssl=true;sslTrustStore=/my/truststore.jks;" +
        "trustStorePassword=public", connectionString);
  }

  @Test
  public void testBuildKerberosAuthConnectionString() {
    System.setProperty("java.security.krb5.conf",
        testFolder.getRoot().getAbsolutePath() + File.separator + "krb5.conf");

    Map<String, Object> configMap = new HashMap<>();
    configMap.put(HOST_CONFIG, "testhost");
    configMap.put(QUERY_TYPE_CONFIG, "refresh");
    configMap.put(QUERY_TABLE_CONFIG, "testtable");
    configMap.put(AUTH_CONFIG, "kerberos");
    configMap.put(KEYTAB_CONFIG, "foo.kt");
    configMap.put(USER_PRINC_CONFIG, "user");
    Config config = ConfigFactory.parseMap(configMap);
    ImpalaMetadataTask metadataTask = new ImpalaMetadataTask();
    metadataTask.configure(config);

    String connectionString = metadataTask.buildConnectionString();
    assertEquals("jdbc:hive2://testhost:21050/;principal=impala/testhost@ENVELOPE.LOCAL" +
        ";auth=kerberos;kerberosAuthType=fromSubject", connectionString);
  }

  @Test
  public void testBuildKerberosAuthConnectionStringWithSsl() {
    System.setProperty("java.security.krb5.conf",
        testFolder.getRoot().getAbsolutePath() + File.separator + "krb5.conf");

    Map<String, Object> configMap = new HashMap<>();
    configMap.put(HOST_CONFIG, "testhost");
    configMap.put(QUERY_TYPE_CONFIG, "refresh");
    configMap.put(QUERY_TABLE_CONFIG, "testtable");
    configMap.put(AUTH_CONFIG, "kerberos");
    configMap.put(KEYTAB_CONFIG, "foo.kt");
    configMap.put(USER_PRINC_CONFIG, "user");
    configMap.put(SSL_CONFIG, true);
    Config config = ConfigFactory.parseMap(configMap);
    ImpalaMetadataTask metadataTask = new ImpalaMetadataTask();
    metadataTask.configure(config);

    String connectionString = metadataTask.buildConnectionString();
    assertEquals("jdbc:hive2://testhost:21050/;" +
        "principal=impala/testhost@ENVELOPE.LOCAL;auth=kerberos;kerberosAuthType=fromSubject;ssl=true", connectionString);
  }

  @Test
  public void testBuildKerberosAuthConnectionStringWithSslWithTrustStore() {
    System.setProperty("java.security.krb5.conf",
        testFolder.getRoot().getAbsolutePath() + File.separator + "krb5.conf");

    Map<String, Object> configMap = new HashMap<>();
    configMap.put(HOST_CONFIG, "testhost");
    configMap.put(QUERY_TYPE_CONFIG, "refresh");
    configMap.put(QUERY_TABLE_CONFIG, "testtable");
    configMap.put(AUTH_CONFIG, "kerberos");
    configMap.put(KEYTAB_CONFIG, "foo.kt");
    configMap.put(USER_PRINC_CONFIG, "user");
    configMap.put(SSL_CONFIG, true);
    configMap.put(SSL_TRUSTSTORE_CONFIG, "/my/truststore.jks");
    Config config = ConfigFactory.parseMap(configMap);
    ImpalaMetadataTask metadataTask = new ImpalaMetadataTask();
    metadataTask.configure(config);

    String connectionString = metadataTask.buildConnectionString();
    assertEquals("jdbc:hive2://testhost:21050/;" +
        "principal=impala/testhost@ENVELOPE.LOCAL;auth=kerberos;kerberosAuthType=fromSubject" +
        ";ssl=true;sslTrustStore=/my/truststore.jks", connectionString);
  }

  @Test
  public void testBuildKerberosAuthConnectionStringWithSslWithTrustStoreWithPassword() {
    System.setProperty("java.security.krb5.conf",
        testFolder.getRoot().getAbsolutePath() + File.separator + "krb5.conf");

    Map<String, Object> configMap = new HashMap<>();
    configMap.put(HOST_CONFIG, "testhost");
    configMap.put(QUERY_TYPE_CONFIG, "refresh");
    configMap.put(QUERY_TABLE_CONFIG, "testtable");
    configMap.put(AUTH_CONFIG, "kerberos");
    configMap.put(KEYTAB_CONFIG, "foo.kt");
    configMap.put(USER_PRINC_CONFIG, "user");
    configMap.put(SSL_CONFIG, true);
    configMap.put(SSL_TRUSTSTORE_CONFIG, "/my/truststore.jks");
    configMap.put(SSL_TRUSTSTORE_PASSWORD_CONFIG, "public");
    Config config = ConfigFactory.parseMap(configMap);
    ImpalaMetadataTask metadataTask = new ImpalaMetadataTask();
    metadataTask.configure(config);

    String connectionString = metadataTask.buildConnectionString();
    assertEquals("jdbc:hive2://testhost:21050/;" +
        "principal=impala/testhost@ENVELOPE.LOCAL;auth=kerberos;kerberosAuthType=fromSubject" +
        ";ssl=true;sslTrustStore=/my/truststore.jks;" +
        "trustStorePassword=public", connectionString);
  }

  @Test
  public void testGetKrb5Config() throws KrbException {
    ImpalaMetadataTask task = new ImpalaMetadataTask();

    System.setProperty("java.security.krb5.conf",
        testFolder.getRoot().getAbsolutePath() + File.separator + "krb5.conf");
    KrbConfig config = task.getKrb5config();
    assertNotNull(config);
    assertEquals("ENVELOPE.LOCAL", config.getDefaultRealm());
  }

  @Test
  public void testGetKerberosRealm() throws KrbException {
    ImpalaMetadataTask task = new ImpalaMetadataTask();
    Config config = ConfigFactory.parseString(String.format("%s = foo.kt", KEYTAB_CONFIG));

    System.setProperty("java.security.krb5.conf",
        testFolder.getRoot().getAbsolutePath() + File.separator + "krb5.conf");
    String realm = task.getKerberosRealm(config);
    assertEquals("ENVELOPE.LOCAL", realm);

    config = ConfigFactory.parseString(String.format("%s = foo.kt\n%s = CORP.GLOBAL", KEYTAB_CONFIG, REALM_CONFIG));
    realm = task.getKerberosRealm(config);
    assertEquals("CORP.GLOBAL", realm);
  }

  @Test
  public void testGetRenewInterval() throws KrbException {
    ImpalaMetadataTask task = new ImpalaMetadataTask();
    Config config = ConfigUtils.configFromResource("/impala-task.conf");
    task.configure(config);

    System.setProperty("java.security.krb5.conf",
        testFolder.getRoot().getAbsolutePath() + File.separator + "krb5.conf");
    long renewInterval = task.getRenewInterval();

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

    assertEquals(189010, ImpalaMetadataTask.parseLifetime(dhms1));
    assertEquals(86400 * 3, ImpalaMetadataTask.parseLifetime(dhms2));
    assertEquals(10 * 3600 + 30 * 60, ImpalaMetadataTask.parseLifetime(hms1));
    assertEquals(12 * 3600 + 45 * 60 + 30, ImpalaMetadataTask.parseLifetime(hms2));
    assertEquals(2750, ImpalaMetadataTask.parseLifetime(sec1));
    assertEquals(3450, ImpalaMetadataTask.parseLifetime(sec2));
  }

  @Test
  public void testInvalidConfigs() {
    for (int i = 1; i <= 6; i++) {
      Config config = ConfigUtils.configFromResource(String.format("/invalid-%d.conf", i));
      ImpalaMetadataTask task = new ImpalaMetadataTask();
      List<ValidationResult> validations = Validator.validate(task, config);
      assertTrue(ValidationUtils.hasValidationFailures(validations));
    }
  }

  @Test
  public void testValidConfigs() {
    for (int i = 1; i <= 6; i++) {
      Config config = ConfigUtils.configFromResource(String.format("/valid-%d.conf", i));
      ImpalaMetadataTask task = new ImpalaMetadataTask();
      List<ValidationResult> validations = Validator.validate(task, config);
      assertFalse(ValidationUtils.hasValidationFailures(validations));
    }
  }

  @Test
  public void testKerberosLogin() throws IOException {
    System.setProperty("java.security.krb5.conf",
        testFolder.getRoot().getAbsolutePath() + File.separator + "krb5.conf");

    // Generate config file
    String configString = Files.toString(
        new File(ClassLoader.getSystemResource("impala-krb5.conf.template").getPath()),
        Charsets.UTF_8
    ).replaceAll("%KEYTAB%",testFolder.getRoot() + File.separator + "kt");
    Config config = ConfigFactory.parseString(configString);
    ImpalaMetadataTask task = new ImpalaMetadataTask();
    task.configure(config);
    try {
      task.login();
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testDeriveRefreshQuery() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(HOST_CONFIG, "testhost");
    configMap.put(QUERY_TYPE_CONFIG, "refresh");
    configMap.put(QUERY_TABLE_CONFIG, "testtable");
    configMap.put(AUTH_CONFIG, "none");
    Config config = ConfigFactory.parseMap(configMap);
    ImpalaMetadataTask metadataTask = new ImpalaMetadataTask();
    metadataTask.configure(config);

    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    String query = metadataTask.deriveQuery(dependencies);

    assertEquals("REFRESH testtable", query);
  }

  @Test
  public void testDeriveInvalidateQuery() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(HOST_CONFIG, "testhost");
    configMap.put(QUERY_TYPE_CONFIG, "invalidate");
    configMap.put(QUERY_TABLE_CONFIG, "testtable");
    configMap.put(AUTH_CONFIG, "none");
    Config config = ConfigFactory.parseMap(configMap);
    ImpalaMetadataTask metadataTask = new ImpalaMetadataTask();
    metadataTask.configure(config);

    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    String query = metadataTask.deriveQuery(dependencies);

    assertEquals("INVALIDATE METADATA testtable", query);
  }

  @Test
  public void testDeriveAddPartitionQuery() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(HOST_CONFIG, "testhost");
    configMap.put(QUERY_TYPE_CONFIG, "add_partition");
    configMap.put(QUERY_TABLE_CONFIG, "testtable");
    configMap.put(QUERY_PART_SPEC_CONFIG, "country='US'");
    configMap.put(AUTH_CONFIG, "none");
    Config config = ConfigFactory.parseMap(configMap);
    ImpalaMetadataTask metadataTask = new ImpalaMetadataTask();
    metadataTask.configure(config);

    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    String query = metadataTask.deriveQuery(dependencies);

    assertEquals("ALTER TABLE testtable ADD IF NOT EXISTS PARTITION (country='US')", query);
  }

  @Test
  public void testDeriveDropPartitionQuery() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(HOST_CONFIG, "testhost");
    configMap.put(QUERY_TYPE_CONFIG, "drop_partition");
    configMap.put(QUERY_TABLE_CONFIG, "testtable");
    configMap.put(QUERY_PART_SPEC_CONFIG, "country='US'");
    configMap.put(AUTH_CONFIG, "none");
    Config config = ConfigFactory.parseMap(configMap);
    ImpalaMetadataTask metadataTask = new ImpalaMetadataTask();
    metadataTask.configure(config);

    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    String query = metadataTask.deriveQuery(dependencies);

    assertEquals("ALTER TABLE testtable DROP IF EXISTS PARTITION (country='US')", query);
  }

  @Test
  public void testDeriveAddPartitionLocationQuery() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(HOST_CONFIG, "testhost");
    configMap.put(QUERY_TYPE_CONFIG, "add_partition");
    configMap.put(QUERY_TABLE_CONFIG, "testtable");
    configMap.put(QUERY_PART_SPEC_CONFIG, "country='US'");
    configMap.put(QUERY_LOCATION_CONFIG, "/path/to/custom/location/country_US");
    configMap.put(AUTH_CONFIG, "none");
    Config config = ConfigFactory.parseMap(configMap);
    ImpalaMetadataTask metadataTask = new ImpalaMetadataTask();
    metadataTask.configure(config);

    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    String query = metadataTask.deriveQuery(dependencies);

    assertEquals("ALTER TABLE testtable ADD IF NOT EXISTS PARTITION (country='US') " +
        "LOCATION '/path/to/custom/location/country_US'", query);
  }

  @Test
  public void testDeriveAddRangePartitionValueQuery() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(HOST_CONFIG, "testhost");
    configMap.put(QUERY_TYPE_CONFIG, "add_partition");
    configMap.put(QUERY_TABLE_CONFIG, "testtable");
    configMap.put(QUERY_PART_RANGE_VAL_CONFIG, "20190122");
    configMap.put(AUTH_CONFIG, "none");
    Config config = ConfigFactory.parseMap(configMap);
    ImpalaMetadataTask metadataTask = new ImpalaMetadataTask();
    metadataTask.configure(config);

    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    String query = metadataTask.deriveQuery(dependencies);

    assertEquals("ALTER TABLE testtable ADD IF NOT EXISTS RANGE PARTITION VALUE = 20190122", query);
  }

  @Test
  public void testDeriveAddRangePartitionBoundariesQuery() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(HOST_CONFIG, "testhost");
    configMap.put(QUERY_TYPE_CONFIG, "add_partition");
    configMap.put(QUERY_TABLE_CONFIG, "testtable");
    configMap.put(QUERY_PART_RANGE_START_CONFIG, "20190122");
    configMap.put(QUERY_PART_RANGE_END_CONFIG, "20190123");
    configMap.put(AUTH_CONFIG, "none");
    Config config = ConfigFactory.parseMap(configMap);
    ImpalaMetadataTask metadataTask = new ImpalaMetadataTask();
    metadataTask.configure(config);

    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    String query = metadataTask.deriveQuery(dependencies);

    assertEquals("ALTER TABLE testtable ADD IF NOT EXISTS RANGE PARTITION 20190122 <= VALUES < 20190123", query);
  }

  @Test
  public void testDeriveDropRangePartitionValueQuery() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(HOST_CONFIG, "testhost");
    configMap.put(QUERY_TYPE_CONFIG, "drop_partition");
    configMap.put(QUERY_TABLE_CONFIG, "testtable");
    configMap.put(QUERY_PART_RANGE_VAL_CONFIG, "20190122");
    configMap.put(AUTH_CONFIG, "none");
    Config config = ConfigFactory.parseMap(configMap);
    ImpalaMetadataTask metadataTask = new ImpalaMetadataTask();
    metadataTask.configure(config);

    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    String query = metadataTask.deriveQuery(dependencies);

    assertEquals("ALTER TABLE testtable DROP IF EXISTS RANGE PARTITION VALUE = 20190122", query);
  }

  @Test
  public void testDeriveDropRangePartitionBoundariesQuery() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(HOST_CONFIG, "testhost");
    configMap.put(QUERY_TYPE_CONFIG, "drop_partition");
    configMap.put(QUERY_TABLE_CONFIG, "testtable");
    configMap.put(QUERY_PART_RANGE_START_CONFIG, "20190122");
    configMap.put(QUERY_PART_RANGE_END_CONFIG, "20190123");
    configMap.put(AUTH_CONFIG, "none");
    Config config = ConfigFactory.parseMap(configMap);
    ImpalaMetadataTask metadataTask = new ImpalaMetadataTask();
    metadataTask.configure(config);

    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    String query = metadataTask.deriveQuery(dependencies);

    assertEquals("ALTER TABLE testtable DROP IF EXISTS RANGE PARTITION 20190122 <= VALUES < 20190123", query);
  }

  @Test
  public void testGetKerberosKeytabFromSpark() {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set("spark.yarn.keytab", "boom-oo-ya-ta-ta-ta.kt");
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(HOST_CONFIG, "testhost");
    configMap.put(QUERY_TYPE_CONFIG, "refresh");
    configMap.put(QUERY_TABLE_CONFIG, "testtable");
    configMap.put(AUTH_CONFIG, "kerberos");
    Config config = ConfigFactory.parseMap(configMap);

    String keytab = ImpalaMetadataTask.getKerberosKeytab(config, sparkConf);

    assertEquals("boom-oo-ya-ta-ta-ta.kt", keytab);
  }

  @Test
  public void testGetKerberosKeytabFromConfig() {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set("spark.yarn.keytab", "boom-oo-ya-ta-ta-ta.kt");
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(HOST_CONFIG, "testhost");
    configMap.put(QUERY_TYPE_CONFIG, "refresh");
    configMap.put(QUERY_TABLE_CONFIG, "testtable");
    configMap.put(AUTH_CONFIG, "kerberos");
    configMap.put(KEYTAB_CONFIG, "foo.kt");
    Config config = ConfigFactory.parseMap(configMap);

    String keytab = ImpalaMetadataTask.getKerberosKeytab(config, sparkConf);

    assertEquals("foo.kt", keytab);
  }

  @Test
  public void testGetKerberosPrincFromSpark() {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set("spark.yarn.principal", "boom-oo-ya-ta-ta-ta");
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(HOST_CONFIG, "testhost");
    configMap.put(QUERY_TYPE_CONFIG, "refresh");
    configMap.put(QUERY_TABLE_CONFIG, "testtable");
    configMap.put(AUTH_CONFIG, "kerberos");
    Config config = ConfigFactory.parseMap(configMap);

    String principal = ImpalaMetadataTask.getKerberosPrincipal(config, sparkConf);

    assertEquals("boom-oo-ya-ta-ta-ta", principal);
  }

  @Test
  public void testGetKerberosPrincFromConfig() {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set("spark.yarn.principal", "boom-oo-ya-ta-ta-ta");
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(HOST_CONFIG, "testhost");
    configMap.put(QUERY_TYPE_CONFIG, "refresh");
    configMap.put(QUERY_TABLE_CONFIG, "testtable");
    configMap.put(AUTH_CONFIG, "kerberos");
    configMap.put(USER_PRINC_CONFIG, "foo");
    Config config = ConfigFactory.parseMap(configMap);

    String principal = ImpalaMetadataTask.getKerberosPrincipal(config, sparkConf);

    assertEquals("foo", principal);
  }
}
