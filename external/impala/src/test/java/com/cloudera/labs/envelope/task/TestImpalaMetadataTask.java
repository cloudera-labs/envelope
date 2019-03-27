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
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.AfterClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.cloudera.labs.envelope.security.SecurityConfig.KEYTAB_CONFIG;
import static com.cloudera.labs.envelope.security.SecurityConfig.USER_PRINC_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.AUTH_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.HOST_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.PASSWORD_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.PORT_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.QUERY_LOCATION_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.QUERY_PART_RANGE_END_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.QUERY_PART_RANGE_START_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.QUERY_PART_RANGE_VAL_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.QUERY_PART_SPEC_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.QUERY_TABLE_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.QUERY_TYPE_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.SSL_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.SSL_TRUSTSTORE_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.USERNAME_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestImpalaMetadataTask {

  private static String oldKrb5 = System.getProperty("java.security.krb5.conf", null);

  @AfterClass
  public static void cleanup() {
    if (oldKrb5 != null) {
      System.setProperty("java.security.krb5.conf", oldKrb5);
    } else {
      System.clearProperty("java.security.krb5.conf");
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
    System.setProperty("java.security.krb5.conf", ClassLoader.getSystemResource("krb5.conf").getPath());

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
    System.setProperty("java.security.krb5.conf", ClassLoader.getSystemResource("krb5.conf").getPath());

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
    System.setProperty("java.security.krb5.conf", ClassLoader.getSystemResource("krb5.conf").getPath());

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
    System.setProperty("java.security.krb5.conf", ClassLoader.getSystemResource("krb5.conf").getPath());

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
    for (int i = 1; i <= 7; i++) {
      Config config = ConfigUtils.configFromResource(String.format("/valid-%d.conf", i));
      ImpalaMetadataTask task = new ImpalaMetadataTask();
      List<ValidationResult> validations = Validator.validate(task, config);
      if (ValidationUtils.hasValidationFailures(validations)) {
        ValidationUtils.logValidationResults(validations);
        fail(String.format("File valid-%d.conf should be valid", i));
      }
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


}
