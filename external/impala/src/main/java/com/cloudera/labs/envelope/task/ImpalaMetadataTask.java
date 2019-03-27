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

import com.cloudera.labs.envelope.component.ProvidesAlias;
import com.cloudera.labs.envelope.run.TaskStep;
import com.cloudera.labs.envelope.security.KerberosLoginContext;
import com.cloudera.labs.envelope.security.KerberosParameterValidations;
import com.cloudera.labs.envelope.security.KerberosUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueType;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kerby.kerberos.kerb.KrbException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static com.cloudera.labs.envelope.security.SecurityConfig.KERBEROS_PREFIX;
import static com.cloudera.labs.envelope.security.SecurityConfig.SERVICE_PRINCIPAL_NAME_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.AUTH_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.HOST_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.PASSWORD_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.PORT_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.QUERY_FORMAT_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.QUERY_LOCATION_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.QUERY_PART_RANGE_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.QUERY_PART_RANGE_END_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.QUERY_PART_RANGE_INCLUSIVITY_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.QUERY_PART_RANGE_START_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.QUERY_PART_RANGE_VAL_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.QUERY_PART_SPEC_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.QUERY_TABLE_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.QUERY_TYPE_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.SSL_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.SSL_TRUSTSTORE_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG;
import static com.cloudera.labs.envelope.task.ImpalaMetadataTaskConfig.USERNAME_CONFIG;

public class ImpalaMetadataTask implements Task, ProvidesAlias, ProvidesValidations {

  private static final Logger LOG = LoggerFactory.getLogger(ImpalaMetadataTask.class);
  private static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

  private static final String DEFAULT_RANGE_INCLUSIVITY = "ie";

  private String connectionString;
  private Config config;

  private transient Connection connection = null;
  private transient KerberosLoginContext loginContext = null;

  private enum KuduRangeOperator {
    LT("<"),LTE("<=");
    private String op;
    KuduRangeOperator(String op) {
      this.op = op;
    }
    String op() {
      return op;
    }
  }

  private static boolean isKerberos(Config config) {
    if (config.hasPath(AUTH_CONFIG)) {
      return config.getString(AUTH_CONFIG).equals("kerberos");
    } else {
      return UserGroupInformation.isSecurityEnabled();
    }
  }

  private void renewIfExpired() throws KrbException {
    if (isKerberos(config)) {
      KerberosUtils.loginIfRequired(loginContext, config);
    }
  }

  String buildConnectionString() {
    // Base connection info
    String url = String
        .format("jdbc:hive2://%s:%d/", config.getString(HOST_CONFIG), config.getInt(PORT_CONFIG));

    // Add authentication info
    switch (config.getString(AUTH_CONFIG)) {
      case "kerberos":
        try {
          url += String.format(";principal=%s/%s@%s;auth=kerberos;kerberosAuthType=fromSubject",
              config.getString(SERVICE_PRINCIPAL_NAME_CONFIG), config.getString(HOST_CONFIG),
              KerberosUtils.getKerberosRealm(config));
        } catch (KrbException e) {
          LOG.error("Could not obtain default Kerberos realm: " + e.getMessage());
          throw new RuntimeException(e);
        }
        break;
      case "none":
        // No auth
        url += ";auth=noSasl";
        break;
      case "ldap":
        break;
      default:
        LOG.error("Invalid auth type [{}]. Should be 'kerberos', 'ldap' or 'none'");
        throw new RuntimeException("Invalid auth type [" + config.getString(AUTH_CONFIG) + "]");
    }

    // Add wire encryption info
    if (config.getBoolean(SSL_CONFIG)) {
      url += ";ssl=true";
      if (config.hasPath(SSL_TRUSTSTORE_CONFIG)) {
        url += String.format(";sslTrustStore=%s", config.getString(SSL_TRUSTSTORE_CONFIG));
      }
      if (config.hasPath(SSL_TRUSTSTORE_PASSWORD_CONFIG)) {
        url += String
            .format(";trustStorePassword=%s", config.getString(SSL_TRUSTSTORE_PASSWORD_CONFIG));
      }
    }

    LOG.debug("Built connection string: " + url);

    return url;
  }

  private void loadDriver() throws ClassNotFoundException {
    Class.forName(DRIVER_NAME);
  }

  private Connection getConnection() {
    if (connection == null) {
      try {
        loadDriver();
        if (config.getString(AUTH_CONFIG).equals("ldap")) {
          connection = DriverManager.getConnection(connectionString,
              config.getString(USERNAME_CONFIG), config.getString(PASSWORD_CONFIG));
        } else {
          connection = DriverManager.getConnection(connectionString);
        }
      } catch (ClassNotFoundException e) {
        LOG.error("Could not load Impala driver, ensure the ImpalaJDBC41.jar is on the classpath");
        throw new RuntimeException(e);
      } catch (SQLException e) {
        LOG.error("Could not open connection to Impala: " + e);
        throw new RuntimeException(e);
      }
    }
    return connection;
  }

  @Override
  public String getAlias() {
    return "impala_ddl";
  }

  @Override
  public void configure(Config config) {
    // Merge with defaults
    this.config = config.withFallback(ConfigFactory.parseString(generateDefaultConfig()));

    // Initialize kerberos if required
    if (isKerberos(config)) {
      try {
        loginContext = KerberosUtils.createKerberosLoginContext("envelope-impala-context", config);
      } catch (LoginException e) {
        throw new RuntimeException("Problem creating Kerberos login context: " + e.getMessage());
      }
    }

    // Build JDBC connection string
    connectionString = buildConnectionString();
  }

  private String generateDefaultConfig() {
    return String.format("%s = %s\n", AUTH_CONFIG, UserGroupInformation.isSecurityEnabled() ? "kerberos" : "none") +
        String.format("%s = false\n", SSL_CONFIG) +
        String.format("%s = 21050\n", PORT_CONFIG) +
        String.format("%s = impala\n", SERVICE_PRINCIPAL_NAME_CONFIG);
  }

  private void executeQuery(String query) throws SQLException {
    Connection conn = getConnection();
    Statement stmt = conn.createStatement();
    try {
      stmt.execute(query);
    } finally {
      if (!stmt.isClosed()) {
        stmt.close();
      }
    }
  }

  private boolean shouldRun() {
    // TODO Should run this time? Options:
    // 1. Timer/Rate limiter
    // 2. Every-N
    // 3. Partition tracker--look for new
    return true;
  }

  String deriveQuery(Map<String, Dataset<Row>> dependencies) {
    String query;
    switch (config.getString(QUERY_TYPE_CONFIG)) {
      case "refresh":
        query = String.format("REFRESH %s", config.getString(QUERY_TABLE_CONFIG));
        break;
      case "invalidate":
        query = String.format("INVALIDATE METADATA %s", config.getString(QUERY_TABLE_CONFIG));
        break;
      case "add_partition":
        // TODO support dynamic partitions
        query = String.format("ALTER TABLE %s ADD IF NOT EXISTS %s",
            config.getString(QUERY_TABLE_CONFIG), buildPartitionSpec(config, dependencies));
        break;
      case "drop_partition":
        // TODO support dynamic partitions
        query = String.format("ALTER TABLE %s DROP IF EXISTS %s",
            config.getString(QUERY_TABLE_CONFIG), buildPartitionSpec(config, dependencies));
        break;
      // TODO add create support
      default:
        throw new RuntimeException("Unrecognized query type: " + config.getString(QUERY_TYPE_CONFIG));
    }

    return query;
  }

  private static String buildPartitionSpec(Config config, Map<String, Dataset<Row>> dependencies) {
    String query;
    // Is this a straight partition spec or a range partition?
    if (config.hasPath(QUERY_PART_SPEC_CONFIG)) {
      // TODO support dynamic partitions
      query = String.format("PARTITION (%s)", config.getString(QUERY_PART_SPEC_CONFIG));
      if (config.hasPath(QUERY_LOCATION_CONFIG)) {
        query += String.format(" LOCATION '%s'", config.getString(QUERY_LOCATION_CONFIG));
      }
    } else {
      // Must be a range partition
      if (config.hasPath(QUERY_PART_RANGE_VAL_CONFIG)){
        query = String.format("RANGE PARTITION VALUE = %s", config.getString(QUERY_PART_RANGE_VAL_CONFIG));
      } else {
        // Must be a boundaried range partition
        if (Long.parseLong(config.getString(QUERY_PART_RANGE_END_CONFIG)) <
            Long.parseLong(config.getString(QUERY_PART_RANGE_START_CONFIG))) {
          throw new RuntimeException("Upper partition range boundary must be strictly greater than lower range boundary");
        }
        query = String.format("RANGE PARTITION %s %s VALUES %s %s", config.getString(QUERY_PART_RANGE_START_CONFIG),
            getInclusivityOperator(config, "lower"), getInclusivityOperator(config, "upper"),
            config.getString(QUERY_PART_RANGE_END_CONFIG));
      }
    }
    return query;
  }

  private static String getInclusivityOperator(Config config, String bound) {
    String inclusivity = config.hasPath(QUERY_PART_RANGE_INCLUSIVITY_CONFIG) ?
        config.getString(QUERY_PART_RANGE_INCLUSIVITY_CONFIG) : DEFAULT_RANGE_INCLUSIVITY;
    String inclusivityVal = bound.equals("lower") ? inclusivity.substring(0,1) : inclusivity.substring(1,2);
    if (inclusivityVal.equals("i")) {
      return KuduRangeOperator.LTE.op();
    } else {
      return KuduRangeOperator.LT.op();
    }
  }

  @Override
  public void run(Map<String, Dataset<Row>> dependencies) {
    if (shouldRun()) {
      final String thisQuery = deriveQuery(dependencies);
      LOG.info("Executing query: {}", thisQuery);
      try {
        if (isKerberos(config)) {
          renewIfExpired();
          Subject.doAs(loginContext.getSubject(), new PrivilegedExceptionAction<Void>() {
            public Void run() throws SQLException {
              executeQuery(thisQuery);
              return null;
            }
          });
        } else {
          executeQuery(thisQuery);
        }
      } catch (PrivilegedActionException e) {
        Exception inner = e.getException();
        inner.printStackTrace();
        throw new RuntimeException("Could not execute metadata task: " + inner.getClass().getCanonicalName() +
            " " + inner.getMessage());
      } catch (SQLException|KrbException e) {
        e.printStackTrace();
        throw new RuntimeException("Could not execute metadata task: " + e.getClass().getCanonicalName() +
            " " + e.getMessage());
      }
    }
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        // Query
        .mandatoryPath(QUERY_TYPE_CONFIG, ConfigValueType.STRING)
        .allowedValues(QUERY_TYPE_CONFIG, "refresh", "invalidate", "add_partition", "drop_partition")
        .mandatoryPath(QUERY_TABLE_CONFIG, ConfigValueType.STRING)
        .optionalPath(QUERY_LOCATION_CONFIG, ConfigValueType.STRING)
        .optionalPath(QUERY_FORMAT_CONFIG, ConfigValueType.STRING)
        .allowedValues(QUERY_FORMAT_CONFIG, "parquet", "text", "kudu")
        // Partition specs
        .ifPathHasValue(QUERY_TYPE_CONFIG, "add_partition", Validations.single().exactlyOnePathExists(
            QUERY_PART_SPEC_CONFIG, QUERY_PART_RANGE_CONFIG))
        .ifPathHasValue(QUERY_TYPE_CONFIG, "add_partition", Validations.single().atMostOnePathExists(
            QUERY_PART_SPEC_CONFIG, QUERY_PART_RANGE_CONFIG
        ))
        .ifPathHasValue(QUERY_TYPE_CONFIG, "drop_partition", Validations.single().atMostOnePathExists(
            QUERY_PART_SPEC_CONFIG, QUERY_PART_RANGE_CONFIG
        ))
        .optionalPath(QUERY_PART_SPEC_CONFIG, ConfigValueType.STRING)
        .optionalPath(QUERY_PART_RANGE_CONFIG, ConfigValueType.OBJECT)
        .ifPathExists(QUERY_PART_RANGE_CONFIG, Validations.single().atMostOnePathExists(
            QUERY_PART_RANGE_VAL_CONFIG, QUERY_PART_RANGE_START_CONFIG
        ))
        .ifPathExists(QUERY_PART_RANGE_CONFIG, Validations.single().atMostOnePathExists(
            QUERY_PART_RANGE_VAL_CONFIG, QUERY_PART_RANGE_END_CONFIG
        ))
        .ifPathExists(QUERY_PART_RANGE_START_CONFIG, Validations.single().mandatoryPath(QUERY_PART_RANGE_END_CONFIG))
        .optionalPath(QUERY_PART_RANGE_START_CONFIG)
        .optionalPath(QUERY_PART_RANGE_END_CONFIG)
        .optionalPath(QUERY_PART_RANGE_VAL_CONFIG)
        .ifPathExists(QUERY_PART_RANGE_CONFIG, Validations.single().optionalPath(QUERY_PART_RANGE_INCLUSIVITY_CONFIG))
        .allowedValues(QUERY_PART_RANGE_INCLUSIVITY_CONFIG, "ie", "ee", "ei", "ii")
        // Connection specs
        .mandatoryPath(HOST_CONFIG, ConfigValueType.STRING)
        .optionalPath(AUTH_CONFIG, ConfigValueType.STRING)
        .allowedValues(AUTH_CONFIG, "kerberos", "ldap", "none")
        .optionalPath(PORT_CONFIG, ConfigValueType.NUMBER)
        .ifPathExists(KERBEROS_PREFIX, KerberosParameterValidations.getValidations())
        .ifPathHasValue(AUTH_CONFIG, "kerberos", KerberosParameterValidations.getValidations())
        .ifPathHasValue(AUTH_CONFIG, "ldap", Validations.single()
            .mandatoryPath(USERNAME_CONFIG, ConfigValueType.STRING))
        .ifPathHasValue(AUTH_CONFIG, "ldap", Validations.single()
            .mandatoryPath(PASSWORD_CONFIG, ConfigValueType.STRING))
        .optionalPath(SSL_CONFIG, ConfigValueType.BOOLEAN)
        .ifPathExists(SSL_CONFIG, Validations.single()
            .optionalPath(SSL_TRUSTSTORE_CONFIG))
        .ifPathExists(SSL_CONFIG, Validations.single()
            .optionalPath(SSL_TRUSTSTORE_PASSWORD_CONFIG))
        // TODO remove below after ENV-353 is fixed
        .optionalPath(TaskStep.DEPENDENCIES_CONFIG)
        .optionalPath(TaskStep.CLASS_CONFIG)
        // TODO remove above after ENV-353 is fixed
        .build();
  }
}
