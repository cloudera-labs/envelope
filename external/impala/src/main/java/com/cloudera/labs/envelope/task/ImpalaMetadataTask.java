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

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.run.TaskStep;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validation;
import com.cloudera.labs.envelope.validate.ValidationResult;
import com.cloudera.labs.envelope.validate.Validations;
import com.cloudera.labs.envelope.validate.Validity;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueType;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kerby.kerberos.kerb.KrbException;
import org.apache.kerby.kerberos.kerb.client.ClientUtil;
import org.apache.kerby.kerberos.kerb.client.KrbConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.File;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ImpalaMetadataTask implements Task, ProvidesAlias, ProvidesValidations {

  public static final String HOST_CONFIG = "host";
  public static final String PORT_CONFIG = "port";
  public static final String SERVICE_PRINCIPAL_NAME_CONFIG = "krb-service-principal";
  public static final String REALM_CONFIG = "krb-realm";
  public static final String KEYTAB_CONFIG = "krb-keytab";
  public static final String USER_PRINC_CONFIG = "krb-user-principal";
  public static final String TICKET_RENEW_INTERVAL = "krb-ticket-renew-interval";
  public static final String DEBUG = "debug";
  public static final String QUERY_TYPE_CONFIG = "query.type";
  public static final String QUERY_TABLE_CONFIG = "query.table";
  public static final String QUERY_PART_SPEC_CONFIG = "query.partition.spec";
  public static final String QUERY_LOCATION_CONFIG = "query.partition.location";
  public static final String QUERY_PART_RANGE_CONFIG = "query.partition.range";
  public static final String QUERY_PART_RANGE_VAL_CONFIG = QUERY_PART_RANGE_CONFIG + ".value";
  public static final String QUERY_PART_RANGE_START_CONFIG = QUERY_PART_RANGE_CONFIG + ".start";
  public static final String QUERY_PART_RANGE_END_CONFIG = QUERY_PART_RANGE_CONFIG + ".end";
  public static final String QUERY_PART_RANGE_INCLUSIVITY_CONFIG = QUERY_PART_RANGE_CONFIG + ".inclusivity";
  public static final String QUERY_FORMAT_CONFIG = "query.format";
  public static final String AUTH_CONFIG = "auth";
  public static final String SSL_CONFIG = "ssl";
  public static final String SSL_TRUSTSTORE_CONFIG = "ssl-truststore";
  public static final String SSL_TRUSTSTORE_PASSWORD_CONFIG = "ssl-truststore-password";
  public static final String USERNAME_CONFIG = "username";
  public static final String PASSWORD_CONFIG = "password";

  private static final Set<String> KNOWN_PATHS = new HashSet<>();
  static {
    KNOWN_PATHS.add(KEYTAB_CONFIG);
    KNOWN_PATHS.add(USER_PRINC_CONFIG);
  }

  private static final Logger LOG = LoggerFactory.getLogger(ImpalaMetadataTask.class);
  private static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

  private static final Pattern LIFETIME_DHMS = Pattern
      .compile("\\s*(\\d+d)?\\s*(\\d+h)?\\s*(\\d+m)?\\s*(\\d+s)?\\s*");
  private static final Pattern LIFETIME_HMS = Pattern.compile("\\s*(\\d+):(\\d+)(:(\\d+))?\\s*");
  private static final Pattern LIFETIME_SECS = Pattern.compile("\\s*(\\d+)\\s*");
  private static final int DEFAULT_LIFETIME_SECS = 86400;
  private static final boolean DEFAULT_DEBUG = false;
  private static final String DEFAULT_RANGE_INCLUSIVITY = "ie";

  private long lastLoginTime = 0;
  private String connectionString;
  private Config config;
  private SparkConf sparkConf;

  private transient KrbConfig krb5config = null;
  private transient Connection connection = null;
  private transient Subject subject = null;
  private transient LoginContext loginContext = null;

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

  private static class KerberosConfiguration extends javax.security.auth.login.Configuration {

    private Map<String, String> optionsMap = new HashMap<>();

    KerberosConfiguration(String keytab, String principal, boolean debug) {
      optionsMap.put("doNotPrompt", "true");
      optionsMap.put("useKeyTab", "true");
      optionsMap.put("storeKey", "true");
      optionsMap.put("refreshKrb5Config", "true");
      optionsMap.put("keyTab", keytab);
      optionsMap.put("principal", principal);
      optionsMap.put("debug", Boolean.toString(debug).toLowerCase());
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      return new AppConfigurationEntry[]{
          new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
              AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
              optionsMap)
      };
    }

  }

  private void createKerberosLoginContext() throws LoginException {
    if (isKerberos(config)) {
      subject = new Subject();
      loginContext = new LoginContext("envelope-impala-context", subject, null,
          new KerberosConfiguration(
              getKerberosKeytab(config, sparkConf),
              getKerberosPrincipal(config, sparkConf),
              isDebug(config)));

      LOG.info("Created Kerberos login context for {} using keytab: {}",
          getKerberosPrincipal(config, sparkConf),
          getKerberosKeytab(config, sparkConf));
    } else {
      LOG.info("No special login context required");
    }
  }

  void login() {
    if (isKerberos(config)) {
      try {
        loginContext.login();
        lastLoginTime = System.currentTimeMillis();
      } catch (FailedLoginException e) {
        LOG.error("Authentication failed: {}", e.getMessage());
        String errorMsg = String
            .format("Could not authenticate using keytab %s and principal %s: %s",
                getKerberosKeytab(config, sparkConf), getKerberosPrincipal(config, sparkConf),
                e.getMessage());
        throw new RuntimeException(errorMsg);
      } catch (LoginException e) {
        LOG.error("Login attempt failed: {}", e.getMessage());
        String errorMsg = String.format("Problem running login: %s", e.getMessage());
        throw new RuntimeException(errorMsg);
      }
    }
  }

  static String getKerberosKeytab(Config config, SparkConf sparkConf) {
    if (config.hasPath(KEYTAB_CONFIG)) return config.getString(KEYTAB_CONFIG);
    else return sparkConf.get("spark.yarn.keytab");
  }

  static String getKerberosPrincipal(Config config, SparkConf sparkConf) {
    if (config.hasPath(USER_PRINC_CONFIG)) return config.getString(USER_PRINC_CONFIG);
    else return sparkConf.get("spark.yarn.principal");
  }

  private static boolean isKerberos(Config config) {
    return config.getString(AUTH_CONFIG).equals("kerberos");
  }

  private static boolean isDebug(Config config) {
    if (config.hasPath(DEBUG)) {
      return config.getBoolean(DEBUG);
    } else {
      return DEFAULT_DEBUG;
    }
  }

  private void renewIfExpired() throws KrbException {
    if (isKerberos(config)) {
      if ((System.currentTimeMillis() - lastLoginTime) > getRenewInterval()) {
        LOG.debug("Kerberos ticket is due for renewal, authenticating again");
        login();
      }
    }
  }

  KrbConfig getKrb5config() throws KrbException {
    if (krb5config == null) {
      // Use the same logic as the standard Java Kerberos classes to load the krb5.conf
      // configuration file (https://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/KerberosReq.html):
      //
      //   1. If the system property java.security.krb5.conf is set, its value is assumed to
      //      specify the path and file name.
      //   2. <java-home>/lib/security/krb5.conf
      //   3. /etc/krb5.conf
      if (System.getProperty("java.security.krb5.conf") != null &&
          fileExists(System.getProperty("java.security.krb5.conf"))) {
        krb5config = ClientUtil.getConfig(new File(System.getProperty("java.security.krb5.conf")));
      } else if (fileExists(System.getProperty("java.home") + File.separator + "lib" +
          File.separator + "security" + File.separator + "krb5.conf")) {
        krb5config = ClientUtil.getConfig(new File(System.getProperty("java.home") +
            File.separator + "lib" + File.separator + "security" + File.separator + "krb5.conf"));
      } else if (fileExists("/etc/krb5.conf")) {
        krb5config = ClientUtil.getConfig(new File("/etc/krb5.conf"));
      } else {
        throw new RuntimeException("Could not find a valid /etc/krb5.conf file");
      }
    }
    return krb5config;
  }

  private boolean fileExists(String file) {
    File theFile = new File(file);
    return theFile.exists();
  }

  int getRenewInterval() throws KrbException {
    String configuredLifetime = getKrb5config().getTicketLifetime();
    if (config.hasPath(TICKET_RENEW_INTERVAL)) {
      return new Long(config.getDuration(TICKET_RENEW_INTERVAL, TimeUnit.SECONDS)).intValue();
    } else if (configuredLifetime != null) {
      return parseLifetime(configuredLifetime);
    } else {
      return DEFAULT_LIFETIME_SECS;
    }
  }

  static int parseLifetime(String lifetime) {
    // Check possible patterns in turn (see https://web.mit.edu/kerberos/krb5-1.13/doc/basic/date_format.html#duration)
    Matcher dhmsMatcher = LIFETIME_DHMS.matcher(lifetime);
    Matcher hmsMatcher = LIFETIME_HMS.matcher(lifetime);
    Matcher secondsMatcher = LIFETIME_SECS.matcher(lifetime);
    if (dhmsMatcher.matches()) {
      int days = 0, hours = 0, minutes = 0, seconds = 0;
      for (int i = 1; i <= dhmsMatcher.groupCount(); i++) {
        String match = dhmsMatcher.group(i);
        if (match != null) {
          int value = Integer.parseInt(match.substring(0, match.length() - 1));
          switch (match.charAt(match.length() - 1)) {
            case 'd':
              days = value;
              break;
            case 'h':
              hours = value;
              break;
            case 'm':
              minutes = value;
              break;
            case 's':
              seconds = value;
              break;
          }
        }
      }
      return (86400 * days) + (3600 * hours) + (60 * minutes) + seconds;
    } else if (hmsMatcher.matches()) {
      int hours = Integer.parseInt(hmsMatcher.group(1));
      int minutes = Integer.parseInt(hmsMatcher.group(2));
      int seconds = hmsMatcher.group(3) == null ? 0 : Integer.parseInt(hmsMatcher.group(4));
      return (3600 * hours) + (60 * minutes) + seconds;
    } else if (secondsMatcher.matches()) {
      return Integer.parseInt(secondsMatcher.group(1));
    } else {
      LOG.warn(
          "Could not determine ticket lifetime from [{}]. Falling back to default [{} seconds]",
          lifetime, DEFAULT_LIFETIME_SECS);
      return DEFAULT_LIFETIME_SECS;
    }
  }

  String getKerberosRealm(Config config) throws KrbException {
    if (config.hasPath(REALM_CONFIG)) {
      return config.getString(REALM_CONFIG);
    }

    // Infer realm
    return getKrb5config().getDefaultRealm();
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
              config.getString(SERVICE_PRINCIPAL_NAME_CONFIG), config.getString(HOST_CONFIG), getKerberosRealm(config));
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

    // Get a copy of the sparkconf
    this.sparkConf = new SparkConf();

    // Initialize kerberos if required
    try {
      createKerberosLoginContext();
    } catch (LoginException e) {
      throw new RuntimeException("Problem creating Kerberos login context: " + e.getMessage());
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
          Subject.doAs(subject, new PrivilegedExceptionAction<Void>() {
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

  public static class KerberosParameterValidation implements Validation {

    private static final String USAGE = String.format(
        "If Kerberos authentication is used must supply either %s or use --keytab and %s or use --principal",
        KEYTAB_CONFIG, USER_PRINC_CONFIG
        );

    @Override
    public ValidationResult validate(Config config) {
      if ((config.hasPath(AUTH_CONFIG) && config.getString(AUTH_CONFIG).equals("kerberos")) ||
          (!config.hasPath(AUTH_CONFIG) && UserGroupInformation.isSecurityEnabled())) {
        SparkConf conf = new SparkConf();
        if (!config.hasPath(KEYTAB_CONFIG) && !conf.contains("spark.yarn.keytab")) {
          return new ValidationResult(this, Validity.INVALID, USAGE);
        }
        if (!config.hasPath(USER_PRINC_CONFIG) && !conf.contains("spark.yarn.principal")) {
          return new ValidationResult(this, Validity.INVALID, USAGE);
        }
        return new ValidationResult(this, Validity.VALID,
            "Both Kerberos keytab and principal have been supplied");
      } else {
        return new ValidationResult(this, Validity.VALID,
            "Kerberos authentication not being used");
      }
    }

    @Override
    public Set<String> getKnownPaths() {
      return KNOWN_PATHS;
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
        .add(new KerberosParameterValidation())
        .ifPathHasValue(AUTH_CONFIG, "kerberos", Validations.single()
            .optionalPath(SERVICE_PRINCIPAL_NAME_CONFIG, ConfigValueType.STRING))
        .ifPathHasValue(AUTH_CONFIG, "kerberos", Validations.single()
            .optionalPath(REALM_CONFIG, ConfigValueType.STRING))
        .ifPathHasValue(AUTH_CONFIG, "kerberos", Validations.single()
            .optionalPath(TICKET_RENEW_INTERVAL))
        .ifPathHasValue(AUTH_CONFIG, "ldap", Validations.single()
            .mandatoryPath(USERNAME_CONFIG, ConfigValueType.STRING))
        .ifPathHasValue(AUTH_CONFIG, "ldap", Validations.single()
            .mandatoryPath(PASSWORD_CONFIG, ConfigValueType.STRING))
        .optionalPath(SSL_CONFIG, ConfigValueType.BOOLEAN)
        .ifPathExists(SSL_CONFIG, Validations.single()
            .optionalPath(SSL_TRUSTSTORE_CONFIG))
        .ifPathExists(SSL_CONFIG, Validations.single()
            .optionalPath(SSL_TRUSTSTORE_PASSWORD_CONFIG))
        .optionalPath(DEBUG, ConfigValueType.BOOLEAN)
        // TODO remove below after ENV-353 is fixed
        .optionalPath(TaskStep.DEPENDENCIES_CONFIG)
        .optionalPath(TaskFactory.CLASS_CONFIG_NAME)
        // TODO remove above after ENV-353 is fixed
        .build();
  }
}
