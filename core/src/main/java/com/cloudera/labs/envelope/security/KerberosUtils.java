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

import com.typesafe.config.Config;
import org.apache.kerby.kerberos.kerb.KrbException;
import org.apache.kerby.kerberos.kerb.client.ClientUtil;
import org.apache.kerby.kerberos.kerb.client.KrbConfig;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.cloudera.labs.envelope.security.SecurityConfig.DEBUG;
import static com.cloudera.labs.envelope.security.SecurityConfig.KEYTAB_CONFIG;
import static com.cloudera.labs.envelope.security.SecurityConfig.REALM_CONFIG;
import static com.cloudera.labs.envelope.security.SecurityConfig.TICKET_RENEW_INTERVAL;
import static com.cloudera.labs.envelope.security.SecurityConfig.USER_PRINC_CONFIG;

public class KerberosUtils {

  private static final Logger LOG = LoggerFactory.getLogger(KerberosUtils.class);
  private static final int DEFAULT_LIFETIME_SECS = 86400;
  private static final boolean DEFAULT_DEBUG = false;
  private static final Pattern LIFETIME_DHMS = Pattern.compile("\\s*(\\d+d)?\\s*(\\d+h)?\\s*(\\d+m)?\\s*(\\d+s)?\\s*");
  private static final Pattern LIFETIME_HMS = Pattern.compile("\\s*(\\d+):(\\d+)(:(\\d+))?\\s*");
  private static final Pattern LIFETIME_SECS = Pattern.compile("\\s*(\\d+)\\s*");

  private static KrbConfig krb5Config;
  private static SparkConf sparkConf;

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

  public static KerberosLoginContext createKerberosLoginContext(String contextName, Config config) throws LoginException {
    Subject subject = new Subject();
    LoginContext loginContext = new LoginContext(contextName, subject, null,
        new KerberosConfiguration(
            getKerberosKeytab(config),
            getKerberosPrincipal(config),
            isKerberosDebug(config)));

    LOG.info("Created Kerberos login context for {} using keytab: {}",
        getKerberosPrincipal(config),
        getKerberosKeytab(config));

    return new KerberosLoginContext(loginContext);
  }

  private static boolean isKerberosDebug(Config config) {
    if (config.hasPath(DEBUG)) {
      return config.getBoolean(DEBUG);
    } else {
      return DEFAULT_DEBUG;
    }
  }

  public static boolean shouldLoginFromKeytab(Config config) {
    return config.hasPath(KEYTAB_CONFIG) || getSparkConf().contains("spark.yarn.keytab");
  }

  static String getKerberosKeytab(Config config) {
    if (config.hasPath(KEYTAB_CONFIG)) return config.getString(KEYTAB_CONFIG);
    else return getSparkConf().get("spark.yarn.keytab");
  }

  static String getKerberosPrincipal(Config config) {
    if (config.hasPath(USER_PRINC_CONFIG)) return config.getString(USER_PRINC_CONFIG);
    else return getSparkConf().get("spark.yarn.principal");
  }

  public static String getKerberosRealm(Config config) {
    if (config.hasPath(REALM_CONFIG)) {
      return config.getString(REALM_CONFIG);
    }

    // Infer realm
    String realm;
    try {
      realm = getKrb5config().getDefaultRealm();
    }
    catch (KrbException e) {
      throw new RuntimeException(e);
    }

    return realm;
  }

  private synchronized static SparkConf getSparkConf() {
    if (sparkConf == null) {
      sparkConf = new SparkConf();
    }
    return sparkConf;
  }

  // For testing
  static void setSparkConf(SparkConf sparkConfig) {
    sparkConf = sparkConfig;
  }

  static KrbConfig getKrb5config() throws KrbException {
    if (krb5Config == null) {
      // Use the same logic as the standard Java Kerberos classes to load the krb5.conf
      // configuration file (https://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/KerberosReq.html):
      //
      //   1. If the system property java.security.krb5.conf is set, its value is assumed to
      //      specify the path and file name.
      //   2. <java-home>/lib/security/krb5.conf
      //   3. /etc/krb5.conf
      if (System.getProperty("java.security.krb5.conf") != null &&
          fileExists(System.getProperty("java.security.krb5.conf"))) {
        krb5Config = ClientUtil.getConfig(new File(System.getProperty("java.security.krb5.conf")));
      } else if (fileExists(System.getProperty("java.home") + File.separator + "lib" +
          File.separator + "security" + File.separator + "krb5.conf")) {
        krb5Config = ClientUtil.getConfig(new File(System.getProperty("java.home") +
            File.separator + "lib" + File.separator + "security" + File.separator + "krb5.conf"));
      } else if (fileExists("/etc/krb5.conf")) {
        krb5Config = ClientUtil.getConfig(new File("/etc/krb5.conf"));
      } else {
        throw new RuntimeException("Could not find a valid /etc/krb5.conf file");
      }
    }
    return krb5Config;
  }

  static int getRenewInterval(Config config) throws KrbException {
    String configuredLifetime = getKrb5config().getTicketLifetime();
    if (config.hasPath(TICKET_RENEW_INTERVAL)) {
      return new Long(config.getDuration(TICKET_RENEW_INTERVAL, TimeUnit.SECONDS)).intValue();
    } else if (configuredLifetime != null) {
      return parseLifetime(configuredLifetime);
    } else {
      return DEFAULT_LIFETIME_SECS;
    }
  }

  private static boolean fileExists(String file) {
    File theFile = new File(file);
    return theFile.exists();
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

  public static void loginIfRequired(KerberosLoginContext loginContext, Config config) {
    try {
      if ((System.currentTimeMillis() - loginContext.getLastLoggedIn()) > getRenewInterval(config) * 1000) {
        LOG.debug("Kerberos ticket is due for renewal, authenticating again");
        loginContext.getLoginContext().login();
        loginContext.recordLoginTime();
      }
    } catch (FailedLoginException e) {
      LOG.error("Authentication failed: {}", e.getMessage());
      String errorMsg = String
          .format("Could not authenticate using keytab %s and principal %s: %s",
              getKerberosKeytab(config), getKerberosPrincipal(config),
              e.getMessage());
      throw new RuntimeException(errorMsg);
    } catch (LoginException e) {
      LOG.error("Login attempt failed: {}", e.getMessage());
      String errorMsg = String.format("Problem running login: %s", e.getMessage());
      throw new RuntimeException(errorMsg);
    } catch (KrbException e) {
      throw new RuntimeException(e);
    }
  }

}
