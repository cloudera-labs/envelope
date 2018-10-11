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

import com.typesafe.config.Config;
import org.apache.hadoop.security.UserGroupInformation;

class KuduUtils {

  public static final String INSERT_IGNORE_CONFIG_NAME = "insert.ignore";
  public static final String IGNORE_MISSING_COLUMNS_CONFIG_NAME = "ignore.missing.columns";
  public static final String IS_SECURE_CONFIG_NAME = "secure";
  public static final String CREDENTIAL_ALIAS_PREFIX = "envelope.kudu.";

  /**
   * Returns whether Kudu is secured by Kerberos
   * @param config Envelope configuration
   * @return true if Kudu is secure
   */
  static boolean isSecure(Config config) {
    boolean isSecure = UserGroupInformation.isSecurityEnabled();
    if (config.hasPath(IS_SECURE_CONFIG_NAME)) {
      isSecure = config.getBoolean(IS_SECURE_CONFIG_NAME);
    }
    return isSecure;
  }

  /**
   * Returns whether or not we should ignore duplicate rows
   */
  static boolean doesInsertIgnoreDuplicates(Config config) {
    return config.hasPath(INSERT_IGNORE_CONFIG_NAME) && config.getBoolean(INSERT_IGNORE_CONFIG_NAME);
  }

  /**
   * Returns whether or not we should ignore missing columns when writing to Kudu
   */
  static boolean ignoreMissingColumns(Config config) {
    return config.hasPath(IGNORE_MISSING_COLUMNS_CONFIG_NAME) && config.getBoolean(IGNORE_MISSING_COLUMNS_CONFIG_NAME);
  }

  /**
   * Builds a unique alias for the supplied Kudu masters. The same Kudu masters will produce the
   * same alias.
   * @param kuduMasterAddresses a comma-separated master:port string
   * @return an Kudu instance alias
   */
  static String buildCredentialAlias(String kuduMasterAddresses) {
    return CREDENTIAL_ALIAS_PREFIX + kuduMasterAddresses;
  }

}
