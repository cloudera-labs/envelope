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

import com.cloudera.labs.envelope.validate.Validation;
import com.cloudera.labs.envelope.validate.ValidationResult;
import com.cloudera.labs.envelope.validate.Validations;
import com.cloudera.labs.envelope.validate.Validity;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.SparkConf;

import java.util.Set;

import static com.cloudera.labs.envelope.security.SecurityConfig.DEBUG;
import static com.cloudera.labs.envelope.security.SecurityConfig.KEYTAB_CONFIG;
import static com.cloudera.labs.envelope.security.SecurityConfig.REALM_CONFIG;
import static com.cloudera.labs.envelope.security.SecurityConfig.SERVICE_PRINCIPAL_NAME_CONFIG;
import static com.cloudera.labs.envelope.security.SecurityConfig.TICKET_RENEW_INTERVAL;
import static com.cloudera.labs.envelope.security.SecurityConfig.USER_PRINC_CONFIG;

public class KerberosParameterValidations {

  public static class KerberosKeytabValidation implements Validation {

    private static final String USAGE = String.format(
        "If Kerberos authentication is used must supply either %s or use --keytab", KEYTAB_CONFIG);

    @Override
    public ValidationResult validate(Config config) {
      SparkConf conf = new SparkConf();
      if (!config.hasPath(KEYTAB_CONFIG) && !conf.contains("spark.yarn.keytab")) {
        return new ValidationResult(this, Validity.INVALID, USAGE);
      }
      return new ValidationResult(this, Validity.VALID,
          "Kerberos keytab has been supplied");
    }

    @Override
    public Set<String> getKnownPaths() {
      return Sets.newHashSet(KEYTAB_CONFIG);
    }

  }

  public static class KerberosUserPrincipalValidation implements Validation {

    private static final String USAGE = String.format(
        "If Kerberos authentication is used must supply either %s or use --principal", USER_PRINC_CONFIG);

    @Override
    public ValidationResult validate(Config config) {
      SparkConf conf = new SparkConf();
      if (!config.hasPath(USER_PRINC_CONFIG) && !conf.contains("spark.yarn.principal")) {
        return new ValidationResult(this, Validity.INVALID, USAGE);
      }
      return new ValidationResult(this, Validity.VALID,
          "Kerberos principal has been supplied");
    }

    @Override
    public Set<String> getKnownPaths() {
      return Sets.newHashSet(USER_PRINC_CONFIG);
    }

  }

  public static Validations getValidations() {
    return Validations.builder()
        .add(new KerberosKeytabValidation())
        .add(new KerberosUserPrincipalValidation())
        .optionalPath(SERVICE_PRINCIPAL_NAME_CONFIG, ConfigValueType.STRING)
        .optionalPath(TICKET_RENEW_INTERVAL)
        .optionalPath(REALM_CONFIG, ConfigValueType.STRING)
        .optionalPath(DEBUG, ConfigValueType.BOOLEAN)
        .build();
  }

}
