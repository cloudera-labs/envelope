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

public class SecurityConfig {

  // General Kerberos configurations
  public static final String KERBEROS_PREFIX = "krb";
  public static final String SERVICE_PRINCIPAL_NAME_CONFIG = KERBEROS_PREFIX + ".service-principal";
  public static final String REALM_CONFIG = KERBEROS_PREFIX + ".realm";
  public static final String KEYTAB_CONFIG = KERBEROS_PREFIX + ".keytab";
  public static final String USER_PRINC_CONFIG = KERBEROS_PREFIX + ".user-principal";
  public static final String TICKET_RENEW_INTERVAL = KERBEROS_PREFIX + ".ticket-renew-interval";
  public static final String DEBUG = KERBEROS_PREFIX + ".debug";

}
