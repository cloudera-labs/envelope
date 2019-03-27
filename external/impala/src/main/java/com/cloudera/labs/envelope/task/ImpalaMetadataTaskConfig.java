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

public class ImpalaMetadataTaskConfig {

  public static final String HOST_CONFIG = "host";
  public static final String PORT_CONFIG = "port";
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

}
