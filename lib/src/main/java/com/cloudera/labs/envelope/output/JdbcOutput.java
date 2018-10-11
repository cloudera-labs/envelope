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

package com.cloudera.labs.envelope.output;

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.List;
import java.util.Properties;
import java.util.Set;

public class JdbcOutput implements BulkOutput, ProvidesAlias, ProvidesValidations {

  public static final String JDBC_CONFIG_URL = "url";
  public static final String JDBC_CONFIG_TABLENAME = "tablename";
  public static final String JDBC_CONFIG_USERNAME = "username";
  public static final String JDBC_CONFIG_PASSWORD = "password";

  private String url;
  private String tableName;
  private String username;
  private String password;

  @Override
  public void configure(Config config) {
    url = config.getString(JDBC_CONFIG_URL);
    tableName = config.getString(JDBC_CONFIG_TABLENAME);
    username = config.getString(JDBC_CONFIG_USERNAME);
    password = config.getString(JDBC_CONFIG_PASSWORD);
  }

  @Override
  public Set<MutationType> getSupportedBulkMutationTypes() {
    return Sets.newHashSet(MutationType.INSERT);
  }

  @Override
  public void applyBulkMutations(List<Tuple2<MutationType, Dataset<Row>>> planned) {
    Properties properties = new Properties();
    properties.put("user",username);
    properties.put("password",password);

    for (Tuple2<MutationType, Dataset<Row>> plan : planned) {
      MutationType mutationType = plan._1();
      Dataset<Row> mutation = plan._2();
      switch (mutationType) {
        case INSERT:
          mutation.write().jdbc(url, tableName, properties);
          break;
        default:
          throw new RuntimeException("JDBC output does not support mutation type: " + mutationType);
      }
    }
  }

  @Override
  public String getAlias() {
    return "jdbc";
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(JDBC_CONFIG_URL, ConfigValueType.STRING)
        .mandatoryPath(JDBC_CONFIG_TABLENAME, ConfigValueType.STRING)
        .mandatoryPath(JDBC_CONFIG_USERNAME, ConfigValueType.STRING)
        .mandatoryPath(JDBC_CONFIG_PASSWORD, ConfigValueType.STRING)
        .allowEmptyValue(JDBC_CONFIG_PASSWORD)
        .build();
  }
  
}
