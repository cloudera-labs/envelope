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

package com.cloudera.labs.envelope.input;

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class HiveInput implements BatchInput, ProvidesAlias, ProvidesValidations {

  public static final String TABLE_CONFIG_NAME = "table";

  private String tableName;

  @Override
  public void configure(Config config) {
    this.tableName = config.getString(TABLE_CONFIG_NAME);
  }

  @Override
  public Dataset<Row> read() throws Exception {
    return Contexts.getSparkSession().read().table(tableName);
  }

  @Override
  public String getAlias() {
    return "hive";
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(TABLE_CONFIG_NAME, ConfigValueType.STRING)
        .build();
  }
  
}
