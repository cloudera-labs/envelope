/**
 * Copyright © 2016-2017 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.input;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.cloudera.labs.envelope.spark.Contexts;
import com.typesafe.config.Config;

public class HiveInput implements BatchInput {

  public static final String TABLE_CONFIG_NAME = "table";

  private Config config;

  @Override
  public void configure(Config config) {
    this.config = config;

    if (!config.hasPath(TABLE_CONFIG_NAME)) {
      throw new RuntimeException("Hive input requires '" + TABLE_CONFIG_NAME + "' property");
    }
  }

  @Override
  public Dataset<Row> read() throws Exception {
    String tableName = config.getString(TABLE_CONFIG_NAME);

    return Contexts.getSparkSession().read().table(tableName);
  }

}
