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
package com.cloudera.labs.envelope.output;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

import scala.Tuple2;

public class HiveOutput implements BulkOutput {

  public final static String TABLE_CONFIG = "table";
  public final static String PARTITION_BY_CONFIG = "partition.by";
  public final static String LOCATION_CONFIG = "location";
  public final static String OPTIONS_CONFIG = "options";

  private Config config;
  private ConfigUtils.OptionMap options;

  @Override
  public void configure(Config config) {
    this.config = config;

    if (!config.hasPath(TABLE_CONFIG)) {
      throw new RuntimeException("Hive output requires '" + TABLE_CONFIG + "' property");
    }

    if (config.hasPath(LOCATION_CONFIG) || config.hasPath(OPTIONS_CONFIG)) {
      options = new ConfigUtils.OptionMap(config);

      if (config.hasPath(LOCATION_CONFIG)) {
        options.resolve("path", LOCATION_CONFIG);
      }

      if (config.hasPath(OPTIONS_CONFIG)) {
        Config optionsConfig = config.getConfig(OPTIONS_CONFIG);
        for (Map.Entry<String, ConfigValue> entry : optionsConfig.entrySet()) {
          String param = entry.getKey();
          String value = entry.getValue().unwrapped().toString();
          if (value != null) {
            options.put(param, value);
          }
        }
      }
    }
  }

  @Override
  public void applyBulkMutations(List<Tuple2<MutationType, Dataset<Row>>> planned) {    
    for (Tuple2<MutationType, Dataset<Row>> plan : planned) {
      MutationType mutationType = plan._1();
      Dataset<Row> mutation = plan._2();
      DataFrameWriter<Row> writer = mutation.write();

      if (hasPartitionColumns()) {
        writer = writer.partitionBy(getPartitionColumns());
      }

      if (hasOptions()) {
        writer = writer.options(options);
      }

      switch (mutationType) {
        case INSERT:
          writer = writer.mode(SaveMode.Append);
          break;
        case OVERWRITE:
          writer = writer.mode(SaveMode.Overwrite);
          break;
        default:
          throw new RuntimeException("Hive output does not support mutation type: " + mutationType);
      }

      writer.insertInto(getTableName());
    }
  }

  @Override
  public Set<MutationType> getSupportedBulkMutationTypes() {
    return Sets.newHashSet(MutationType.INSERT, MutationType.OVERWRITE);
  }

  private boolean hasPartitionColumns() {
    return config.hasPath(PARTITION_BY_CONFIG);
  }

  private boolean hasOptions() {
    return options != null;
  }

  private String[] getPartitionColumns() {
    List<String> colNames = config.getStringList(PARTITION_BY_CONFIG);
    return colNames.toArray(new String[colNames.size()]) ;
  }

  private String getTableName() {
    return config.getString(TABLE_CONFIG);
  }

  @Override
  public String getAlias() {
    return "hive";
  }
}
