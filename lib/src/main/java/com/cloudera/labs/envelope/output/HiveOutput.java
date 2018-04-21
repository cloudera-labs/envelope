/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import com.cloudera.labs.envelope.spark.Contexts;

import scala.Tuple2;

public class HiveOutput implements BulkOutput, ProvidesAlias {

  public final static String TABLE_CONFIG = "table";
  public final static String PARTITION_BY_CONFIG = "partition.by";
  public final static String LOCATION_CONFIG = "location";
  public final static String OPTIONS_CONFIG = "options";
  public final static String ALIGN_COLUMNS_CONFIG = "align.columns";
  public final static String SPARK_SQL_CASE_SENSITIVE_CONFIG = "spark.sql.caseSensitive";

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
      Dataset<Row> mutation = (getAlignColumns()) ? alignColumns(plan._2()) : plan._2();
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

  public Dataset<Row> alignColumns(Dataset<Row> input) {
    Boolean caseSensitive = Contexts.getSparkSession().sparkContext().getConf().
                            getBoolean(SPARK_SQL_CASE_SENSITIVE_CONFIG, false);

    Set<String> inputCols = new HashSet<String>();
    for (String col : Arrays.asList(input.schema().fieldNames())) {
      inputCols.add((caseSensitive) ? col : col.toLowerCase());
    }

    List<String> tableCols = new ArrayList<String>();
    for (String col : Contexts.getSparkSession().table(getTableName()).schema().fieldNames()) {
      tableCols.add((caseSensitive) ? col : col.toLowerCase());
    }

    List<Column> alignedCols = new ArrayList<Column>();
    for (String column : tableCols) {
      alignedCols.add((inputCols.contains(column)) ? functions.col(column) : 
                                                     functions.lit(null).alias(column));
    } 

    return input.select(alignedCols.toArray(new Column[alignedCols.size()]));
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

  private boolean getAlignColumns() {
    return config.hasPath(ALIGN_COLUMNS_CONFIG) && config.getBoolean(ALIGN_COLUMNS_CONFIG);
  }

  @Override
  public String getAlias() {
    return "hive";
  }
}
