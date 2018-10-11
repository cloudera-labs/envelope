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
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.functions;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HiveOutput implements BulkOutput, ProvidesAlias, ProvidesValidations {

  public final static String TABLE_CONFIG = "table";
  public final static String PARTITION_BY_CONFIG = "partition.by";
  public final static String LOCATION_CONFIG = "location";
  public final static String OPTIONS_CONFIG = "options";
  public final static String ALIGN_COLUMNS_CONFIG = "align.columns";
  public final static String SPARK_SQL_CASE_SENSITIVE_CONFIG = "spark.sql.caseSensitive";

  private String tableName;
  private ConfigUtils.OptionMap options;
  private String[] partitionColumns;
  private boolean doesAlignColumns;

  @Override
  public void configure(Config config) {
    tableName = config.getString(TABLE_CONFIG);

    if (config.hasPath(PARTITION_BY_CONFIG)) {
      List<String> colNames = config.getStringList(PARTITION_BY_CONFIG);
      partitionColumns = colNames.toArray(new String[0]);
    }

    doesAlignColumns = ConfigUtils.getOrElse(config, ALIGN_COLUMNS_CONFIG, false);

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
      Dataset<Row> mutation = (doesAlignColumns) ? alignColumns(plan._2()) : plan._2();
      DataFrameWriter<Row> writer = mutation.write();

      if (partitionColumns != null) {
        writer = writer.partitionBy(partitionColumns);
      }

      if (options != null) {
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

      writer.insertInto(tableName);
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
    for (String col : Contexts.getSparkSession().table(tableName).schema().fieldNames()) {
      tableCols.add((caseSensitive) ? col : col.toLowerCase());
    }

    List<Column> alignedCols = new ArrayList<Column>();
    for (String column : tableCols) {
      alignedCols.add((inputCols.contains(column)) ? functions.col(column) :
                                                     functions.lit(null).alias(column));
    }

    return input.select(alignedCols.toArray(new Column[alignedCols.size()]));
  }

  @Override
  public String getAlias() {
    return "hive";
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(TABLE_CONFIG, ConfigValueType.STRING)
        .optionalPath(LOCATION_CONFIG, ConfigValueType.STRING)
        .optionalPath(OPTIONS_CONFIG, ConfigValueType.OBJECT)
        .optionalPath(PARTITION_BY_CONFIG, ConfigValueType.LIST)
        .optionalPath(ALIGN_COLUMNS_CONFIG, ConfigValueType.BOOLEAN)
        .handlesOwnValidationPath(OPTIONS_CONFIG)
        .build();
  }

}
