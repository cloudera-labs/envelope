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
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.List;
import java.util.Set;

public class FileSystemOutput implements BulkOutput, ProvidesAlias, ProvidesValidations {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemOutput.class);

  public final static String FORMAT_CONFIG = "format";
  public final static String CSV_FORMAT = "csv";
  public final static String PARQUET_FORMAT = "parquet";
  public final static String JSON_FORMAT = "json";
  public final static String PATH_CONFIG = "path";
  public final static String PARTITION_COLUMNS_CONFIG = "partition.by";

  // CSV optional parameters
  public final static String CSV_SEPARATOR_CONFIG = "separator";
  public final static String CSV_QUOTE_CONFIG = "quote";
  public final static String CSV_ESCAPE_CONFIG = "escape";
  public final static String CSV_ESCAPE_QUOTES_CONFIG = "escape-quotes";
  public final static String CSV_QUOTE_ALL_CONFIG = "quote-all";
  public final static String CSV_HEADER_CONFIG = "header";
  public final static String CSV_NULL_VALUE_CONFIG = "null-value";
  public final static String CSV_COMPRESSION_CONFIG = "compression";
  public final static String CSV_DATE_CONFIG = "date-format";
  public final static String CSV_TIMESTAMP_CONFIG = "timestamp-format";

  private ConfigUtils.OptionMap options;
  private String format;
  private String path;
  private List<String> columns;

  @Override
  public void configure(Config config) {
    format = config.getString(FORMAT_CONFIG);
    path = config.getString(PATH_CONFIG);
    if (config.hasPath(PARTITION_COLUMNS_CONFIG)) {
      columns = config.getStringList(PARTITION_COLUMNS_CONFIG);
    }

    if (config.getString(FORMAT_CONFIG).equals(CSV_FORMAT)) {
      options = new ConfigUtils.OptionMap(config)
          .resolve("sep", CSV_SEPARATOR_CONFIG)
          .resolve("quote", CSV_QUOTE_CONFIG)
          .resolve("escape", CSV_ESCAPE_CONFIG)
          .resolve("escapeQuotes", CSV_ESCAPE_QUOTES_CONFIG)
          .resolve("quoteAll", CSV_QUOTE_ALL_CONFIG)
          .resolve("header", CSV_HEADER_CONFIG)
          .resolve("nullValue", CSV_NULL_VALUE_CONFIG)
          .resolve("compression", CSV_COMPRESSION_CONFIG)
          .resolve("dateFormat", CSV_DATE_CONFIG)
          .resolve("timestampFormat", CSV_TIMESTAMP_CONFIG);
    }
  }

  @Override
  public void applyBulkMutations(List<Tuple2<MutationType, Dataset<Row>>> planned) {
    for (Tuple2<MutationType, Dataset<Row>> plan : planned) {
      MutationType mutationType = plan._1();
      Dataset<Row> mutation = plan._2();

      DataFrameWriter<Row> writer = mutation.write();

      if (columns != null) {
        LOG.debug("Partitioning output");

        writer = writer.partitionBy(columns.toArray(new String[columns.size()]));
      }

      switch (mutationType) {
        case INSERT:
          writer = writer.mode(SaveMode.Append);
          break;
        case OVERWRITE:
          writer = writer.mode(SaveMode.Overwrite);
          break;
        default:
          throw new RuntimeException("Filesystem output does not support mutation type: " + mutationType);
      }

      switch (format) {
        case PARQUET_FORMAT:
          LOG.debug("Writing Parquet: {}", path);
          writer.parquet(path);
          break;
        case CSV_FORMAT:
          LOG.debug("Writing CSV: {}", path);
          writer.options(options).csv(path);
          break;
        case JSON_FORMAT:
          LOG.debug("Writing JSON: {}", path);
          writer.json(path);
          break;
        default:
          throw new RuntimeException("Filesystem output does not support file format: " + format);
      }
    }
  }

  @Override
  public Set<MutationType> getSupportedBulkMutationTypes() {
    return Sets.newHashSet(MutationType.INSERT, MutationType.OVERWRITE);
  }

  @Override
  public String getAlias() {
    return "filesystem";
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(FORMAT_CONFIG, ConfigValueType.STRING)
        .allowedValues(FORMAT_CONFIG, PARQUET_FORMAT, CSV_FORMAT, JSON_FORMAT)
        .mandatoryPath(PATH_CONFIG, ConfigValueType.STRING)
        .optionalPath(PARTITION_COLUMNS_CONFIG, ConfigValueType.LIST)
        .optionalPath(CSV_SEPARATOR_CONFIG)
        .optionalPath(CSV_QUOTE_CONFIG)
        .optionalPath(CSV_ESCAPE_CONFIG)
        .optionalPath(CSV_ESCAPE_QUOTES_CONFIG)
        .optionalPath(CSV_QUOTE_ALL_CONFIG)
        .optionalPath(CSV_HEADER_CONFIG)
        .optionalPath(CSV_NULL_VALUE_CONFIG)
        .optionalPath(CSV_COMPRESSION_CONFIG)
        .optionalPath(CSV_DATE_CONFIG)
        .optionalPath(CSV_TIMESTAMP_CONFIG)
        .build();
  }
}
