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
import java.util.Set;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

import scala.Tuple2;

public class FileSystemOutput implements BulkOutput {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemOutput.class);

  public final static String FORMAT_CONFIG = "format";
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

  private Config config;
  private ConfigUtils.OptionMap options;

  @Override
  public void configure(Config config) {
    this.config = config;

    if (!config.hasPath(FORMAT_CONFIG) || config.getString(FORMAT_CONFIG).isEmpty()) {
      throw new RuntimeException("Filesystem output requires '" + FORMAT_CONFIG + "' property");
    }

    if (!config.hasPath(PATH_CONFIG) || config.getString(PATH_CONFIG).isEmpty() ) {
      throw new RuntimeException("Filesystem output requires '" + PATH_CONFIG + "' property");
    }

    if (config.hasPath(PARTITION_COLUMNS_CONFIG) && config.getList(PARTITION_COLUMNS_CONFIG).isEmpty() ) {
      throw new RuntimeException("Filesystem output '" + PATH_CONFIG + "' property cannot be empty if defined");
    }

    if (config.getString(FORMAT_CONFIG).equals("csv")) {
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

      String format = config.getString(FORMAT_CONFIG);
      String path = config.getString(PATH_CONFIG);

      DataFrameWriter<Row> writer = mutation.write();

      if (config.hasPath(PARTITION_COLUMNS_CONFIG)) {
        LOG.debug("Partitioning output");
        List<String> columns = config.getStringList(PARTITION_COLUMNS_CONFIG);
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
        case "parquet":
          LOG.debug("Writing Parquet: {}", path);
          writer.parquet(path);
          break;
        case "csv":
          LOG.debug("Writing CSV: {}", path);
          writer.options(options).csv(path);
          break;
        case "json":
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
}
