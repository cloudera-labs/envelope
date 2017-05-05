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

import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemInput implements BatchInput {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemInput.class);

  public static final String FORMAT_CONFIG = "format";
  public static final String PATH_CONFIG = "path";

  // CSV optional parameters
  public static final String CSV_HEADER_CONFIG = "header";
  public static final String CSV_SEPARATOR_CONFIG = "separator";
  public static final String CSV_ENCODING_CONFIG = "encoding";
  public static final String CSV_QUOTE_CONFIG = "quote";
  public static final String CSV_ESCAPE_CONFIG = "escape";
  public static final String CSV_COMMENT_CONFIG = "comment";
  public static final String CSV_INFER_SCHEMA_CONFIG = "infer-schema";
  public static final String CSV_IGNORE_LEADING_CONFIG = "ignore-leading-ws";
  public static final String CSV_IGNORE_TRAILING_CONFIG = "ignore-trailing-ws";
  public static final String CSV_NULL_VALUE_CONFIG = "null-value";
  public static final String CSV_NAN_VALUE_CONFIG = "nan-value";
  public static final String CSV_POS_INF_CONFIG = "positive-infinity";
  public static final String CSV_NEG_INF_CONFIG = "negative-infinity";
  public static final String CSV_DATE_CONFIG = "date-format";
  public static final String CSV_TIMESTAMP_CONFIG = "timestamp-format";
  public static final String CSV_MAX_COLUMNS_CONFIG = "max-columns";
  public static final String CSV_MAX_CHARS_COLUMN_CONFIG = "max-chars-per-column";
  public static final String CSV_MAX_MALFORMED_LOG_CONFIG = "max-malformed-logged";
  public static final String CSV_MODE_CONFIG = "mode";

  private Config config;
  private ConfigUtils.OptionMap options;

  @Override
  public void configure(Config config) {
    this.config = config;

    if (!config.hasPath(FORMAT_CONFIG) || config.getString(FORMAT_CONFIG).isEmpty()) {
      throw new RuntimeException("Filesystem input requires '" + FORMAT_CONFIG + "' config");
    }

    if (!config.hasPath(PATH_CONFIG) || config.getString(PATH_CONFIG).isEmpty()) {
      throw new RuntimeException("Filesystem input requires '" + PATH_CONFIG + "' config");
    }

    if (config.getString(FORMAT_CONFIG).equals("csv")) {
      options = new ConfigUtils.OptionMap(config)
          .resolve("sep", CSV_SEPARATOR_CONFIG)
          .resolve("encoding", CSV_ENCODING_CONFIG)
          .resolve("quote", CSV_QUOTE_CONFIG)
          .resolve("escape", CSV_ESCAPE_CONFIG)
          .resolve("comment", CSV_COMMENT_CONFIG)
          .resolve("header", CSV_HEADER_CONFIG)
          .resolve("inferSchema", CSV_INFER_SCHEMA_CONFIG)
          .resolve("ignoreLeadingWhiteSpace", CSV_IGNORE_LEADING_CONFIG)
          .resolve("ignoreTrailingWhiteSpace", CSV_IGNORE_TRAILING_CONFIG)
          .resolve("nullValue", CSV_NULL_VALUE_CONFIG)
          .resolve("nanValue", CSV_NAN_VALUE_CONFIG)
          .resolve("positiveInf", CSV_POS_INF_CONFIG)
          .resolve("negativeInf", CSV_NEG_INF_CONFIG)
          .resolve("dateFormat", CSV_DATE_CONFIG)
          .resolve("timestampFormat", CSV_TIMESTAMP_CONFIG)
          .resolve("maxColumns", CSV_MAX_COLUMNS_CONFIG)
          .resolve("maxCharsPerColumn", CSV_MAX_CHARS_COLUMN_CONFIG)
          .resolve("maxMalformedLogPerPartition", CSV_MAX_MALFORMED_LOG_CONFIG)
          .resolve("mode", CSV_MODE_CONFIG);
    }
  }

  @Override
  public Dataset<Row> read() throws Exception {
    String format = config.getString(FORMAT_CONFIG);
    String path = config.getString(PATH_CONFIG);

    Dataset<Row> fs = null;

    switch (format) {
      case "parquet":
        LOG.debug("Reading Parquet: {}", path);
        fs = Contexts.getSparkSession().read().parquet(path);
        break;
      case "json":
        LOG.debug("Reading JSON: {}", path);
        fs = Contexts.getSparkSession().read().json(path);
        break;
      case "csv":
        LOG.debug("Reading CSV: {}", path);
        fs = Contexts.getSparkSession().read().options(options).csv(path);
        break;
      default:
        throw new RuntimeException("Filesystem input format not supported: " + format);
    }

    return fs;
  }

}
