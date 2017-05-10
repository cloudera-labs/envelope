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

import com.cloudera.labs.envelope.input.translate.TranslateFunction;
import com.cloudera.labs.envelope.input.translate.TranslatorFactory;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
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

  // InputFormat mandatory parameters
  public static final String INPUT_FORMAT_TYPE_CONFIG = "format-class";
  public static final String INPUT_FORMAT_KEY_CONFIG = "key-class";
  public static final String INPUT_FORMAT_VALUE_CONFIG = "value-class";

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

    if (config.getString(FORMAT_CONFIG).equals("input-format")) {
      if (!config.hasPath(INPUT_FORMAT_TYPE_CONFIG)) {
        throw new RuntimeException("Filesystem 'input-format' requires '" + INPUT_FORMAT_TYPE_CONFIG + "' config");
      }

      if (!config.hasPath(INPUT_FORMAT_KEY_CONFIG)) {
        throw new RuntimeException("Filesystem 'input-format' requires '" + INPUT_FORMAT_KEY_CONFIG + "' config");
      }

      if (!config.hasPath(INPUT_FORMAT_VALUE_CONFIG)) {
        throw new RuntimeException("Filesystem 'input-format' requires '" + INPUT_FORMAT_VALUE_CONFIG + "' config");
      }

      if (!config.hasPath("translator")) {
        throw new RuntimeException("Filesystem 'input-format' requires 'translator' config");
      }
    }
  }

  @SuppressWarnings("unchecked")
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
      case "input-format":
        String inputType = config.getString(INPUT_FORMAT_TYPE_CONFIG);
        String keyType = config.getString(INPUT_FORMAT_KEY_CONFIG);
        String valueType = config.getString(INPUT_FORMAT_VALUE_CONFIG);

        Config translatorConfig = config.getConfig("translator");

        LOG.debug("Reading InputFormat[{}]: {}", inputType, path);

        Class<? extends InputFormat> typeClazz = Class.forName(inputType).asSubclass(InputFormat.class);
        Class<?> keyClazz = Class.forName(keyType);
        Class<?> valueClazz = Class.forName(valueType);

        JavaSparkContext context = new JavaSparkContext(Contexts.getSparkSession().sparkContext());
        JavaPairRDD<?, ?> rdd = context.newAPIHadoopFile(path, typeClazz, keyClazz, valueClazz, new Configuration());

        // NOTE: Suppressed unchecked warning
        // Look at https://books.google.com/books?id=zaoK0Z2STlkC&pg=PA28&lpg=PA28&dq=java+capture+%3C?%3E&source=bl&ots=6Yvmcb-2HP&sig=plvfyf16f7npvQ4IEanVAIqPsRg&hl=en&sa=X&ved=0ahUKEwjvh-62m-PTAhUBy2MKHeL8D-MQ6AEITDAG#v=onepage&q&f=false
        // for using a Wildcard Capture helper - might work here?
        fs = Contexts.getSparkSession().createDataFrame(rdd.flatMap(new TranslateFunction(translatorConfig)),
            TranslatorFactory.create(translatorConfig).getSchema());
        break;
      default:
        throw new RuntimeException("Filesystem input format not supported: " + format);
    }

    return fs;
  }

}
