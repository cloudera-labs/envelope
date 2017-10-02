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

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.labs.envelope.input.translate.TranslateFunction;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.utils.AvroUtils;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.typesafe.config.Config;

import scala.Tuple2;

public class FileSystemInput implements BatchInput {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemInput.class);

  public static final String FORMAT_CONFIG = "format";
  public static final String PATH_CONFIG = "path";

  // Schema optional parameters
  public static final String FIELD_NAMES_CONFIG = "field.names";
  public static final String FIELD_TYPES_CONFIG = "field.types";
  public static final String AVRO_LITERAL_CONFIG = "avro-schema.literal";
  public static final String AVRO_FILE_CONFIG = "avro-schema.file";

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

  public static final String CSV_FORMAT = "csv";
  public static final String PARQUET_FORMAT = "parquet";
  public static final String JSON_FORMAT = "json";
  public static final String INPUT_FORMAT_FORMAT = "input-format";
  public static final String TEXT_FORMAT = "text";

  private Config config;
  private ConfigUtils.OptionMap options;
  private StructType schema;

  @Override
  public void configure(Config config) {
    this.config = config;

    if (!config.hasPath(FORMAT_CONFIG) || config.getString(FORMAT_CONFIG).isEmpty()) {
      throw new RuntimeException("Filesystem input requires '" + FORMAT_CONFIG + "' config");
    }

    if (!config.hasPath(PATH_CONFIG) || config.getString(PATH_CONFIG).isEmpty()) {
      throw new RuntimeException("Filesystem input requires '" + PATH_CONFIG + "' config");
    }

    if (config.getString(FORMAT_CONFIG).equals(CSV_FORMAT) || config.getString(FORMAT_CONFIG).equals(JSON_FORMAT)) {
      if ((config.hasPath(FIELD_NAMES_CONFIG) || config.hasPath(FIELD_TYPES_CONFIG)) &&
          (config.hasPath(AVRO_LITERAL_CONFIG) || config.hasPath(AVRO_FILE_CONFIG))) {
        throw new RuntimeException(String.format("Filesystem input has too many schema parameters set. Set either '%s' " +
            "and '%s', or '%s', or '%s'", FIELD_NAMES_CONFIG, FIELD_TYPES_CONFIG, AVRO_FILE_CONFIG, AVRO_LITERAL_CONFIG));

      } else if (config.hasPath(FIELD_NAMES_CONFIG) || config.hasPath(FIELD_TYPES_CONFIG)) {

        if (!config.hasPath(FIELD_NAMES_CONFIG) || config.getStringList(FIELD_NAMES_CONFIG).isEmpty()) {
          throw new RuntimeException("Filesystem input schema parameter missing: " + FIELD_NAMES_CONFIG);
        } else if (!config.hasPath(FIELD_TYPES_CONFIG) || config.getStringList(FIELD_TYPES_CONFIG).isEmpty()) {
          throw new RuntimeException("Filesystem input schema parameter missing: " + FIELD_TYPES_CONFIG);
        }

        List<String> names = config.getStringList(FIELD_NAMES_CONFIG);
        List<String> types = config.getStringList(FIELD_TYPES_CONFIG);

        this.schema = RowUtils.structTypeFor(names, types);

      } else if (config.hasPath(AVRO_FILE_CONFIG) || config.hasPath(AVRO_LITERAL_CONFIG)) {
        if (config.hasPath(AVRO_FILE_CONFIG) && config.hasPath(AVRO_LITERAL_CONFIG)) {
          throw new RuntimeException(String.format("Filesystem input cannot have both schema parameters defined, '%s' and '%s'",
              AVRO_FILE_CONFIG, AVRO_LITERAL_CONFIG));
        }

        Schema avroSchema;
        if (config.hasPath(AVRO_FILE_CONFIG)) {
          if (config.getString(AVRO_FILE_CONFIG).trim().isEmpty()) {
            throw new RuntimeException("Filesystem input schema parameter is missing, '" + AVRO_FILE_CONFIG + "'");
          } else {
            try {
              File avroFile = new File(config.getString(AVRO_FILE_CONFIG));
              avroSchema = new Schema.Parser().parse(avroFile);
            } catch (IOException e) {
              throw new RuntimeException("Error parsing Avro schema file", e);
            }
          }
        } else {
          if (config.getString(AVRO_LITERAL_CONFIG).trim().isEmpty()) {
            throw new RuntimeException("Filesystem input schema parameter is missing, '" + AVRO_LITERAL_CONFIG + "'");
          } else {
            avroSchema = new Schema.Parser().parse(config.getString(AVRO_LITERAL_CONFIG));
          }
        }

        this.schema = AvroUtils.structTypeFor(avroSchema);
      }
    }

    if (config.getString(FORMAT_CONFIG).equals(CSV_FORMAT)) {
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

    if (config.getString(FORMAT_CONFIG).equals(INPUT_FORMAT_FORMAT)) {
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

  @Override
  public Dataset<Row> read() throws Exception {
    String format = config.getString(FORMAT_CONFIG);
    String path = config.getString(PATH_CONFIG);

    Dataset<Row> fs;

    switch (format) {
      case PARQUET_FORMAT:
        fs = readParquet(path);
        break;
      case JSON_FORMAT:
        fs = readJSON(path);
        break;
      case CSV_FORMAT:
        fs = readCSV(path);
        break;
      case INPUT_FORMAT_FORMAT:
        fs = readInputFormat(path);
        break;
      case TEXT_FORMAT:
        fs = readText(path);
        break;
      default:
        throw new RuntimeException("Filesystem input format not supported: " + format);
    }

    return fs;
  }
  
  private Dataset<Row> readParquet(String path) {
    LOG.debug("Reading Parquet: {}", path);

    return Contexts.getSparkSession().read().parquet(path);
  }
  
  private Dataset<Row> readJSON(String path) {
    LOG.debug("Reading JSON: {}", path);

    if (null != schema) {
      return Contexts.getSparkSession().read().schema(schema).json(path);
    } else {
      return Contexts.getSparkSession().read().json(path);
    }
  }
  
  private Dataset<Row> readCSV(String path) {
    LOG.debug("Reading CSV: {}", path);

    if (null != schema) {
      return Contexts.getSparkSession().read().schema(schema).options(options).csv(path);
    } else {
      return Contexts.getSparkSession().read().options(options).csv(path);
    }
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  private Dataset<Row> readInputFormat(String path) throws Exception {
    String inputType = config.getString(INPUT_FORMAT_TYPE_CONFIG);
    String keyType = config.getString(INPUT_FORMAT_KEY_CONFIG);
    String valueType = config.getString(INPUT_FORMAT_VALUE_CONFIG);

    LOG.debug("Reading InputFormat[{}]: {}", inputType, path);

    Class<? extends InputFormat> typeClazz = Class.forName(inputType).asSubclass(InputFormat.class);
    Class<?> keyClazz = Class.forName(keyType);
    Class<?> valueClazz = Class.forName(valueType);

    @SuppressWarnings("resource")
    JavaSparkContext context = new JavaSparkContext(Contexts.getSparkSession().sparkContext());
    JavaPairRDD<?, ?> rdd = context.newAPIHadoopFile(path, typeClazz, keyClazz, valueClazz, new Configuration());
    
    TranslateFunction translateFunction = new TranslateFunction(config.getConfig("translator"));

    return Contexts.getSparkSession().createDataFrame(rdd.flatMap(translateFunction), translateFunction.getSchema());
  }
  
  private Dataset<Row> readText(String path) throws Exception {
    Dataset<Row> lines = Contexts.getSparkSession().read().text(path);

    if (config.hasPath("translator")) {
      Dataset<Tuple2<String, String>> keyedLines = lines.map(
          new PrepareLineForTranslationFunction(), Encoders.tuple(Encoders.STRING(), Encoders.STRING()));
      
      TranslateFunction<String, String> translateFunction = new TranslateFunction<>(config.getConfig("translator"));
      
      return keyedLines.flatMap(translateFunction, RowEncoder.apply(translateFunction.getSchema()));
    }
    else {
      return lines;
    }
  }

  @Override
  public String getAlias() {
    return "filesystem";
  }

  @SuppressWarnings("serial")
  private static class PrepareLineForTranslationFunction implements MapFunction<Row, Tuple2<String, String>> {
    @Override
    public Tuple2<String, String> call(Row line) throws Exception {
      return new Tuple2<String, String>(null, line.getString(0));
    }
  }
}
