/*
 * Copyright (c) 2015-2019, Cloudera, Inc. All Rights Reserved.
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

import com.cloudera.labs.envelope.component.CanReturnErroredData;
import com.cloudera.labs.envelope.component.InstantiatedComponent;
import com.cloudera.labs.envelope.component.InstantiatesComponents;
import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.schema.DeclaresProvidingSchema;
import com.cloudera.labs.envelope.schema.InputTranslatorCompatibilityValidation;
import com.cloudera.labs.envelope.schema.Schema;
import com.cloudera.labs.envelope.schema.SchemaFactory;
import com.cloudera.labs.envelope.schema.SchemaNegotiator;
import com.cloudera.labs.envelope.schema.UsesExpectedSchema;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.translate.TranslateFunction;
import com.cloudera.labs.envelope.translate.TranslationResults;
import com.cloudera.labs.envelope.translate.Translator;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.utils.SchemaUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class FileSystemInput implements BatchInput, ProvidesAlias, ProvidesValidations,
    InstantiatesComponents, UsesExpectedSchema, DeclaresProvidingSchema, CanReturnErroredData {

  private static final Logger LOG = LoggerFactory.getLogger(FileSystemInput.class);

  public static final String FORMAT_CONFIG = "format";
  public static final String PATH_CONFIG = "path";

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

  public static final String INPUT_FORMAT_TYPE_CONFIG = "format-class";
  public static final String CSV_FORMAT = "csv";
  public static final String PARQUET_FORMAT = "parquet";
  public static final String JSON_FORMAT = "json";
  public static final String INPUT_FORMAT_FORMAT = "input-format";
  public static final String TEXT_FORMAT = "text";

  public static final String TRANSLATOR_CONFIG = "translator";

  public static final String SCHEMA_CONFIG = "schema";

  private ConfigUtils.OptionMap options;
  private StructType schema;
  private String format;
  private String path;
  private String inputType;
  private boolean hasTranslator;
  private Config translatorConfig;
  private StructType expectedSchema;

  @Override
  public String getAlias() {
    return "filesystem";
  }
  
  private Dataset<Row> errors;
  
  @Override
  public void configure(Config config) {
    format = config.getString(FORMAT_CONFIG);
    path = config.getString(PATH_CONFIG);

    if (format.equals(CSV_FORMAT)) {
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

    if ((format.equals(CSV_FORMAT) || format.equals(JSON_FORMAT)) &&
        config.hasPath(SCHEMA_CONFIG)) {
      Config schemaConfig = config.getConfig(SCHEMA_CONFIG);
      this.schema = SchemaFactory.create(schemaConfig, true).getSchema();
    }

    if (format.equals(INPUT_FORMAT_FORMAT)) {
      inputType = config.getString(INPUT_FORMAT_TYPE_CONFIG);
    }

    hasTranslator = config.hasPath(TRANSLATOR_CONFIG);
    if (hasTranslator) {
      translatorConfig = config.getConfig(TRANSLATOR_CONFIG);
    }
  }

  @Override
  public Dataset<Row> read() throws Exception {
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

  private Dataset<Row> readText(String path) {
    Dataset<Row> lines = Contexts.getSparkSession().read().text(path);

    if (hasTranslator) {
      TranslateFunction translateFunction = getTranslateFunction(translatorConfig);
      TranslationResults results = new TranslationResults(
          lines.javaRDD().flatMap(translateFunction),
          translateFunction.getProvidingSchema(),
          getProvidingSchema());
      errors = results.getErrors();

      return results.getTranslated();
    }
    else {
      return lines;
    }
  }

  private Dataset<Row> readInputFormat(String path) throws Exception {
    LOG.debug("Reading InputFormat[{}]: {}", inputType, path);

    Class<? extends InputFormat> inputFormatClass = Class.forName(inputType).asSubclass(InputFormat.class);

    TranslateFunction translateFunction = getTranslateFunction(translatorConfig);

    Dataset<Row> encoded = getEncodedRowsFromInputFormat(path, inputFormatClass);

    TranslationResults results = new TranslationResults(
        encoded.javaRDD().flatMap(translateFunction),
        translateFunction.getProvidingSchema(),
        getProvidingSchema());
    errors = results.getErrors();

    return results.getTranslated();
  }

  @Override
  public void receiveExpectedSchema(StructType expectedSchema) {
    this.expectedSchema = expectedSchema;
  }

  @Override
  public StructType getProvidingSchema() {
    DataType keyDataType = getKeyDataType();
    DataType valueDataType = getValueDataType();

    List<StructField> fields = Lists.newArrayList();

    if (keyDataType != null) {
      fields.add(DataTypes.createStructField("key", keyDataType, true));
    }
    fields.add(DataTypes.createStructField(Translator.VALUE_FIELD_NAME, valueDataType, false));

    return DataTypes.createStructType(fields);
  }

  private DataType getKeyDataType() {
    if (Arrays.asList(expectedSchema.fieldNames()).contains("key")) {
      DataType keyDataType = expectedSchema.fields()[expectedSchema.fieldIndex("key")].dataType();

      if (convertToClass(keyDataType) == null) {
        throw new RuntimeException("Translator for filesystem input's input format is not compatible"
            + " because it does not use a supported type for the 'key' field");
      }

      return keyDataType;
    }
    else {
      // If the translator doesn't want the key field we don't specify a type so as to signal
      // that we want the key to be discarded. This is important for the 'input-format' format
      // because it is not known at runtime what the data type of an input format key should be.
      // We don't do the same for the value field because that is mandatory and so the translator
      // would typically be specifying which data type to use.
      return null;
    }
  }

  private DataType getValueDataType() {
    DataType valueDataType;

    if (Arrays.asList(expectedSchema.fieldNames()).contains(Translator.VALUE_FIELD_NAME)) {
      valueDataType = expectedSchema.fields()[expectedSchema.fieldIndex(Translator.VALUE_FIELD_NAME)].dataType();
    }
    else {
      // In the rare situation that the translator does not expect a specific data type for the
      // value column then we are in a tricky situation because there is no way to inspect the
      // input format class to see what data types it will return. Instead, we compromise and assume
      // it is text, since it seems reasonable that a schemaless translator would probably be used
      // with the input-format format so that raw lines of text from the file could be retrieved.
      valueDataType = DataTypes.StringType;
    }

    if (convertToClass(valueDataType) == null) {
      throw new RuntimeException("Translator for filesystem input's input format is not compatible"
          + " because it does not use a supported type for the '" + Translator.VALUE_FIELD_NAME + "' field");
    }

    return valueDataType;
  }

  private Dataset<Row> getEncodedRowsFromInputFormat(String path, Class<? extends InputFormat> inputFormatClass) {
    JavaSparkContext context = new JavaSparkContext(Contexts.getSparkSession().sparkContext());
    JavaPairRDD rawRDD = context.newAPIHadoopFile(
        path, inputFormatClass, convertToClass(getKeyDataType()), convertToClass(getValueDataType()),
        new Configuration());

    boolean useKey = getKeyDataType() != null;
    JavaRDD<Row> encodedRDD = rawRDD.map(new EncodeRecordAsKeyValueFunction(useKey));

    return Contexts.getSparkSession().createDataFrame(encodedRDD, getProvidingSchema());
  }

  private static class EncodeRecordAsKeyValueFunction implements Function<Tuple2, Row> {
    private boolean useKey;

    EncodeRecordAsKeyValueFunction(boolean useKey) {
      this.useKey = useKey;
    }

    @Override
    public Row call(Tuple2 record) {
      Object value = unwrapWritable(record._2());

      if (useKey) {
        Object key = unwrapWritable(record._1());
        return RowFactory.create(key, value);
      }
      else {
        return RowFactory.create(value);
      }
    }

    private Object unwrapWritable(Object object) {
      if (object instanceof Text) {
        return object.toString();
      }
      else if (object instanceof LongWritable) {
        return ((LongWritable)object).get();
      }
      else if (object instanceof BytesWritable) {
        return ((BytesWritable)object).getBytes();
      }
      else {
        return object;
      }
    }
  }

  private Class<?> convertToClass(DataType dataType) {
    if (dataType == null) {
      return Text.class;
    }
    else if (dataType.equals(DataTypes.StringType)) {
      return Text.class;
    }
    else if (dataType.equals(DataTypes.LongType)) {
      return LongWritable.class;
    }
    else if (dataType.equals(DataTypes.BinaryType)) {
      return BytesWritable.class;
    }
    else {
       throw new RuntimeException("Filesystem input is not compatible with translator. " +
        "Use a translator that expects the value to be a string, long, or byte array.");
    }
  }

  private TranslateFunction getTranslateFunction(Config translatorConfig) {
    TranslateFunction translateFunction = new TranslateFunction(translatorConfig);
    SchemaNegotiator.negotiate(this, translateFunction);

    return translateFunction;
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(FORMAT_CONFIG, ConfigValueType.STRING)
        .mandatoryPath(PATH_CONFIG, ConfigValueType.STRING)
        .allowedValues(FORMAT_CONFIG,
            PARQUET_FORMAT, JSON_FORMAT, INPUT_FORMAT_FORMAT, TEXT_FORMAT, CSV_FORMAT)
        .ifPathHasValue(FORMAT_CONFIG, TEXT_FORMAT,
            Validations.single().optionalPath(TRANSLATOR_CONFIG, ConfigValueType.OBJECT))
        .ifPathHasValue(FORMAT_CONFIG, INPUT_FORMAT_FORMAT,
            Validations.single().mandatoryPath(INPUT_FORMAT_TYPE_CONFIG, ConfigValueType.STRING))
        .ifPathHasValue(FORMAT_CONFIG, INPUT_FORMAT_FORMAT,
            Validations.single().mandatoryPath(TRANSLATOR_CONFIG, ConfigValueType.OBJECT))
        .optionalPath(SCHEMA_CONFIG, ConfigValueType.OBJECT)
        .optionalPath(CSV_HEADER_CONFIG)
        .optionalPath(CSV_SEPARATOR_CONFIG)
        .optionalPath(CSV_ENCODING_CONFIG)
        .optionalPath(CSV_QUOTE_CONFIG)
        .optionalPath(CSV_ESCAPE_CONFIG)
        .optionalPath(CSV_COMMENT_CONFIG)
        .optionalPath(CSV_INFER_SCHEMA_CONFIG)
        .optionalPath(CSV_IGNORE_LEADING_CONFIG)
        .optionalPath(CSV_IGNORE_TRAILING_CONFIG)
        .optionalPath(CSV_NULL_VALUE_CONFIG)
        .optionalPath(CSV_NAN_VALUE_CONFIG)
        .optionalPath(CSV_POS_INF_CONFIG)
        .optionalPath(CSV_NEG_INF_CONFIG)
        .optionalPath(CSV_DATE_CONFIG)
        .optionalPath(CSV_TIMESTAMP_CONFIG)
        .optionalPath(CSV_MAX_COLUMNS_CONFIG)
        .optionalPath(CSV_MAX_CHARS_COLUMN_CONFIG)
        .optionalPath(CSV_MAX_MALFORMED_LOG_CONFIG)
        .optionalPath(CSV_MODE_CONFIG)
        .handlesOwnValidationPath(TRANSLATOR_CONFIG)
        .handlesOwnValidationPath(SCHEMA_CONFIG)
        .add(new InputTranslatorCompatibilityValidation())
        .build();
  }

  @Override
  public Set<InstantiatedComponent> getComponents(Config config, boolean configure) {
    Set<InstantiatedComponent> components = Sets.newHashSet();

    if (config.hasPath(TRANSLATOR_CONFIG)) {
      TranslateFunction translateFunction = getTranslateFunction(config.getConfig(TRANSLATOR_CONFIG));
      components.addAll(translateFunction.getComponents(config.getConfig(TRANSLATOR_CONFIG), configure));
    }

    if (config.hasPath(SCHEMA_CONFIG)) {
      components.addAll(SchemaUtils.getSchemaComponents(config, configure, SCHEMA_CONFIG));
    }

    return components;
  }
  
  @Override
  public Dataset<Row> getErroredData() {
    if (errors == null) {
      return Contexts.getSparkSession().emptyDataFrame();
    }

    return errors;
  }

}
