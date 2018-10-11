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

package com.cloudera.labs.envelope.input.translate;

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.utils.DateTimeUtils.DateTimeParser;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.cloudera.labs.envelope.utils.TranslatorUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * A translator implementation for plain delimited text messages, e.g. CSV.
 */
public class DelimitedTranslator implements Translator<String, String>, ProvidesAlias, ProvidesValidations {

  private String delimiter;
  private List<String> fieldNames;
  private List<String> fieldTypes;
  private DateTimeParser dateTimeParser;
  private StructType schema;
  private List<Object> values = Lists.newArrayList();
  private boolean doesAppendRaw;
  private boolean delimiterRegex;

  public static final String DELIMITER_CONFIG_NAME = "delimiter";
  public static final String DELIMITER_REGEX_CONFIG_NAME = "delimiter-regex";
  public static final String FIELD_NAMES_CONFIG_NAME = "field.names";
  public static final String FIELD_TYPES_CONFIG_NAME = "field.types";
  public static final String TIMESTAMP_FORMAT_CONFIG_NAME = "timestamp.formats";

  @Override
  public void configure(Config config) throws IllegalArgumentException {
    delimiter = resolveDelimiter(config.getString(DELIMITER_CONFIG_NAME));
    fieldNames = config.getStringList(FIELD_NAMES_CONFIG_NAME);
    fieldTypes = config.getStringList(FIELD_TYPES_CONFIG_NAME);
    delimiterRegex = ConfigUtils.getOrElse(config, DELIMITER_REGEX_CONFIG_NAME, false);
    doesAppendRaw = TranslatorUtils.doesAppendRaw(config);
    if (doesAppendRaw) {
      fieldNames.add(TranslatorUtils.getAppendRawKeyFieldName(config));
      fieldTypes.add("string");
      fieldNames.add(TranslatorUtils.getAppendRawValueFieldName(config));
      fieldTypes.add("string");
    }

    dateTimeParser = new DateTimeParser();
    if (config.hasPath(TIMESTAMP_FORMAT_CONFIG_NAME)) {
      dateTimeParser.configureFormat(
          config.getStringList(TIMESTAMP_FORMAT_CONFIG_NAME));
    }

    schema = RowUtils.structTypeFor(fieldNames, fieldTypes);
  }

  @Override
  public Iterable<Row> translate(String key, String value) {
    int numFields = (doesAppendRaw) ? (fieldNames.size() - 2) : fieldNames.size();
    String[] stringValues = value.split((delimiterRegex) ?
                            delimiter : Pattern.quote(delimiter), fieldNames.size());
    values.clear();

    for (int valuePos = 0; valuePos < numFields; valuePos++) {
      if (valuePos < stringValues.length) {
        String fieldValue = stringValues[valuePos];
     
        if (fieldValue.length() == 0) {
          values.add(null);
        }
        else {
          switch (fieldTypes.get(valuePos)) {
            case "string":
              values.add(fieldValue);
              break;
            case "float":
              values.add(Float.parseFloat(fieldValue));
              break;
            case "double":
              values.add(Double.parseDouble(fieldValue));
              break;
            case "int":
              values.add(Integer.parseInt(fieldValue));
              break;
            case "long":
              values.add(Long.parseLong(fieldValue));
              break;
            case "boolean":
              values.add(Boolean.parseBoolean(fieldValue));
              break;
            case "timestamp":
              values.add(new Timestamp(
                  dateTimeParser.parse(fieldValue).getMillis()));
              break;
            default:
              throw new RuntimeException("Unsupported delimited field type: "
                  + fieldTypes.get(valuePos));
          }
        }
      } else {
        values.add(null);
      }
    }

    Row row = RowFactory.create(values.toArray());
    
    if (doesAppendRaw) {
      row = RowUtils.append(row, key);
      row = RowUtils.append(row, value);
    }

    return Collections.singleton(row);
  }

  @Override
  public StructType getSchema() {
    return schema;
  }

  private String resolveDelimiter(String delimiterArg) {
    if (delimiterArg.startsWith("chars:")) {
      String[] codePoints = delimiterArg.substring("chars:".length()).split(",");

      StringBuilder delimiter = new StringBuilder();
      for (String codePoint : codePoints) {
        delimiter.append(Character.toChars(Integer.parseInt(codePoint)));
      }

      return delimiter.toString();
    }
    else {
      return delimiterArg;
    }
  }

  @Override
  public String getAlias() {
    return "delimited";
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(DELIMITER_CONFIG_NAME, ConfigValueType.STRING)
        .mandatoryPath(FIELD_NAMES_CONFIG_NAME, ConfigValueType.LIST)
        .mandatoryPath(FIELD_TYPES_CONFIG_NAME, ConfigValueType.LIST)
        .optionalPath(DELIMITER_REGEX_CONFIG_NAME, ConfigValueType.BOOLEAN)
        .optionalPath(TIMESTAMP_FORMAT_CONFIG_NAME, ConfigValueType.LIST)
        .addAll(TranslatorUtils.APPEND_RAW_VALIDATIONS)
        .build();
  }
  
}
