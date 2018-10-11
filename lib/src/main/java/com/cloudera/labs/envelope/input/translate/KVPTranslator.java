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
import com.cloudera.labs.envelope.utils.DateTimeUtils.DateTimeParser;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.cloudera.labs.envelope.utils.TranslatorUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * A translator implementation for text key-value pair messages.
 */
public class KVPTranslator implements Translator<String, String>, ProvidesAlias, ProvidesValidations {

  private String kvpDelimiter;
  private String fieldDelimiter;
  private List<String> fieldNames;
  private List<String> fieldTypes;
  private DateTimeParser dateTimeParser;
  private StructType schema;
  private List<Object> values = Lists.newArrayList();
  private Map<String, String> kvpMap = Maps.newHashMap();
  private boolean doesAppendRaw;

  public static final String KVP_DELIMITER_CONFIG_NAME = "delimiter.kvp";
  public static final String FIELD_DELIMITER_CONFIG_NAME = "delimiter.field";
  public static final String FIELD_NAMES_CONFIG_NAME = "field.names";
  public static final String FIELD_TYPES_CONFIG_NAME = "field.types";
  public static final String TIMESTAMP_FORMAT_CONFIG_NAME = "timestamp.formats";

  @Override
  public void configure(Config config) {
    kvpDelimiter = resolveDelimiter(config.getString(KVP_DELIMITER_CONFIG_NAME));
    fieldDelimiter = resolveDelimiter(config.getString(FIELD_DELIMITER_CONFIG_NAME));
    fieldNames = config.getStringList(FIELD_NAMES_CONFIG_NAME);
    fieldTypes = config.getStringList(FIELD_TYPES_CONFIG_NAME);
    
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
    kvpMap.clear();
    values.clear();

    String[] kvps = value.split(Pattern.quote(kvpDelimiter));
    for (String kvp : kvps) {
      String[] components = kvp.split(Pattern.quote(fieldDelimiter));
      String kvpKey = components[0];
      String kvpValue = components.length == 2 ? components[1] : null;

      kvpMap.put(kvpKey, kvpValue);
    }
    
    int numNonAppendedFields = fieldNames.size() - (doesAppendRaw ? 2 : 0);

    for (int fieldPos = 0; fieldPos < numNonAppendedFields; fieldPos++) {
      String fieldName = fieldNames.get(fieldPos);
      String fieldType = fieldTypes.get(fieldPos);
      
      if (kvpMap.containsKey(fieldName)) {
        String kvpValue = kvpMap.get(fieldName);
        
        if (kvpValue == null) {
          values.add(null);
        }
        else {
          switch (fieldType) {
            case "string":
              values.add(kvpValue);
              break;
            case "float":
              values.add(Float.parseFloat(kvpValue));
              break;
            case "double":
              values.add(Double.parseDouble(kvpValue));
              break;
            case "int":
              values.add(Integer.parseInt(kvpValue));
              break;
            case "long":
              values.add(Long.parseLong(kvpValue));
              break;
            case "boolean":
              values.add(Boolean.parseBoolean(kvpValue));
              break;
            case "timestamp":
              values.add(new Timestamp(
                  dateTimeParser.parse(kvpValue).getMillis()));
              break;
            default:
              throw new RuntimeException("Unsupported KVP field type: "
                  + fieldType);
          }
        }
      }
      else {
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
    return "kvp";
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(KVP_DELIMITER_CONFIG_NAME, ConfigValueType.STRING)
        .mandatoryPath(FIELD_DELIMITER_CONFIG_NAME, ConfigValueType.STRING)
        .mandatoryPath(FIELD_NAMES_CONFIG_NAME, ConfigValueType.LIST)
        .mandatoryPath(FIELD_TYPES_CONFIG_NAME, ConfigValueType.LIST)
        .optionalPath(TIMESTAMP_FORMAT_CONFIG_NAME, ConfigValueType.LIST)
        .addAll(TranslatorUtils.APPEND_RAW_VALIDATIONS)
        .build();
  }
  
}
