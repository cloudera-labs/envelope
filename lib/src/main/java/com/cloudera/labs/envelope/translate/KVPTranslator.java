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

package com.cloudera.labs.envelope.translate;

import com.cloudera.labs.envelope.component.ComponentFactory;
import com.cloudera.labs.envelope.component.InstantiatedComponent;
import com.cloudera.labs.envelope.component.InstantiatesComponents;
import com.cloudera.labs.envelope.component.ProvidesAlias;
import com.cloudera.labs.envelope.schema.Schema;
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.cloudera.labs.envelope.utils.SchemaUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.SupportedFieldTypesValidation;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A translator implementation for text key-value pair messages.
 */
public class KVPTranslator implements Translator, ProvidesAlias, ProvidesValidations,
    InstantiatesComponents {

  private String kvpDelimiter;
  private String fieldDelimiter;
  private StructType schema;
  private List<Object> values = Lists.newArrayList();
  private Map<String, String> kvpMap = new HashMap<>();
  private Map<RowUtils.RowValueMetadata,Object> rowValueMetadata = new HashMap<>(); 

  public static final String KVP_DELIMITER_CONFIG_NAME = "delimiter.kvp";
  public static final String FIELD_DELIMITER_CONFIG_NAME = "delimiter.field";
  public static final String TIMESTAMP_FORMAT_CONFIG_NAME = "timestamp.formats";
  public static final String SCHEMA_CONFIG = "schema"; 

  @Override
  public void configure(Config config) {
    kvpDelimiter = resolveDelimiter(config.getString(KVP_DELIMITER_CONFIG_NAME));
    fieldDelimiter = resolveDelimiter(config.getString(FIELD_DELIMITER_CONFIG_NAME));
    schema = ComponentFactory.create(Schema.class, config.getConfig(SCHEMA_CONFIG), true).getSchema();

    if (config.hasPath(TIMESTAMP_FORMAT_CONFIG_NAME)) {
      rowValueMetadata.put(RowUtils.RowValueMetadata.TIMESTAMP_FORMATS,
                           new HashSet<String>(config.getStringList(TIMESTAMP_FORMAT_CONFIG_NAME)));
    }
  }

  @Override
  public Iterable<Row> translate(Row message) {
    String value = message.getAs(Translator.VALUE_FIELD_NAME);

    kvpMap.clear();
    values.clear();

    String[] kvps = value.split(Pattern.quote(kvpDelimiter));
    for (String kvp : kvps) {
      String[] components = kvp.split(Pattern.quote(fieldDelimiter));
      String kvpKey = components[0];
      String kvpValue = components.length == 2 ? components[1] : null;

      kvpMap.put(kvpKey, kvpValue);
    }

    for (int fieldPos = 0; fieldPos < schema.length(); fieldPos++) {
      String fieldName = schema.fieldNames()[fieldPos];
      DataType fieldType = schema.fields()[fieldPos].dataType();

      Object rowVal = null;
      if (kvpMap.containsKey(fieldName)) {
        String kvpValue = kvpMap.get(fieldName);
        if (kvpValue != null) {
          rowVal = RowUtils.toRowValue(kvpValue, fieldType, rowValueMetadata);
        }
      }
      values.add(rowVal);
    }

    Row row = new RowWithSchema(schema, values.toArray());

    return Collections.singleton(row);
  }

  @Override
  public StructType getExpectingSchema() {
    return SchemaUtils.stringValueSchema();
  }

  @Override
  public StructType getProvidingSchema() {
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
        .mandatoryPath(SCHEMA_CONFIG, ConfigValueType.OBJECT)
        .add(new SupportedFieldTypesValidation(SCHEMA_CONFIG, 
            new HashSet<DataType>(Arrays.asList(new DecimalType(),    DataTypes.StringType,
                                                DataTypes.FloatType,  DataTypes.DoubleType,
                                                DataTypes.ShortType,  DataTypes.IntegerType,
                                                DataTypes.LongType,   DataTypes.BooleanType,
                                                DataTypes.BinaryType, DataTypes.DateType,
                                                DataTypes.TimestampType))))
        .optionalPath(TIMESTAMP_FORMAT_CONFIG_NAME, ConfigValueType.LIST)
        .handlesOwnValidationPath(SCHEMA_CONFIG)
        .build();
  }

  @Override
  public Set<InstantiatedComponent> getComponents(Config config, boolean configure) {
    return SchemaUtils.getSchemaComponents(config, configure, SCHEMA_CONFIG);
  }

}
