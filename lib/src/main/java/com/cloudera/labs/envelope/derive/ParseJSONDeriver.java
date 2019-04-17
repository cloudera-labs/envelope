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

package com.cloudera.labs.envelope.derive;

import com.cloudera.labs.envelope.component.ComponentFactory;
import com.cloudera.labs.envelope.component.InstantiatedComponent;
import com.cloudera.labs.envelope.component.InstantiatesComponents;
import com.cloudera.labs.envelope.component.ProvidesAlias;
import com.cloudera.labs.envelope.schema.Schema;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.utils.SchemaUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Map;
import java.util.Set;

public class ParseJSONDeriver implements Deriver, ProvidesAlias, ProvidesValidations, InstantiatesComponents {

  public static final String STEP_NAME_CONFIG = "step";
  public static final String FIELD_NAME_CONFIG = "field";
  public static final String AS_STRUCT_CONFIG = "as-struct";
  public static final String STRUCT_FIELD_NAME_CONFIG = "struct-field";
  public static final String SCHEMA_CONFIG = "schema";
  public static final String OPTION_CONFIG_PREFIX = "option";

  private String stepName;
  private String fieldName;
  private boolean asStruct;
  private String structFieldName;
  private StructType schema;
  private Map<String, String> options;

  @Override
  public void configure(Config config) {
    this.stepName = config.getString(STEP_NAME_CONFIG);
    this.fieldName = config.getString(FIELD_NAME_CONFIG);
    this.asStruct = ConfigUtils.getOrElse(config, AS_STRUCT_CONFIG, false);
    if (this.asStruct) {
      this.structFieldName = config.getString(STRUCT_FIELD_NAME_CONFIG);
    }
    this.schema = ComponentFactory.create(Schema.class, config.getConfig(SCHEMA_CONFIG), true).getSchema();
    this.options = extractOptions(config);
  }

  @Override
  public Dataset<Row> derive(Map<String, Dataset<Row>> dependencies) {
    String parsedStructTemporaryFieldName = "__parsed_json";

    Dataset<Row> dependency = dependencies.get(stepName);

    Dataset<Row> parsed = dependency.select(
        functions.from_json(new Column(fieldName), schema, options).as(parsedStructTemporaryFieldName));

    if (asStruct) {
      return parsed.withColumnRenamed(parsedStructTemporaryFieldName, structFieldName);
    }
    else {
      for (StructField parsedField : schema.fields()) {
        parsed = parsed.withColumn(
            parsedField.name(), new Column(parsedStructTemporaryFieldName + "." + parsedField.name()));
      }

      return parsed.drop(parsedStructTemporaryFieldName);
    }
  }

  private Map<String, String> extractOptions(Config config) {
    Map<String, String> options = Maps.newHashMap();

    if (config.hasPath(OPTION_CONFIG_PREFIX)) {
      Config optionsConfig = config.getConfig(OPTION_CONFIG_PREFIX);

      for (String key : optionsConfig.root().keySet()) {
        options.put(key, optionsConfig.getAnyRef(key).toString());
      }
    }

    return options;
  }

  @Override
  public String getAlias() {
    return "parse-json";
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(STEP_NAME_CONFIG, ConfigValueType.STRING)
        .mandatoryPath(FIELD_NAME_CONFIG, ConfigValueType.STRING)
        .mandatoryPath(SCHEMA_CONFIG, ConfigValueType.OBJECT)
        .optionalPath(AS_STRUCT_CONFIG, ConfigValueType.BOOLEAN)
        .ifPathHasValue(AS_STRUCT_CONFIG, true,
            Validations.single().mandatoryPath(STRUCT_FIELD_NAME_CONFIG, ConfigValueType.STRING))
        .handlesOwnValidationPath(SCHEMA_CONFIG)
        .optionalPath(OPTION_CONFIG_PREFIX, ConfigValueType.OBJECT)
        .handlesOwnValidationPath(OPTION_CONFIG_PREFIX)
        .build();
  }

  @Override
  public Set<InstantiatedComponent> getComponents(Config config, boolean configure) {
    return SchemaUtils.getSchemaComponents(config, configure, SCHEMA_CONFIG);
  }

}
