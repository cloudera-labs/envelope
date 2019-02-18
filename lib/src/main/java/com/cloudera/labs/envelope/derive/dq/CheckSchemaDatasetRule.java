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

package com.cloudera.labs.envelope.derive.dq;

import com.cloudera.labs.envelope.component.InstantiatedComponent;
import com.cloudera.labs.envelope.component.InstantiatesComponents;
import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.schema.SchemaFactory;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.utils.SchemaUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CheckSchemaDatasetRule implements DatasetRule, ProvidesAlias, ProvidesValidations,  InstantiatesComponents {

  public static final String SCHEMA_CONFIG = "schema";
  private static final boolean DEFAULT_EXACT_MATCH = false;
  public static final String EXACT_MATCH_CONFIG = "exactmatch";

  private String name;
  private StructType requiredSchema;
  private boolean exactMatch;

  @Override
  public void configure(String name, Config config) {
    this.name = name;
    requiredSchema = SchemaFactory.create(config.getConfig(SCHEMA_CONFIG), true).getSchema();
    exactMatch = ConfigUtils.getOrElse(config, EXACT_MATCH_CONFIG, DEFAULT_EXACT_MATCH);
  }

  @Override
  public Dataset<Row> check(Dataset<Row> dataset, Map<String, Dataset<Row>> stepDependencies) {
    boolean schemasMatch = schemasMatch(requiredSchema, dataset.schema(), exactMatch);
    List<Row> datasetRows = Lists.newArrayList((Row)new RowWithSchema(SCHEMA, name, schemasMatch));
    return Contexts.getSparkSession().createDataFrame(datasetRows, SCHEMA);
  }

  private static boolean schemasMatch(StructType requiredSchema, StructType actualSchema, boolean exactMatch) {
    Map<String, DataType> requiredFields = toFieldTypeMap(requiredSchema.fields());
    Map<String, DataType> actualFields = toFieldTypeMap(actualSchema.fields());

    if (!Sets.difference(requiredFields.keySet(), actualFields.keySet()).isEmpty()) {
      // Actual fields does not contain all of the required field names
      return false;
    }

    if (exactMatch && requiredFields.size() != actualFields.size()) {
      // if we need an exact match and the numbers of fields are different
      return false;
    }

    // Check each of the field types are correct
    for (Map.Entry<String, DataType> requiredField : requiredFields.entrySet()) {
      DataType actualType = actualFields.get(requiredField.getKey());
      if (actualType != requiredField.getValue()) {
        return false;
      }
    }

    return true;
  }

  private static Map<String, DataType> toFieldTypeMap(StructField[] fields) {
    Map<String, DataType> map = new HashMap<>();
    for (StructField field : fields) {
      map.put(field.name(), field.dataType());
    }
    return map;
  }

  @Override
  public String getAlias() {
    return "checkschema";
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(SCHEMA_CONFIG, ConfigValueType.OBJECT)
        .optionalPath(EXACT_MATCH_CONFIG, ConfigValueType.BOOLEAN)
        .handlesOwnValidationPath(SCHEMA_CONFIG)
        .build();
  }

  @Override
  public Set<InstantiatedComponent> getComponents(Config config, boolean configure) {
    return SchemaUtils.getSchemaComponents(config, configure, SCHEMA_CONFIG);
  }

}
