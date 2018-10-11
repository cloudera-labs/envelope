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

package com.cloudera.labs.envelope.derive.dq;

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validation;
import com.cloudera.labs.envelope.validate.ValidationResult;
import com.cloudera.labs.envelope.validate.Validations;
import com.cloudera.labs.envelope.validate.Validity;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CheckSchemaDatasetRule implements DatasetRule, ProvidesAlias, ProvidesValidations {

  public static final String FIELDS_CONFIG = "fields";
  public static final String FIELD_NAME_CONFIG = "name";
  public static final String FIELD_TYPE_CONFIG = "type";
  public static final String DECIMAL_SCALE_CONFIG = "scale";
  public static final String DECIMAL_PRECISION_CONFIG = "precision";
  public static final String EXACT_MATCH_CONFIG = "exactmatch";

  private static final boolean DEFAULT_EXACT_MATCH = false;

  private String name;
  private StructType requiredSchema;
  private boolean exactMatch;

  @Override
  public void configure(String name, Config config) {
    this.name = name;
    List<? extends ConfigObject> fields = config.getObjectList(FIELDS_CONFIG);
    requiredSchema = parseSchema(fields);
    exactMatch = ConfigUtils.getOrElse(config, EXACT_MATCH_CONFIG, DEFAULT_EXACT_MATCH);
  }

  @Override
  public Dataset<Row> check(Dataset<Row> dataset, Map<String, Dataset<Row>> stepDependencies) {
    boolean schemasMatch = schemasMatch(requiredSchema, dataset.schema(), exactMatch);
    List<Row> datasetRows = Lists.newArrayList((Row)new RowWithSchema(SCHEMA, name, schemasMatch));
    return Contexts.getSparkSession().createDataFrame(datasetRows, SCHEMA);
  }

  private static StructType parseSchema(List<? extends ConfigObject> fieldsConfig) {
    StructField[] fields = new StructField[fieldsConfig.size()];
    for (int i = 0; i < fieldsConfig.size(); i++) {
      fields[i] = parseField(fieldsConfig.get(i).toConfig());
    }
    return new StructType(fields);
  }

  private static StructField parseField(Config fieldsConfig) {
    String name = fieldsConfig.getString(FIELD_NAME_CONFIG);
    DataType type = parseDataType(fieldsConfig);
    return new StructField(name, type, true, Metadata.empty());
  }

  private static DataType parseDataType(Config fieldsConfig) {
    String type = fieldsConfig.getString(FIELD_TYPE_CONFIG);
    switch (type) {
      case "string":
        return DataTypes.StringType;
      case "byte":
        return DataTypes.ByteType;
      case "short":
        return DataTypes.ShortType;
      case "int":
        return DataTypes.IntegerType;
      case "long":
        return DataTypes.LongType;
      case "float":
        return DataTypes.FloatType;
      case "double":
        return DataTypes.DoubleType;
      case "decimal":
        return DataTypes.createDecimalType(
                fieldsConfig.getInt(DECIMAL_SCALE_CONFIG),
                fieldsConfig.getInt(DECIMAL_PRECISION_CONFIG));
      case "boolean":
        return DataTypes.BooleanType;
      case "binary":
        return DataTypes.BinaryType;
      case "date":
        return DataTypes.DateType;
      case "timestamp":
        return DataTypes.TimestampType;
      case "array":
      case "map":
      case "struct":
        throw new RuntimeException("Schema check does not currently support complex types");
      default:
        throw new RuntimeException("Unknown type: " + type);
    }
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
        .mandatoryPath(FIELDS_CONFIG, ConfigValueType.LIST)
        .add(new SchemaFieldsValidation())
        .optionalPath(EXACT_MATCH_CONFIG, ConfigValueType.BOOLEAN)
        .build();
  }

  private static class SchemaFieldsValidation implements Validation {
    @Override
    public ValidationResult validate(Config config) {
      try {
        parseSchema(config.getObjectList(FIELDS_CONFIG));
      }
      catch (Exception e) {
        return new ValidationResult(this, Validity.INVALID, "Schema configuration is invalid");
      }

      return new ValidationResult(this, Validity.VALID, "Schema configuration is valid");
    }

    @Override
    public Set<String> getKnownPaths() {
      return Sets.newHashSet(FIELDS_CONFIG);
    }
  }
  
}
