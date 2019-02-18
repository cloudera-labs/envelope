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

package com.cloudera.labs.envelope.schema;

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.validate.FlatSchemaValidation;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;

public class FlatSchema implements Schema, ProvidesAlias, ProvidesValidations {

  public static final String FIELD_NAMES_CONFIG = "field.names";
  public static final String FIELD_TYPES_CONFIG = "field.types";

  private StructType schema;

  @Override
  public void configure(Config config) {
    List<String> names = config.getStringList(FIELD_NAMES_CONFIG);
    List<String> types = config.getStringList(FIELD_TYPES_CONFIG);
    List<StructField> fields = Lists.newArrayList();
    
    for (int fieldNum = 0; fieldNum < names.size(); fieldNum++) {
      fields.add(DataTypes.createStructField(names.get(fieldNum),
        ConfigurationDataTypes.getSparkDataType(types.get(fieldNum)), true));
    }
    this.schema = DataTypes.createStructType(fields);
  }

  @Override
  public String getAlias() {
    return "flat";
  }

  @Override
  public StructType getSchema() {
    return schema;
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(FIELD_NAMES_CONFIG, ConfigValueType.LIST)
        .mandatoryPath(FIELD_TYPES_CONFIG, ConfigValueType.LIST)
        .add(new FlatSchemaValidation(FIELD_NAMES_CONFIG, FIELD_TYPES_CONFIG))
        .build();
  }
  
}
