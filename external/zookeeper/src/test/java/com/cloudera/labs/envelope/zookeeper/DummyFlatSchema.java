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

package com.cloudera.labs.envelope.zookeeper;

import com.cloudera.labs.envelope.schema.ConfigurationDataTypes;
import com.cloudera.labs.envelope.schema.Schema;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import java.util.List;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DummyFlatSchema implements Schema, ProvidesValidations {
  private StructType schema;
  public static final String DUMMY_FLAT_FIELDS_CONFIG = "field.names";
  public static final String DUMMY_FLAT_TYPES_CONFIG = "field.types";

  @Override
  public void configure(Config config) {
    List<String> names = config.getStringList(DUMMY_FLAT_FIELDS_CONFIG);
    List<String> types = config.getStringList(DUMMY_FLAT_TYPES_CONFIG);
    List<StructField> fields = Lists.newArrayList();
    for (int fieldNum = 0; fieldNum < names.size(); fieldNum++)
    {
      fields.add(DataTypes.createStructField(names.get(fieldNum),
                 ConfigurationDataTypes.getSparkDataType(types.get(fieldNum)),
                 true));
    }
    this.schema = DataTypes.createStructType(fields);
  }

  @Override
  public StructType getSchema() {
    return schema;
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(DUMMY_FLAT_FIELDS_CONFIG, ConfigValueType.LIST)
        .mandatoryPath(DUMMY_FLAT_TYPES_CONFIG, ConfigValueType.LIST)
        .build();
  }
}
