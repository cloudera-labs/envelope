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

package com.cloudera.labs.envelope.validate;

import com.cloudera.labs.envelope.component.ComponentFactory;
import com.cloudera.labs.envelope.schema.FlatSchema;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;

public class TestSupportedFieldTypesValidation {

  public static String SCHEMA_CONFIG = "schema";

  Config config = ConfigFactory.empty()
      .withValue(SCHEMA_CONFIG + "." + ComponentFactory.TYPE_CONFIG_NAME,
          ConfigValueFactory.fromAnyRef("flat"))
      .withValue(SCHEMA_CONFIG + "." + FlatSchema.FIELD_NAMES_CONFIG,
          ConfigValueFactory.fromIterable(
              Lists.newArrayList("field1", "field2", "field3", "field4","field5")))
      .withValue(SCHEMA_CONFIG + "." + FlatSchema.FIELD_TYPES_CONFIG,
          ConfigValueFactory.fromIterable(
              Lists.newArrayList("binary", "integer", "string", "decimal(14,7)", "timestamp")));

  @Test
  public void testValid() {
    SupportedFieldTypesValidation v = new SupportedFieldTypesValidation(SCHEMA_CONFIG,
      new HashSet<DataType>(Arrays.asList(DataTypes.IntegerType, DataTypes.StringType, new DecimalType(),
                                          DataTypes.TimestampType, DataTypes.BinaryType)));
    ValidationResult vr = v.validate(config);
    assertEquals(vr.getValidity(), Validity.VALID);
  }

  @Test
  public void testInvalid() {
    SupportedFieldTypesValidation v = new SupportedFieldTypesValidation(SCHEMA_CONFIG,
      new HashSet<DataType>(Arrays.asList(DataTypes.IntegerType, new DecimalType(),
                                          DataTypes.TimestampType, DataTypes.BinaryType)));
    ValidationResult vr = v.validate(config);
    assertEquals(vr.getValidity(), Validity.INVALID);
  }
}
