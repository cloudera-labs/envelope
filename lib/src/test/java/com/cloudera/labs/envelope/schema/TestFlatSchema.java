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

import com.cloudera.labs.envelope.component.ComponentFactory;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.cloudera.labs.envelope.validate.ValidationAssert.assertNoValidationFailures;
import static com.cloudera.labs.envelope.validate.ValidationAssert.assertValidationFailures;
import static org.junit.Assert.assertEquals;

public class TestFlatSchema {

  private Config config = ConfigFactory.empty();

  @Test
  public void validSchema() {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(ComponentFactory.TYPE_CONFIG_NAME, "flat");
    paramMap.put(FlatSchema.FIELD_NAMES_CONFIG, Lists.newArrayList(
        "decimalField", "stringField", "floatField", "doubleField", "byteField",
        "shortField", "integerField", "longField", "booleanField",
        "binaryField", "dateField", "timestampField", "decimalField2"));
    paramMap.put(FlatSchema.FIELD_TYPES_CONFIG, Lists.newArrayList(
        "decimal(10,0)", "string", "float", "double", "byte", "short", "integer",
        "long", "boolean", "binary", "date", "timestamp", " decimal ( 38 , 38 )"));
    config = ConfigFactory.parseMap(paramMap);
    FlatSchema flatSchema = new FlatSchema(); 
    assertNoValidationFailures(flatSchema, config);
    flatSchema.configure(config);
    StructType schema = flatSchema.getSchema();
    assertEquals(schema.fields().length, 13);
    assertEquals(schema.fields()[0].name(), "decimalField");
    assertEquals(schema.fields()[1].name(), "stringField");
    assertEquals(schema.fields()[2].name(), "floatField");
    assertEquals(schema.fields()[3].name(), "doubleField");
    assertEquals(schema.fields()[4].name(), "byteField");
    assertEquals(schema.fields()[5].name(), "shortField");
    assertEquals(schema.fields()[6].name(), "integerField");
    assertEquals(schema.fields()[7].name(), "longField");
    assertEquals(schema.fields()[8].name(), "booleanField");
    assertEquals(schema.fields()[9].name(), "binaryField");
    assertEquals(schema.fields()[10].name(), "dateField");
    assertEquals(schema.fields()[11].name(), "timestampField");
    assertEquals(schema.fields()[12].name(), "decimalField2");
    assertEquals(schema.fields()[0].dataType(), new DecimalType(10,0));
    assertEquals(schema.fields()[1].dataType(), DataTypes.StringType);
    assertEquals(schema.fields()[2].dataType(), DataTypes.FloatType);
    assertEquals(schema.fields()[3].dataType(), DataTypes.DoubleType);
    assertEquals(schema.fields()[4].dataType(), DataTypes.ByteType);
    assertEquals(schema.fields()[5].dataType(), DataTypes.ShortType);
    assertEquals(schema.fields()[6].dataType(), DataTypes.IntegerType);
    assertEquals(schema.fields()[7].dataType(), DataTypes.LongType);
    assertEquals(schema.fields()[8].dataType(), DataTypes.BooleanType);
    assertEquals(schema.fields()[9].dataType(), DataTypes.BinaryType);
    assertEquals(schema.fields()[10].dataType(), DataTypes.DateType);
    assertEquals(schema.fields()[11].dataType(), DataTypes.TimestampType);
    assertEquals(schema.fields()[12].dataType(), new DecimalType(38,38));
  }

  @Test
  public void missingConfig() {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(ComponentFactory.TYPE_CONFIG_NAME, "flat");
    config = ConfigFactory.parseMap(paramMap);
    FlatSchema flatSchema = new FlatSchema(); 
    assertValidationFailures(flatSchema, config);
  }

  @Test
  public void missingNames() {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(ComponentFactory.TYPE_CONFIG_NAME, "flat");
    paramMap.put(FlatSchema.FIELD_TYPES_CONFIG, Lists.newArrayList("long", "integer"));
    config = ConfigFactory.parseMap(paramMap);
    FlatSchema flatSchema = new FlatSchema(); 
    assertValidationFailures(flatSchema, config);
  }

  @Test
  public void missingTypes() {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(ComponentFactory.TYPE_CONFIG_NAME, "flat");
    paramMap.put(FlatSchema.FIELD_NAMES_CONFIG, Lists.newArrayList("A Long", "An Int"));
    config = ConfigFactory.parseMap(paramMap);
    FlatSchema flatSchema = new FlatSchema(); 
    assertValidationFailures(flatSchema, config);
  }

}
