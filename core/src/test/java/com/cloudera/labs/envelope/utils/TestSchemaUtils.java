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

package com.cloudera.labs.envelope.utils;

import com.google.common.collect.Lists;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestSchemaUtils {

  @Test
  public void testSubsetSchemaSomeFields() {
    StructField field1 = DataTypes.createStructField("field1", DataTypes.StringType, true);
    StructField field2 = DataTypes.createStructField("field2", DataTypes.IntegerType, true);
    StructField field3 = DataTypes.createStructField("field3", DataTypes.FloatType, true);
    StructType schema = DataTypes.createStructType(Lists.newArrayList(field1, field2, field3));

    StructType subset = SchemaUtils.subsetSchema(schema, Lists.newArrayList("field1", "field3"));

    assertEquals(subset.fields().length, 2);
    assertEquals(subset.fields()[0].name(), "field1");
    assertEquals(subset.fields()[1].name(), "field3");
  }

  @Test
  public void testSubsetSchemaAllFields() {
    StructField field1 = DataTypes.createStructField("field1", DataTypes.StringType, true);
    StructField field2 = DataTypes.createStructField("field2", DataTypes.IntegerType, true);
    StructField field3 = DataTypes.createStructField("field3", DataTypes.FloatType, true);
    StructType schema = DataTypes.createStructType(Lists.newArrayList(field1, field2, field3));

    StructType subset = SchemaUtils.subsetSchema(schema, Lists.newArrayList("field1", "field2", "field3"));

    assertEquals(subset.fields().length, 3);
    assertEquals(subset.fields()[0].name(), "field1");
    assertEquals(subset.fields()[1].name(), "field2");
    assertEquals(subset.fields()[2].name(), "field3");
  }

  @Test
  public void testSubsetSchemaNoFields() {
    StructField field1 = DataTypes.createStructField("field1", DataTypes.StringType, true);
    StructField field2 = DataTypes.createStructField("field2", DataTypes.IntegerType, true);
    StructField field3 = DataTypes.createStructField("field3", DataTypes.FloatType, true);
    StructType schema = DataTypes.createStructType(Lists.newArrayList(field1, field2, field3));

    StructType subset = SchemaUtils.subsetSchema(schema, Lists.<String>newArrayList());

    assertEquals(subset.fields().length, 0);
  }

  @Test
  public void testSubtractSchemaSomeFields() {
    StructField field1 = DataTypes.createStructField("field1", DataTypes.StringType, true);
    StructField field2 = DataTypes.createStructField("field2", DataTypes.IntegerType, true);
    StructField field3 = DataTypes.createStructField("field3", DataTypes.FloatType, true);
    StructType schema = DataTypes.createStructType(Lists.newArrayList(field1, field2, field3));

    StructType subset = SchemaUtils.subtractSchema(schema, Lists.newArrayList("field1", "field3"));

    assertEquals(subset.fields().length, 1);
    assertEquals(subset.fields()[0].name(), "field2");
  }

  @Test
  public void testSubtractSchemaAllFields() {
    StructField field1 = DataTypes.createStructField("field1", DataTypes.StringType, true);
    StructField field2 = DataTypes.createStructField("field2", DataTypes.IntegerType, true);
    StructField field3 = DataTypes.createStructField("field3", DataTypes.FloatType, true);
    StructType schema = DataTypes.createStructType(Lists.newArrayList(field1, field2, field3));

    StructType subset = SchemaUtils.subtractSchema(schema, Lists.newArrayList("field1", "field2", "field3"));

    assertEquals(subset.fields().length, 0);
  }

  @Test
  public void testSubtractSchemaNoFields() {
    StructField field1 = DataTypes.createStructField("field1", DataTypes.StringType, true);
    StructField field2 = DataTypes.createStructField("field2", DataTypes.IntegerType, true);
    StructField field3 = DataTypes.createStructField("field3", DataTypes.FloatType, true);
    StructType schema = DataTypes.createStructType(Lists.newArrayList(field1, field2, field3));

    StructType subset = SchemaUtils.subtractSchema(schema, Lists.<String>newArrayList());

    assertEquals(subset.fields().length, 3);
    assertEquals(subset.fields()[0].name(), "field1");
    assertEquals(subset.fields()[1].name(), "field2");
    assertEquals(subset.fields()[2].name(), "field3");
  }

  @Test
  public void testAppendFields() {
    StructField field1 = DataTypes.createStructField("field1", DataTypes.StringType, true);
    StructField field2 = DataTypes.createStructField("field2", DataTypes.IntegerType, true);
    StructField field3 = DataTypes.createStructField("field3", DataTypes.FloatType, true);
    StructType schema = DataTypes.createStructType(Lists.newArrayList(field1, field2, field3));
    StructField field4 = DataTypes.createStructField("field4", DataTypes.BooleanType, true);
    StructField field5 = DataTypes.createStructField("field5", DataTypes.StringType, true);

    StructType appendSchema = SchemaUtils.appendFields(schema, Lists.newArrayList(field4, field5));

    assertEquals(appendSchema.length(), 5);
    assertEquals(appendSchema.fields()[0], field1);
    assertEquals(appendSchema.fields()[1], field2);
    assertEquals(appendSchema.fields()[2], field3);
    assertEquals(appendSchema.fields()[3], field4);
    assertEquals(appendSchema.fields()[4], field5);
  }

  @Test
  public void testStructTypeFor() {
    List<String> fieldNames = Lists.newArrayList("field1", "field2", "field3", "field4", "field5", "field6");
    List<String> fieldTypes = Lists.newArrayList("string", "float", "double", "int", "long", "boolean");

    StructType structFromRowUtils = SchemaUtils.structTypeFor(fieldNames, fieldTypes);

    StructField field1 = DataTypes.createStructField("field1", DataTypes.StringType, true);
    StructField field2 = DataTypes.createStructField("field2", DataTypes.FloatType, true);
    StructField field3 = DataTypes.createStructField("field3", DataTypes.DoubleType, true);
    StructField field4 = DataTypes.createStructField("field4", DataTypes.IntegerType, true);
    StructField field5 = DataTypes.createStructField("field5", DataTypes.LongType, true);
    StructField field6 = DataTypes.createStructField("field6", DataTypes.BooleanType, true);
    StructType structFromAPI = DataTypes.createStructType(Lists.newArrayList(field1, field2, field3, field4, field5, field6));

    assertEquals(structFromRowUtils, structFromAPI);
  }


}
