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
import mockit.integration.junit4.JMockit;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 *
 */
@RunWith(JMockit.class)
public class TestAvroUtils {

  @Test
  public void toSchemaNullable() throws Exception {

    StructType input = DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("field1", DataTypes.BooleanType, false),
        DataTypes.createStructField("field2", DataTypes.StringType, true),
        DataTypes.createStructField("field3", DataTypes.DateType, false),
        DataTypes.createStructField("field4", DataTypes.TimestampType, false)
    ));

    Schema schema = AvroUtils.schemaFor(input);

    assertEquals("Invalid field count", 4, schema.getFields().size());

    // Not nullable
    assertEquals("Invalid field name", "field1", schema.getFields().get(0).name());
    assertEquals("Invalid field type", Schema.Type.BOOLEAN, schema.getFields().get(0).schema().getType());
    assertEquals("Invalid field default", null, schema.getFields().get(0).defaultVal());

    // Nullable, as opposed to Optional, Avro construct, but no default
    assertEquals("Invalid nullable (union) type", Schema.Type.UNION, schema.getFields().get(1).schema().getType());
    assertEquals("Invalid nullable (union) type count", 2, schema.getFields().get(1).schema().getTypes().size());
    assertEquals("Invalid field type", Schema.Type.STRING, schema.getFields().get(1).schema().getTypes().get(0).getType());
    assertEquals("Invalid union default", null, schema.getFields().get(1).defaultVal());

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toTypeSchemaArrayNullable() throws Exception {
    Schema schema = AvroUtils.typeFor(DataTypes.createArrayType(DataTypes.IntegerType, false));

    assertEquals("Invalid type", Schema.Type.UNION, schema.getType());
    assertEquals("Invalid union size", 2, schema.getTypes().size());

    for (Schema s : schema.getTypes()) {
      assertThat("Invalid union types", s.getType(), anyOf(is(Schema.Type.ARRAY), is(Schema.Type.NULL)));
      if (s.getType().equals(Schema.Type.ARRAY)) {
        assertEquals("Invalid element type", Schema.Type.INT, s.getElementType().getType());
      }
    }

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toTypeSchemaArrayNotNullable() throws Exception {
    Schema schema = AvroUtils.typeFor(DataTypes.createArrayType(DataTypes.IntegerType, false),false);

    assertEquals("Invalid type", Schema.Type.ARRAY, schema.getType());
    assertEquals("Invalid element type", Schema.Type.INT, schema.getElementType().getType());

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toTypeSchemaArrayContainsNull() throws Exception {
    Schema schema = AvroUtils.typeFor(DataTypes.createArrayType(DataTypes.IntegerType, true), false);

    assertEquals("Invalid type", Schema.Type.ARRAY, schema.getType());
    assertEquals("Invalid element type", Schema.Type.UNION, schema.getElementType().getType());
    assertEquals("Invalid union count", 2, schema.getElementType().getTypes().size());

    for (Schema s : schema.getElementType().getTypes()) {
      assertThat("Invalid union types", s.getType(), anyOf(is(Schema.Type.INT), is(Schema.Type.NULL)));
    }

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toSchemaArraysNested() throws Exception {

    StructType input = DataTypes.createStructType(Lists.newArrayList(
        // Outer
        DataTypes.createStructField("Outer", DataTypes.createArrayType(
            // Inner
            DataTypes.createArrayType(DataTypes.IntegerType, false),
            false), false)
    ));

    Schema schema = AvroUtils.schemaFor(input);

    assertEquals("Invalid field count", 1, schema.getFields().size());
    assertEquals("Invalid field name", "Outer", schema.getFields().get(0).name());
    assertEquals("Invalid field type", Schema.Type.ARRAY, schema.getFields().get(0).schema().getType());
    assertEquals("Invalid outer element type, i.e the inner type", Schema.Type.ARRAY, schema.getFields().get(0).schema().getElementType().getType());
    assertEquals("Invalid inner element type", Schema.Type.INT, schema.getFields().get(0).schema().getElementType().getElementType().getType());

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toTypeSchemaMapsNullable() throws Exception {
    Schema schema = AvroUtils.typeFor(DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, false));

    assertEquals("Invalid type", Schema.Type.UNION, schema.getType());
    assertEquals("Invalid union size", 2, schema.getTypes().size());

    for (Schema s : schema.getTypes()) {
      assertThat("Invalid union types", s.getType(), anyOf(is(Schema.Type.MAP), is(Schema.Type.NULL)));
      if (s.getType().equals(Schema.Type.MAP)) {
        assertEquals("Invalid value type", Schema.Type.INT, s.getValueType().getType());
      }
    }

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toTypeSchemaMapNotNullable() throws Exception {
    Schema schema = AvroUtils.typeFor(DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, false),false);

    assertEquals("Invalid type", Schema.Type.MAP, schema.getType());
    assertEquals("Invalid value type", Schema.Type.INT, schema.getValueType().getType());

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toTypeSchemaMapContainsNull() throws Exception {
    Schema schema = AvroUtils.typeFor(DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, true),false);

    assertEquals("Invalid type", Schema.Type.MAP, schema.getType());
    assertEquals("Invalid value type", Schema.Type.UNION, schema.getValueType().getType());
    assertEquals("Invalid union count", 2, schema.getValueType().getTypes().size());

    for (Schema s : schema.getValueType().getTypes()) {
      assertThat("Invalid union types", s.getType(), anyOf(is(Schema.Type.INT), is(Schema.Type.NULL)));
    }

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toTypeSchemaMapNonStringKey() {
    Schema schema = AvroUtils.typeFor(DataTypes.createMapType(DataTypes.FloatType, DataTypes.IntegerType, false),false);

    // Ignores the key types -- in conversion, will need to ensure keys can render to Strings
    assertEquals("Invalid type", Schema.Type.MAP, schema.getType());
    assertEquals("Invalid value type", Schema.Type.INT, schema.getValueType().getType());

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toSchemaMapsNested() throws Exception {

    StructType input = DataTypes.createStructType(Lists.newArrayList(
        // Outer
        DataTypes.createStructField("Outer", DataTypes.createMapType(DataTypes.StringType,
            // Inner
            DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, false),
            false), false)
    ));

    Schema schema = AvroUtils.schemaFor(input);

    assertEquals("Invalid field count", 1, schema.getFields().size());
    assertEquals("Invalid field name", "Outer", schema.getFields().get(0).name());
    assertEquals("Invalid field type", Schema.Type.MAP, schema.getFields().get(0).schema().getType());
    assertEquals("Invalid outer value type, i.e the inner type", Schema.Type.MAP, schema.getFields().get(0).schema().getValueType().getType());
    assertEquals("Invalid inner value type", Schema.Type.INT, schema.getFields().get(0).schema().getValueType().getValueType().getType());

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toTypeSchemaStructTypeNullable() throws Exception {
    Schema schema = AvroUtils.typeFor(DataTypes.createStructType(
        Lists.newArrayList(
            DataTypes.createStructField("field1", DataTypes.StringType, false)
        )),
        true);

    assertEquals("Invalid type", Schema.Type.UNION, schema.getType());
    assertEquals("Invalid union size", 2, schema.getTypes().size());

    for (Schema s : schema.getTypes()) {
      assertThat("Invalid union types", s.getType(), anyOf(is(Schema.Type.RECORD), is(Schema.Type.NULL)));
      if (s.getType().equals(Schema.Type.RECORD)) {
        assertEquals("Invalid record name", "record0", s.getName());
        assertEquals("Invalid field count", 1, s.getFields().size());
        assertEquals("Invalid field name", "field1", s.getFields().get(0).name());
        assertEquals("Invalid field type", Schema.Type.STRING, s.getFields().get(0).schema().getType());
      }
    }

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toTypeSchemaStructTypeNotNullable() throws Exception {
    Schema schema = AvroUtils.typeFor(DataTypes.createStructType(
        Lists.newArrayList(
            DataTypes.createStructField("field1", DataTypes.StringType, false)
        )),
        false);

    assertEquals("Invalid type", Schema.Type.RECORD, schema.getType());
    assertEquals("Invalid record name", "record0", schema.getName());
    assertEquals("Invalid field count", 1, schema.getFields().size());
    assertEquals("Invalid field name", "field1", schema.getFields().get(0).name());
    assertEquals("Invalid field type", Schema.Type.STRING, schema.getFields().get(0).schema().getType());

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toTypeSchemaStructTypeFieldNullable() throws Exception {
    Schema schema = AvroUtils.typeFor(DataTypes.createStructType(
        Lists.newArrayList(
            DataTypes.createStructField("field1", DataTypes.StringType, true)
        )),
        false);

    assertEquals("Invalid type", Schema.Type.RECORD, schema.getType());
    assertEquals("Invalid record name", "record0", schema.getName());
    assertEquals("Invalid field count", 1, schema.getFields().size());
    assertEquals("Invalid field name", "field1", schema.getFields().get(0).name());
    assertEquals("Invalid field type", Schema.Type.UNION, schema.getFields().get(0).schema().getType());

    for (Schema s : schema.getFields().get(0).schema().getTypes()) {
      assertThat("Invalid union types", s.getType(), anyOf(is(Schema.Type.STRING), is(Schema.Type.NULL)));
    }

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toSchemaStructTypesNested() throws Exception {

    StructType input = DataTypes.createStructType(Lists.newArrayList(
        // Outer
        DataTypes.createStructField("Outer", DataTypes.createStructType(
            Lists.newArrayList(
                // Inner
                DataTypes.createStructField("Inner", DataTypes.createStructType(Lists.newArrayList(
                    DataTypes.createStructField("field1", DataTypes.IntegerType, false)
                    )),
                    false)
            )), false)
        )
    );

    Schema schema = AvroUtils.schemaFor(input);

    assertEquals("Invalid outer record name", "record0", schema.getName());
    assertEquals("Invalid outer field count", 1, schema.getFields().size());
    assertEquals("Invalid outer field name", "Outer", schema.getFields().get(0).name());
    assertEquals("Invalid outer field type", Schema.Type.RECORD, schema.getFields().get(0).schema().getType());

    assertEquals("Invalid inner record name", "record1", schema.getFields().get(0).schema().getName());
    assertEquals("Invalid inner field count", 1, schema.getFields().get(0).schema().getFields().size());
    assertEquals("Invalid inner field name", "Inner", schema.getFields().get(0).schema().getFields().get(0).name());
    assertEquals("Invalid inner field type", Schema.Type.RECORD, schema.getFields().get(0).schema().getFields().get(0).schema().getType());

    assertEquals("Invalid inner record name", "record2", schema.getFields().get(0).schema().getFields().get(0).schema().getName());
    assertEquals("Invalid nested field count", 1, schema.getFields().get(0).schema().getFields().get(0).schema().getFields().size());
    assertEquals("Invalid nested field name", "field1", schema.getFields().get(0).schema().getFields().get(0).schema().getFields().get(0).name());
    assertEquals("Invalid nested field type", Schema.Type.INT, schema.getFields().get(0).schema().getFields().get(0).schema().getFields().get(0).schema().getType());

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toTypeSchemaBooleanNullable() throws Exception {
    Schema schema = AvroUtils.typeFor(DataTypes.BooleanType);

    assertEquals("Invalid type", Schema.Type.UNION, schema.getType());
    assertEquals("Invalid union size", 2, schema.getTypes().size());

    for (Schema s : schema.getTypes()) {
      assertThat("Invalid union types", s.getType(), anyOf(is(Schema.Type.BOOLEAN), is(Schema.Type.NULL)));
    }

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toTypeSchemaBooleanNotNullable() throws Exception {
    Schema schema = AvroUtils.typeFor(DataTypes.BooleanType, false);

    assertEquals("Invalid type", Schema.Type.BOOLEAN, schema.getType());

    //System.out.println(schema.toString(true));
  }


  @Test
  public void toTypeSchemaStringNullable() throws Exception {
    Schema schema = AvroUtils.typeFor(DataTypes.StringType);

    assertEquals("Invalid type", Schema.Type.UNION, schema.getType());
    assertEquals("Invalid union size", 2, schema.getTypes().size());

    for (Schema s : schema.getTypes()) {
      assertThat("Invalid union types", s.getType(), anyOf(is(Schema.Type.STRING), is(Schema.Type.NULL)));
    }

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toTypeSchemaStringNotNullable() throws Exception {
    Schema schema = AvroUtils.typeFor(DataTypes.StringType, false);

    assertEquals("Invalid type", Schema.Type.STRING, schema.getType());

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toTypeSchemaBinaryNullable() throws Exception {
    Schema schema = AvroUtils.typeFor(DataTypes.BinaryType);

    assertEquals("Invalid type", Schema.Type.UNION, schema.getType());
    assertEquals("Invalid union size", 2, schema.getTypes().size());

    for (Schema s : schema.getTypes()) {
      assertThat("Invalid union types", s.getType(), anyOf(is(Schema.Type.BYTES), is(Schema.Type.NULL)));
    }

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toTypeSchemaBinaryNotNullable() throws Exception {
    Schema schema = AvroUtils.typeFor(DataTypes.BinaryType, false);

    assertEquals("Invalid type", Schema.Type.BYTES, schema.getType());

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toTypeSchemaDateNullable() throws Exception {
    Schema schema = AvroUtils.typeFor(DataTypes.DateType);

    assertEquals("Invalid type", Schema.Type.UNION, schema.getType());
    assertEquals("Invalid union size", 2, schema.getTypes().size());

    for (Schema s : schema.getTypes()) {
      assertThat("Invalid union types", s.getType(), anyOf(is(Schema.Type.INT), is(Schema.Type.NULL)));
      if (s.getType().equals(Schema.Type.INT)) {
        assertEquals("Invalid LogicalType", LogicalTypes.date(), s.getLogicalType());
      }
    }

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toTypeSchemaDateNotNullable() throws Exception {
    Schema schema = AvroUtils.typeFor(DataTypes.DateType, false);

    assertEquals("Invalid type", Schema.Type.INT, schema.getType());
    assertEquals("Invalid LogicalType", LogicalTypes.date(), schema.getLogicalType());

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toTypeSchemaTimestampNullable() throws Exception {
    Schema schema = AvroUtils.typeFor(DataTypes.TimestampType);

    assertEquals("Invalid type", Schema.Type.UNION, schema.getType());
    assertEquals("Invalid union size", 2, schema.getTypes().size());

    for (Schema s : schema.getTypes()) {
      assertThat("Invalid union types", s.getType(), anyOf(is(Schema.Type.LONG), is(Schema.Type.NULL)));
      if (s.getType().equals(Schema.Type.LONG)) {
        assertEquals("Invalid LogicalType", LogicalTypes.timestampMillis(), s.getLogicalType());
      }
    }

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toTypeSchemaTimestampNotNullable() throws Exception {
    Schema schema = AvroUtils.typeFor(DataTypes.TimestampType, false);

    assertEquals("Invalid type", Schema.Type.LONG, schema.getType());
    assertEquals("Invalid LogicalType", LogicalTypes.timestampMillis(), schema.getLogicalType());

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toTypeSchemaTypeDoubleNullable() throws Exception {
    Schema schema = AvroUtils.typeFor(DataTypes.DoubleType);

    assertEquals("Invalid type", Schema.Type.UNION, schema.getType());
    assertEquals("Invalid union size", 2, schema.getTypes().size());

    for (Schema s : schema.getTypes()) {
      assertThat("Invalid union types", s.getType(), anyOf(is(Schema.Type.DOUBLE), is(Schema.Type.NULL)));
    }

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toTypeSchemaDoubleNotNullable() throws Exception {
    Schema schema = AvroUtils.typeFor(DataTypes.DoubleType, false);

    assertEquals("Invalid type", Schema.Type.DOUBLE, schema.getType());

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toTypeSchemaFloatNullable() throws Exception {
    Schema schema = AvroUtils.typeFor(DataTypes.FloatType);

    assertEquals("Invalid type", Schema.Type.UNION, schema.getType());
    assertEquals("Invalid union size", 2, schema.getTypes().size());

    for (Schema s : schema.getTypes()) {
      assertThat("Invalid union types", s.getType(), anyOf(is(Schema.Type.FLOAT), is(Schema.Type.NULL)));
    }

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toTypeSchemaFloatNotNullable() throws Exception {
    Schema schema = AvroUtils.typeFor(DataTypes.FloatType, false);

    assertEquals("Invalid type", Schema.Type.FLOAT, schema.getType());

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toTypeSchemaIntNullable() throws Exception {
    Schema schema = AvroUtils.typeFor(DataTypes.IntegerType);

    assertEquals("Invalid type", Schema.Type.UNION, schema.getType());
    assertEquals("Invalid union size", 2, schema.getTypes().size());

    for (Schema s : schema.getTypes()) {
      assertThat("Invalid union types", s.getType(), anyOf(is(Schema.Type.INT), is(Schema.Type.NULL)));
    }

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toTypeSchemaIntNotNullable() throws Exception {
    Schema schema = AvroUtils.typeFor(DataTypes.IntegerType, false);

    assertEquals("Invalid type", Schema.Type.INT, schema.getType());

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toTypeSchemaLongNullable() throws Exception {
    Schema schema = AvroUtils.typeFor(DataTypes.LongType);

    assertEquals("Invalid type", Schema.Type.UNION, schema.getType());
    assertEquals("Invalid union size", 2, schema.getTypes().size());

    for (Schema s : schema.getTypes()) {
      assertThat("Invalid union types", s.getType(), anyOf(is(Schema.Type.LONG), is(Schema.Type.NULL)));
    }

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toTypeSchemaLongNotNullable() throws Exception {
    Schema schema = AvroUtils.typeFor(DataTypes.LongType, false);

    assertEquals("Invalid type", Schema.Type.LONG, schema.getType());

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toTypeSchemaNullNullable() throws Exception {
    Schema schema = AvroUtils.typeFor(DataTypes.NullType);

    // Invalid Avro syntax to have a union of two nulls (or any other duplicate types), so this resolves to just NULL
    assertEquals("Invalid type", Schema.Type.NULL, schema.getType());

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toTypeSchemaNullNotNullable() throws Exception {
    Schema schema = AvroUtils.typeFor(DataTypes.NullType, false);

    assertEquals("Invalid type", Schema.Type.NULL, schema.getType());

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toTypeSchemaDecimalNullable() throws Exception {
    // Defaults to precision 10, scale 0
    Schema schema = AvroUtils.typeFor(DataTypes.createDecimalType());

    assertEquals("Invalid type", Schema.Type.UNION, schema.getType());
    assertEquals("Invalid union size", 2, schema.getTypes().size());

    for (Schema s : schema.getTypes()) {
      assertThat("Invalid union types", s.getType(), anyOf(is(Schema.Type.BYTES), is(Schema.Type.NULL)));
      if (s.getType().equals(Schema.Type.BYTES)) {
        assertEquals("Invalid LogicalType", LogicalTypes.decimal(10, 0), s.getLogicalType());
      }
    }

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toTypeSchemaDecimalNotNullable() throws Exception {
    Schema schema = AvroUtils.typeFor(DataTypes.createDecimalType(), false);

    assertEquals("Invalid type", Schema.Type.BYTES, schema.getType());
    assertEquals("Invalid LogicalType", LogicalTypes.decimal(10, 0), schema.getLogicalType());

    //System.out.println(schema.toString(true));
  }

  @Test
  public void toDataTypeNull() {
    assertEquals("Invalid DataType", DataTypes.NullType,
        AvroUtils.dataTypeFor(SchemaBuilder.builder().nullType()));
  }

  @Test
  public void toDataTypeInt() {
    assertEquals("Invalid DataType", DataTypes.IntegerType,
        AvroUtils.dataTypeFor(SchemaBuilder.builder().intType()));
  }

  @Test
  public void toDataTypeFloat() {
    assertEquals("Invalid DataType", DataTypes.FloatType,
        AvroUtils.dataTypeFor(SchemaBuilder.builder().floatType()));
  }

  @Test
  public void toDataTypeEnum() {
    assertEquals("Invalid DataType", DataTypes.StringType,
        AvroUtils.dataTypeFor(SchemaBuilder.enumeration("Enum").symbols("FOO")));
  }

  @Test
  public void toDataTypeFixed() {
    assertEquals("Invalid DataType", DataTypes.BinaryType,
        AvroUtils.dataTypeFor(SchemaBuilder.fixed("Fixed").size(16)));
  }

  @Test
  public void toDataTypeString() {
    assertEquals("Invalid DataType", DataTypes.StringType,
        AvroUtils.dataTypeFor(SchemaBuilder.builder().stringType()));
  }

  @Test
  public void toDataTypeBytes() {
    assertEquals("Invalid DataType", DataTypes.BinaryType,
        AvroUtils.dataTypeFor(SchemaBuilder.builder().bytesType()));
  }

  @Test
  public void toDataTypeLong() {
    assertEquals("Invalid DataType", DataTypes.LongType,
        AvroUtils.dataTypeFor(SchemaBuilder.builder().longType()));
  }

  @Test
  public void toDataTypeDouble() {
    assertEquals("Invalid DataType", DataTypes.DoubleType,
        AvroUtils.dataTypeFor(SchemaBuilder.builder().doubleType()));
  }

  @Test
  public void toDataTypeBoolean() {
    assertEquals("Invalid DataType", DataTypes.BooleanType,
        AvroUtils.dataTypeFor(SchemaBuilder.builder().booleanType()));
  }

  @Test
  public void toDataTypeArray() {
    assertEquals("Invalid DataType", DataTypes.createArrayType(DataTypes.IntegerType, false),
        AvroUtils.dataTypeFor(SchemaBuilder.array().items().intType()));
  }

  @Test
  public void toDataTypeArrayContainsNull() {
    assertEquals("Invalid DataType", DataTypes.createArrayType(DataTypes.IntegerType, true),
        AvroUtils.dataTypeFor(SchemaBuilder.array().items().nullable().intType()));
  }

  @Test
  public void toDataTypeArrayNested() {
    Schema inner = SchemaBuilder.array().items().intType();
    Schema outer = SchemaBuilder.array().items(inner);
    assertEquals("Invalid DataType",
        // Outer
        DataTypes.createArrayType(
            // Inner
            DataTypes.createArrayType(DataTypes.IntegerType, false),
            false
        ),
        AvroUtils.dataTypeFor(outer));
  }

  @Test
  public void toDataTypeMap() {
    assertEquals("Invalid DataType", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, false),
        AvroUtils.dataTypeFor(SchemaBuilder.map().values().intType()));
  }

  @Test
  public void toDataTypeMapContainsNull() {
    assertEquals("Invalid DataType", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, true),
        AvroUtils.dataTypeFor(SchemaBuilder.map().values().nullable().intType()));
  }

  @Test
  public void toDataTypeMapNested() {
    Schema inner = SchemaBuilder.map().values().intType();
    Schema outer = SchemaBuilder.map().values(inner);
    assertEquals("Invalid DataType",
        // Outer
        DataTypes.createMapType(DataTypes.StringType,
            // Inner
            DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, false),
            false
        ),
        AvroUtils.dataTypeFor(outer));
  }

  @Test
  public void toDataTypeRecord() {
    Schema record = SchemaBuilder.record("test").fields()
        .name("field1").type().intType().noDefault()
        .endRecord();

    assertEquals("Invalid DataType", DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("field1", DataTypes.IntegerType, false)
    )), AvroUtils.dataTypeFor(record));
  }

  @Test
  public void toDataTypeRecordNullable() {
    Schema record = SchemaBuilder.record("test").fields()
        .name("field1").type().nullable().intType().noDefault()
        .endRecord();

    assertEquals("Invalid DataType", DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("field1", DataTypes.IntegerType, true)
    )), AvroUtils.dataTypeFor(record));
  }

  @Test
  public void toDataTypeRecordOptional() {
    Schema record = SchemaBuilder.record("test").fields()
        .name("field1").type().optional().intType()
        .endRecord();

    assertEquals("Invalid DataType", DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("field1", DataTypes.IntegerType, true)
    )), AvroUtils.dataTypeFor(record));
  }

  @Test
  public void toDataTypeRecordNested() {
    Schema inner = SchemaBuilder.record("inner").fields()
        .name("field1").type().intType().noDefault()
        .endRecord();

    Schema outer = SchemaBuilder.record("outer").fields()
        .name("inner").type(inner).noDefault()
        .endRecord();

    assertEquals("Invalid DataType",
        DataTypes.createStructType(Lists.newArrayList(
          // Outer
          DataTypes.createStructField("inner",
            // Inner
            DataTypes.createStructType(Lists.newArrayList(
              DataTypes.createStructField("field1", DataTypes.IntegerType, false)
            )), false))
        ),
        AvroUtils.dataTypeFor(outer));
  }

  @Test
  public void toDataTypeUnion() {
    Schema union = SchemaBuilder.unionOf()
        .intType().and()
        .floatType()
        .endUnion();

    assertEquals("Invalid DataType", DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("member0", DataTypes.IntegerType, false),
        DataTypes.createStructField("member1", DataTypes.FloatType, false)
    )), AvroUtils.dataTypeFor(union));
  }

  @Test
  public void toDataTypeUnionAsSimpleOptional() {
    Schema union = SchemaBuilder.unionOf()
        .intType().and()
        .nullType()
        .endUnion();

    assertEquals("Invalid DataType", DataTypes.IntegerType, AvroUtils.dataTypeFor(union));
  }

  @Test
  public void toDataTypeUnionIncludingNull() {
    Schema union = SchemaBuilder.unionOf()
        .intType().and()
        .floatType().and()
        .nullType()
        .endUnion();

    assertEquals("Invalid DataType", DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("member0", DataTypes.IntegerType, false),
        DataTypes.createStructField("member1", DataTypes.FloatType, false),
        DataTypes.createStructField("member2", DataTypes.NullType, false)
    )), AvroUtils.dataTypeFor(union));
  }

  @Test
  public void toDataTypeDate() {
    Schema logicalType = LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType());
    assertEquals("Invalid DataType", DataTypes.DateType, AvroUtils.dataTypeFor(logicalType));
  }

  @Test
  public void toDataTypeTimestamp() {
    Schema logicalType = LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder().longType());
    assertEquals("Invalid DataType", DataTypes.TimestampType, AvroUtils.dataTypeFor(logicalType));
  }

  @Test
  public void toDataTypeDecimal() {
    Schema logicalType = LogicalTypes.decimal(4, 2).addToSchema(SchemaBuilder.builder().bytesType());
    assertEquals("Invalid DataType", DataTypes.createDecimalType(4, 2), AvroUtils.dataTypeFor(logicalType));
  }

  @Test
  public void toDataTypeIgnoredLogicalType() {
    Schema logicalType = LogicalTypes.timeMicros().addToSchema(SchemaBuilder.builder().longType());
    assertEquals("Invalid DataType", DataTypes.LongType, AvroUtils.dataTypeFor(logicalType));
  }

}