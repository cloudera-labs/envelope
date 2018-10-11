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

import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Mocked;
import mockit.integration.junit4.JMockit;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.joda.time.DateTime;
import org.junit.Rule;
import org.junit.Test;
import org.junit.matchers.JUnitMatchers;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(JMockit.class)
public class TestRowUtils {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testSubsetSchemaSomeFields() {
    StructField field1 = DataTypes.createStructField("field1", DataTypes.StringType, true);
    StructField field2 = DataTypes.createStructField("field2", DataTypes.IntegerType, true);
    StructField field3 = DataTypes.createStructField("field3", DataTypes.FloatType, true);
    StructType schema = DataTypes.createStructType(Lists.newArrayList(field1, field2, field3));

    StructType subset = RowUtils.subsetSchema(schema, Lists.newArrayList("field1", "field3"));

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

    StructType subset = RowUtils.subsetSchema(schema, Lists.newArrayList("field1", "field2", "field3"));

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

    StructType subset = RowUtils.subsetSchema(schema, Lists.<String>newArrayList());

    assertEquals(subset.fields().length, 0);
  }
  
  @Test
  public void testSubtractSchemaSomeFields() {
    StructField field1 = DataTypes.createStructField("field1", DataTypes.StringType, true);
    StructField field2 = DataTypes.createStructField("field2", DataTypes.IntegerType, true);
    StructField field3 = DataTypes.createStructField("field3", DataTypes.FloatType, true);
    StructType schema = DataTypes.createStructType(Lists.newArrayList(field1, field2, field3));

    StructType subset = RowUtils.subtractSchema(schema, Lists.newArrayList("field1", "field3"));

    assertEquals(subset.fields().length, 1);
    assertEquals(subset.fields()[0].name(), "field2");
  }
  
  @Test
  public void testSubtractSchemaAllFields() {
    StructField field1 = DataTypes.createStructField("field1", DataTypes.StringType, true);
    StructField field2 = DataTypes.createStructField("field2", DataTypes.IntegerType, true);
    StructField field3 = DataTypes.createStructField("field3", DataTypes.FloatType, true);
    StructType schema = DataTypes.createStructType(Lists.newArrayList(field1, field2, field3));

    StructType subset = RowUtils.subtractSchema(schema, Lists.newArrayList("field1", "field2", "field3"));

    assertEquals(subset.fields().length, 0);
  }
  
  @Test
  public void testSubtractSchemaNoFields() {
    StructField field1 = DataTypes.createStructField("field1", DataTypes.StringType, true);
    StructField field2 = DataTypes.createStructField("field2", DataTypes.IntegerType, true);
    StructField field3 = DataTypes.createStructField("field3", DataTypes.FloatType, true);
    StructType schema = DataTypes.createStructType(Lists.newArrayList(field1, field2, field3));

    StructType subset = RowUtils.subtractSchema(schema, Lists.<String>newArrayList());

    assertEquals(subset.fields().length, 3);
    assertEquals(subset.fields()[0].name(), "field1");
    assertEquals(subset.fields()[1].name(), "field2");
    assertEquals(subset.fields()[2].name(), "field3");
  }

  @Test
  public void testSubsetRowAllFields() {
    StructField field1 = DataTypes.createStructField("field1", DataTypes.StringType, true);
    StructField field2 = DataTypes.createStructField("field2", DataTypes.IntegerType, true);
    StructField field3 = DataTypes.createStructField("field3", DataTypes.FloatType, true);
    StructType schema = DataTypes.createStructType(Lists.newArrayList(field1, field2, field3));

    Row row = new RowWithSchema(schema, "hello", 1, 2.0);
    Row subsetRow = RowUtils.subsetRow(row, schema);

    assertEquals(row, subsetRow);
  }

  @Test
  public void testSubsetRowSomeFields() {
    StructField field1 = DataTypes.createStructField("field1", DataTypes.StringType, true);
    StructField field2 = DataTypes.createStructField("field2", DataTypes.IntegerType, true);
    StructField field3 = DataTypes.createStructField("field3", DataTypes.FloatType, true);
    StructType schema = DataTypes.createStructType(Lists.newArrayList(field1, field2, field3));
    StructType subsetSchema = DataTypes.createStructType(Lists.newArrayList(field1, field3));

    Row row = new RowWithSchema(schema, "hello", 1, 2.0);
    Row subsetRow = RowUtils.subsetRow(row, subsetSchema);

    assertEquals(subsetRow.length(), 2);
    assertEquals(subsetRow.get(0), "hello");
    assertEquals(subsetRow.get(1), 2.0);
  }

  @Test
  public void testSubsetRowNoFields() {
    StructField field1 = DataTypes.createStructField("field1", DataTypes.StringType, true);
    StructField field2 = DataTypes.createStructField("field2", DataTypes.IntegerType, true);
    StructField field3 = DataTypes.createStructField("field3", DataTypes.FloatType, true);
    StructType schema = DataTypes.createStructType(Lists.newArrayList(field1, field2, field3));
    StructType subsetSchema = DataTypes.createStructType(Lists.<StructField>newArrayList());

    Row row = new RowWithSchema(schema, "hello", 1, 2.0);
    Row subsetRow = RowUtils.subsetRow(row, subsetSchema);

    assertEquals(subsetRow.length(), 0);
  }

  @Test
  public void testGet() {
    StructField field1 = DataTypes.createStructField("field1", DataTypes.StringType, true);
    StructField field2 = DataTypes.createStructField("field2", DataTypes.IntegerType, true);
    StructField field3 = DataTypes.createStructField("field3", DataTypes.FloatType, true);
    StructType schema = DataTypes.createStructType(Lists.newArrayList(field1, field2, field3));

    Row row = new RowWithSchema(schema, "hello", 1, 2.0);

    assertEquals(RowUtils.get(row, "field1"), "hello");
    assertEquals(RowUtils.get(row, "field2"), 1);
    assertEquals(RowUtils.get(row, "field3"), 2.0);
  }

  @Test
  public void testSet() {
    StructField field1 = DataTypes.createStructField("field1", DataTypes.StringType, true);
    StructField field2 = DataTypes.createStructField("field2", DataTypes.IntegerType, true);
    StructField field3 = DataTypes.createStructField("field3", DataTypes.FloatType, true);
    StructType schema = DataTypes.createStructType(Lists.newArrayList(field1, field2, field3));

    Row row = new RowWithSchema(schema, "hello", 1, 2.0);
    Row setRow = RowUtils.set(row, "field2", 100);
    setRow = RowUtils.set(setRow, "field1", "world");

    assertEquals(setRow.length(), 3);
    assertEquals(RowUtils.get(setRow, "field1"), "world");
    assertEquals(RowUtils.get(setRow, "field2"), 100);
    assertEquals(RowUtils.get(setRow, "field3"), 2.0);
  }

  @Test
  public void testAppendWithoutSchema() {
    Row row = RowFactory.create("hello", 1, true);
    Row appendRow = RowUtils.append(row, -50.0);
    
    assertEquals(appendRow.length(), 4);
    assertEquals(appendRow.get(0), "hello");
    assertEquals(appendRow.get(1), 1);
    assertEquals(appendRow.get(2), true);
    assertEquals(appendRow.get(3), -50.0);
  }
  
  @Test
  public void testAppendWithSchema() {
    StructField field1 = DataTypes.createStructField("field1", DataTypes.StringType, true);
    StructField field2 = DataTypes.createStructField("field2", DataTypes.IntegerType, true);
    StructField field3 = DataTypes.createStructField("field3", DataTypes.FloatType, true);
    StructType schema = DataTypes.createStructType(Lists.newArrayList(field1, field2, field3));

    Row row = new RowWithSchema(schema, "hello", 1, 2.0);
    Row appendRow = RowUtils.append(row, "field4", DataTypes.BooleanType, true);
    appendRow = RowUtils.append(appendRow, "field5", DataTypes.StringType, "world");

    assertEquals(appendRow.length(), 5);
    assertEquals(RowUtils.get(appendRow, "field1"), "hello");
    assertEquals(RowUtils.get(appendRow, "field2"), 1);
    assertEquals(RowUtils.get(appendRow, "field3"), 2.0);
    assertEquals(RowUtils.get(appendRow, "field4"), true);
    assertEquals(RowUtils.get(appendRow, "field5"), "world");
  }

  @Test
  public void testAppendFields() {
    StructField field1 = DataTypes.createStructField("field1", DataTypes.StringType, true);
    StructField field2 = DataTypes.createStructField("field2", DataTypes.IntegerType, true);
    StructField field3 = DataTypes.createStructField("field3", DataTypes.FloatType, true);
    StructType schema = DataTypes.createStructType(Lists.newArrayList(field1, field2, field3));
    StructField field4 = DataTypes.createStructField("field4", DataTypes.BooleanType, true);
    StructField field5 = DataTypes.createStructField("field5", DataTypes.StringType, true);

    StructType appendSchema = RowUtils.appendFields(schema, Lists.newArrayList(field4, field5));

    assertEquals(appendSchema.length(), 5);
    assertEquals(appendSchema.fields()[0], field1);
    assertEquals(appendSchema.fields()[1], field2);
    assertEquals(appendSchema.fields()[2], field3);
    assertEquals(appendSchema.fields()[3], field4);
    assertEquals(appendSchema.fields()[4], field5);
  }

  @Test
  public void testValuesFor() {
    StructField field1 = DataTypes.createStructField("field1", DataTypes.StringType, true);
    StructField field2 = DataTypes.createStructField("field2", DataTypes.IntegerType, true);
    StructField field3 = DataTypes.createStructField("field3", DataTypes.FloatType, true);
    StructType schema = DataTypes.createStructType(Lists.newArrayList(field1, field2, field3));

    Row row = new RowWithSchema(schema, "hello", 1, 2.0);
    Object[] values = RowUtils.valuesFor(row);

    assertEquals(values.length, 3);
    assertEquals(values[0], "hello");
    assertEquals(values[1], 1);
    assertEquals(values[2], 2.0);
  }

  @Test
  public void testStructTypeFor() {
    List<String> fieldNames = Lists.newArrayList("field1", "field2", "field3", "field4", "field5", "field6");
    List<String> fieldTypes = Lists.newArrayList("string", "float", "double", "int", "long", "boolean");

    StructType structFromRowUtils = RowUtils.structTypeFor(fieldNames, fieldTypes);

    StructField field1 = DataTypes.createStructField("field1", DataTypes.StringType, true);
    StructField field2 = DataTypes.createStructField("field2", DataTypes.FloatType, true);
    StructField field3 = DataTypes.createStructField("field3", DataTypes.DoubleType, true);
    StructField field4 = DataTypes.createStructField("field4", DataTypes.IntegerType, true);
    StructField field5 = DataTypes.createStructField("field5", DataTypes.LongType, true);
    StructField field6 = DataTypes.createStructField("field6", DataTypes.BooleanType, true);
    StructType structFromAPI = DataTypes.createStructType(Lists.newArrayList(field1, field2, field3, field4, field5, field6));

    assertEquals(structFromRowUtils, structFromAPI);
  }

  @Test
  public void testDifferent() {
    StructField field1 = DataTypes.createStructField("field1", DataTypes.StringType, true);
    StructField field2 = DataTypes.createStructField("field2", DataTypes.IntegerType, true);
    StructField field3 = DataTypes.createStructField("field3", DataTypes.FloatType, true);
    StructType schema = DataTypes.createStructType(Lists.newArrayList(field1, field2, field3));

    Row row1 = new RowWithSchema(schema, "hello", 1, 2.0);
    Row row2 = new RowWithSchema(schema, "hello", 10, -2.0);

    assertTrue(RowUtils.different(row1, row2, Lists.newArrayList("field1", "field2", "field3")));
    assertTrue(!RowUtils.different(row1, row2, Lists.newArrayList("field1")));
  }
  
  @Test
  public void testDifferentForDifferentSchemas() {
    StructField field1 = DataTypes.createStructField("field1", DataTypes.StringType, true);
    StructField field2 = DataTypes.createStructField("field2", DataTypes.IntegerType, true);
    StructField field3 = DataTypes.createStructField("field3", DataTypes.FloatType, true);
    StructType schema1 = DataTypes.createStructType(Lists.newArrayList(field1, field2, field3));
    StructType schema2 = DataTypes.createStructType(Lists.newArrayList(field3, field2, field1));
    
    Row row1 = new RowWithSchema(schema1, "hello", 1, 2.0);
    Row row2 = new RowWithSchema(schema2, -2.0, 10, "hello");

    assertTrue(RowUtils.different(row1, row2, Lists.newArrayList("field1", "field2", "field3")));
    assertTrue(!RowUtils.different(row1, row2, Lists.newArrayList("field1")));
  }

  @Test
  public void testToRowValueBinary() {
    DataType field = DataTypes.BinaryType;

    byte[] byteArray = "Test".getBytes();
    ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray);

    assertEquals("Invalid byte[]", byteArray, RowUtils.toRowValue(byteArray, field));
    assertEquals("Invalid ByteBuffer", byteArray, RowUtils.toRowValue(byteBuffer, field));

    thrown.expect(RuntimeException.class);
    RowUtils.toRowValue(123, field);
  }

  @Test
  public void testToRowValueBoolean() {
    DataType field = DataTypes.BooleanType;

    assertEquals("Invalid Boolean", true, RowUtils.toRowValue(true, field));
    assertEquals("Invalid 'true'", true, RowUtils.toRowValue("true", field));
    assertEquals("Invalid 'true'", true, RowUtils.toRowValue("TrUe", field));
    assertEquals("Invalid 'false'", false, RowUtils.toRowValue("false", field));
    assertEquals("Invalid 'false'", false, RowUtils.toRowValue("FaLsE", field));

    try {
      RowUtils.toRowValue(123, field);
      fail("Expected a RuntimeException for invalid type");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized input format"));
    }
  }

  @Test
  public void testToRowValueCalendarInterval() {
    DataType field = DataTypes.CalendarIntervalType;

    thrown.expect(RuntimeException.class);
    thrown.expectMessage(JUnitMatchers.containsString("StructField DataType unrecognized or not yet implemented"));

    RowUtils.toRowValue("INTERVAL 1 MONTH", field);
  }

  @Test
  public void testToRowValueDate() {
    DataType field = DataTypes.DateType;

    DateTime dateObj = DateTime.parse("2017-01-01T00:00:00"); // Pass-thru the TZ
    Date sqlDate = new Date(dateObj.getMillis());

    assertEquals("Invalid Long", sqlDate, RowUtils.toRowValue(dateObj.getMillis(), field));
    assertEquals("Invalid String", sqlDate, RowUtils.toRowValue("2017-001", field)); // ISO Date format
    assertEquals("Invalid Date", sqlDate, RowUtils.toRowValue(dateObj.toDate(), field));
    assertEquals("Invalid DateTime", sqlDate, RowUtils.toRowValue(dateObj, field));

    thrown.expect(RuntimeException.class);
    thrown.expectMessage(JUnitMatchers.containsString("Invalid or unrecognized input format"));
    RowUtils.toRowValue(123, field);
  }

  @Test
  public void testToRowValueTimestamp() {
    DataType field = DataTypes.TimestampType;

    DateTime dateObj = DateTime.parse("2017-01-01T00:00:00"); // Pass-thru the TZ
    Timestamp sqlTimestamp = new Timestamp(dateObj.getMillis());

    assertEquals("Invalid Long", sqlTimestamp, RowUtils.toRowValue(dateObj.getMillis(), field));
    assertEquals("Invalid String", sqlTimestamp, RowUtils.toRowValue("2017-001", field)); // ISO Date format
    assertEquals("Invalid Date", sqlTimestamp, RowUtils.toRowValue(dateObj.toDate(), field));
    assertEquals("Invalid DateTime", sqlTimestamp, RowUtils.toRowValue(dateObj, field));

    thrown.expect(RuntimeException.class);
    thrown.expectMessage(JUnitMatchers.containsString("Invalid or unrecognized input format"));
    RowUtils.toRowValue(123, field);
  }

  @Test
  public void testToRowValueDouble() {
    DataType field = DataTypes.DoubleType;

    Double value = Double.valueOf("123");

    assertEquals("Invalid Double", value, RowUtils.toRowValue(value, field));
    assertEquals("Invalid Number", value, RowUtils.toRowValue(123L, field));
    assertEquals("Invalid String", value, RowUtils.toRowValue("123", field));

    try {
      RowUtils.toRowValue("foo", field);
      fail("Expected a RuntimeException for invalid type");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized input format"));
    }

    try {
      RowUtils.toRowValue(ByteBuffer.allocate(1), field);
      fail("Expected a RuntimeException for invalid type");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized input format"));
    }
  }

  @Test
  public void testToRowValueFloat() {
    DataType field = DataTypes.FloatType;

    Float value = Float.valueOf("123");

    assertEquals("Invalid Float", value, RowUtils.toRowValue(value, field));
    assertEquals("Invalid Number", value, RowUtils.toRowValue(123L, field));
    assertEquals("Invalid String", value, RowUtils.toRowValue("123", field));

    try {
      RowUtils.toRowValue("foo", field);
      fail("Expected a RuntimeException for invalid type");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized input format"));
    }

    try {
      RowUtils.toRowValue(ByteBuffer.allocate(1), field);
      fail("Expected a RuntimeException for invalid type");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized input format"));
    }
  }

  @Test
  public void testToRowValueInteger() {
    DataType field = DataTypes.IntegerType;

    Integer value = Integer.valueOf("123");

    assertEquals("Invalid Integer", value, RowUtils.toRowValue(value, field));
    assertEquals("Invalid Number", value, RowUtils.toRowValue(123L, field));
    assertEquals("Invalid String", value, RowUtils.toRowValue("123", field));

    try {
      RowUtils.toRowValue("foo", field);
      fail("Expected a RuntimeException for invalid type");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized input format"));
    }

    try {
      RowUtils.toRowValue(ByteBuffer.allocate(1), field);
      fail("Expected a RuntimeException for invalid type");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized input format"));
    }
  }

  @Test
  public void testToRowValueLong() {
    DataType field = DataTypes.LongType;

    Long value = Long.valueOf("123");

    assertEquals("Invalid Long", value, RowUtils.toRowValue(value, field));
    assertEquals("Invalid Number", value, RowUtils.toRowValue(123, field));
    assertEquals("Invalid String", value, RowUtils.toRowValue("123", field));

    try {
      RowUtils.toRowValue("foo", field);
      fail("Expected a RuntimeException for invalid type");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized input format"));
    }

    try {
      RowUtils.toRowValue(ByteBuffer.allocate(1), field);
      fail("Expected a RuntimeException for invalid type");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized input format"));
    }
  }

  @Test
  public void testToRowValueNull() {
    DataType field = DataTypes.NullType;

    assertEquals("Invalid NULL", null, RowUtils.toRowValue(null, field));

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Invalid or unrecognized input format");
    RowUtils.toRowValue(ByteBuffer.allocate(1), field);
  }

  @Test
  public void testToRowValueByte() {
    DataType field = DataTypes.ByteType;

    Byte value = Byte.valueOf("123");

    assertEquals("Invalid Byte", value, RowUtils.toRowValue(value, field));
    assertEquals("Invalid Number", value, RowUtils.toRowValue(123, field));
    assertEquals("Invalid String", value, RowUtils.toRowValue("123", field));

    try {
      RowUtils.toRowValue("foo", field);
      fail("Expected a RuntimeException for invalid type");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized input format"));
    }

    try {
      RowUtils.toRowValue(ByteBuffer.allocate(1), field);
      fail("Expected a RuntimeException for invalid type");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized input format"));
    }
  }

  @Test
  public void testToRowValueShort() {
    DataType field = DataTypes.ShortType;

    Short value = Short.valueOf("123");

    assertEquals("Invalid Short", value, RowUtils.toRowValue(value, field));
    assertEquals("Invalid Number", value, RowUtils.toRowValue(123, field));
    assertEquals("Invalid String", value, RowUtils.toRowValue("123", field));

    try {
      RowUtils.toRowValue("foo", field);
      fail("Expected a RuntimeException for invalid type");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized input format"));
    }

    try {
      RowUtils.toRowValue(ByteBuffer.allocate(1), field);
      fail("Expected a RuntimeException for invalid type");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized input format"));
    }
  }

  @Test
  public void testToRowValueString() {
    DataType field = DataTypes.StringType;

    String value = "value";

    assertEquals("Invalid String", value, RowUtils.toRowValue(value, field));
  }

  @Test
  public void testToRowValueDecimal() {
    DataType defaultField = DataTypes.createDecimalType(); // precision 10, scale 0

    BigDecimal defaultDecimal = new BigDecimal("10");

    assertEquals("Invalid double", defaultDecimal, RowUtils.toRowValue(10.157D, defaultField));
    assertEquals("Invalid string", defaultDecimal, RowUtils.toRowValue("10.157", defaultField));
    assertEquals("Invalid BigDecimal", defaultDecimal, RowUtils.toRowValue(new BigDecimal("10"), defaultField));

    assertEquals("Invalid long", defaultDecimal, RowUtils.toRowValue(10L, defaultField));
    assertEquals("Invalid BigInteger", defaultDecimal, RowUtils.toRowValue(new BigInteger("10"), defaultField));

    assertEquals("Invalid precision", 2,
        ((BigDecimal) RowUtils.toRowValue("10.157", defaultField)).precision());
    assertEquals("Invalid scale", 0,
        ((BigDecimal) RowUtils.toRowValue("10.157", defaultField)).scale());

    try {
      RowUtils.toRowValue(ByteBuffer.allocate(1), defaultField);
      fail("Expected a RuntimeException for invalid type");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized input format"));
    }

    DataType customField = DataTypes.createDecimalType(3, 2);

    BigDecimal customDecimal = new BigDecimal("1.23");

    assertEquals("Invalid double", customDecimal, RowUtils.toRowValue(1.23D, customField));
    assertEquals("Invalid string", customDecimal, RowUtils.toRowValue("1.23", customField));
    assertEquals("Invalid BigDecimal", customDecimal, RowUtils.toRowValue(new BigDecimal("1.23"), customField));

    assertEquals("Invalid long", customDecimal, RowUtils.toRowValue(123L, customField));
    assertEquals("Invalid BigInteger", customDecimal, RowUtils.toRowValue(new BigInteger("123"), customField));

    assertEquals("Invalid precision", 3,
        ((BigDecimal) RowUtils.toRowValue("1.23", customField)).precision());
    assertEquals("Invalid scale", 2,
        ((BigDecimal) RowUtils.toRowValue("1.23", customField)).scale());
  }

  @Test
  public void testToRowValueArray() {
    DataType field = DataTypes.createArrayType(DataTypes.IntegerType);

    List<?> expectedInts = Lists.newArrayList(1, 2, 3);
    List<?> expectedNulls = Lists.newArrayList(1, null, 3);

    try {
      RowUtils.toRowValue(12.34, field);
      fail("Expected a RuntimeException for invalid type");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized input format"));
    }

    // Lists
    assertEquals("Invalid List of Ints", expectedInts, RowUtils.toRowValue(Lists.newArrayList(1, 2, 3), field));
    assertEquals("Invalid List of Mixed", expectedInts, RowUtils.toRowValue(Lists.<Object>newArrayList("1", 2, 3L), field));
    assertEquals("Invalid List of Explicit Nulls", expectedNulls, RowUtils.toRowValue(Lists.newArrayList(1, null, 3), field));

    try {
      RowUtils.toRowValue(Lists.newArrayList(1, ByteBuffer.allocate(1), 3), field);
      fail("Expected a RuntimeException for invalid element conversion");
    } catch (Exception e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized element format"));
    }

    // Rows
    assertEquals("Invalid List of Ints", expectedInts, RowUtils.toRowValue(RowFactory.create(1, 2, 3), field));
    assertEquals("Invalid List of Mixed", expectedInts, RowUtils.toRowValue(RowFactory.create("1", 2, 3L), field));
    assertEquals("Invalid List of Explicit Nulls", expectedNulls, RowUtils.toRowValue(RowFactory.create(1, null, 3), field));

    try {
      RowUtils.toRowValue(RowFactory.create(1, ByteBuffer.allocate(1), 3), field);
      fail("Expected a RuntimeException for invalid element conversion");
    } catch (Exception e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized value format"));
    }
  }

  @Test
  public void testToRowValueArrayNested() {
    // An array of INT arrays
    DataType nested = DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.IntegerType));

    List<Integer> expectedInts = Lists.newArrayList(1, 2, 3);
    List<List<Integer>> expectedArrays = Lists.newArrayList();
    expectedArrays.add(expectedInts);

    List<Integer> expectedInnerNulls = Lists.newArrayList(1, null, 3);
    List<List<Integer>> expectedOuterNulls = Lists.newArrayList();
    expectedOuterNulls.add(expectedInnerNulls);

    List<List<Integer>> expectedNullArrays = Lists.newArrayList();
    expectedNullArrays.add(null);

    //
    // Nested Lists
    //

    // Valid inner values
    List<Integer> testInts = Lists.newArrayList(1, 2, 3);
    List<List<Integer>> testArrayInts = Lists.newArrayList();
    testArrayInts.add(testInts);
    assertEquals("Invalid List of Arrays", expectedArrays, RowUtils.toRowValue(testArrayInts, nested));

    // Valid inner conversion
    List<?> testMixed = Lists.<Object>newArrayList("1", 2, 3L);
    List<List<?>> testArrayMixed = Lists.newArrayList();
    testArrayMixed.add(testMixed);
    assertEquals("Invalid List of Mixed", expectedArrays, RowUtils.toRowValue(testArrayInts, nested));

    // Valid outer null value (implicit with ArrayType)
    List<List<Integer>> testArrayNull = Lists.newArrayList();
    testArrayNull.add(null);
    assertEquals("Invalid List of Null Arrays", expectedNullArrays, RowUtils.toRowValue(testArrayNull, nested));

    // Invalid outer type
    try {
      RowUtils.toRowValue(Lists.newArrayList(ByteBuffer.allocate(1)), nested);
      fail("Expected a RuntimeException for invalid outer type conversion");
    } catch (Exception e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized element format"));
    }

    // Invalid inner type
    List<Object> testInvalidElement = Lists.newArrayList();
    testInvalidElement.add(1);
    testInvalidElement.add(false);
    testInvalidElement.add(3);
    List<List<Object>> testArrayInvalidElement = Lists.newArrayList();
    testArrayInvalidElement.add(testInvalidElement);

    try {
      RowUtils.toRowValue(testArrayInvalidElement, nested);
      fail("Expected a RuntimeException for invalid inner type conversion");
    } catch (Exception e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized element format"));
    }

    // Valid inner null
    List<Object> testNullElement = Lists.newArrayList();
    testNullElement.add(1);
    testNullElement.add(null);
    testNullElement.add(3);
    List<List<Object>> testArrayNullElement = Lists.newArrayList();
    testArrayNullElement.add(testNullElement);
    assertEquals("Invalid List of Explicit Inner Nulls", expectedOuterNulls, RowUtils.toRowValue(testArrayNullElement, nested));

    //
    // Nested Rows
    //

    // Valid inner values
    Row rowInts = RowFactory.create(RowFactory.create(1, 2, 3));
    assertEquals("Invalid List of Arrays", expectedArrays, RowUtils.toRowValue(rowInts, nested));

    // Valid inner conversions
    Row rowMixed = RowFactory.create(RowFactory.create("1", 2, 3L));
    assertEquals("Invalid List of Mixed", expectedArrays, RowUtils.toRowValue(rowMixed, nested));

    // Valid outer null value (implicit with ArrayType)
    assertEquals("Invalid List of Null Arrays", expectedNullArrays, RowUtils.toRowValue(testArrayNull, nested));

    // Invalid outer type
    try {
      RowUtils.toRowValue(RowFactory.create(ByteBuffer.allocate(1)), nested);
      fail("Expected a RuntimeException for invalid outer type conversion");
    } catch (Exception e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized value format"));
    }

    // Invalid inner type
    try {
      RowUtils.toRowValue(RowFactory.create(RowFactory.create(1, false, 3)), nested);
      fail("Expected a RuntimeException for invalid inner type conversion");
    } catch (Exception e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized value format"));
    }

    // Valid inner null
    Row validNull = RowFactory.create(RowFactory.create(1, null, 3));
    assertEquals("Invalid List of Explicit Inner Nulls", expectedOuterNulls, RowUtils.toRowValue(validNull, nested));

    //
    // Mixed types
    //

    // List->Row
    List<Row> nestedRow = Lists.newArrayList(RowFactory.create(1, 2, 3));
    assertEquals("Invalid List of Rows", expectedArrays, RowUtils.toRowValue(nestedRow, nested));

    // Row->List
    Row nestedList = RowFactory.create(Lists.newArrayList(1, 2, 3));
    assertEquals("Invalid List of Lists", expectedArrays, RowUtils.toRowValue(nestedList, nested));
  }

  @Test
  public void testToRowValueMap() {
    DataType fieldNotNullable = DataTypes.createMapType(DataTypes.LongType, DataTypes.IntegerType, false);
    DataType fieldNullable = DataTypes.createMapType(DataTypes.LongType, DataTypes.IntegerType, true);

    Map<Object, Object> expectedValues = Maps.newHashMap();
    expectedValues.put(9L, 1);
    expectedValues.put(8L, 2);

    Map<Object, Object> expectedNulls = Maps.newHashMap();
    expectedNulls.put(9L, null);
    expectedNulls.put(8L, 2);

    Map<Object, Object> inputMap = Maps.newHashMap();

    // Straight values
    inputMap.put(9L, 1);
    inputMap.put(8L, 2);
    assertEquals("Invalid map of values", expectedValues, RowUtils.toRowValue(inputMap, fieldNotNullable));

    // Convert values
    inputMap.clear();
    inputMap.put(9L, 1L);
    inputMap.put(8L, "2");
    assertEquals("Invalid map of values", expectedValues, RowUtils.toRowValue(inputMap, fieldNotNullable));

    // Convert keys
    inputMap.clear();
    inputMap.put(9, 1);
    inputMap.put("8", 2);
    assertEquals("Invalid map of values", expectedValues, RowUtils.toRowValue(inputMap, fieldNotNullable));

    // Invalid type
    try {
      RowUtils.toRowValue(ByteBuffer.allocate(1), fieldNotNullable);
      fail("Expected a RuntimeException for type");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized input format"));
    }

    // Invalid value
    try {
      inputMap.clear();
      inputMap.put(9L, ByteBuffer.allocate(1));
      inputMap.put(8L, 2);
      RowUtils.toRowValue(inputMap, fieldNotNullable);
      fail("Expected a RuntimeException for invalid value type");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized value format"));
    }

    // Invalid key
    try {
      inputMap.clear();
      inputMap.put(ByteBuffer.allocate(1), 1);
      inputMap.put(8L, 2);
      RowUtils.toRowValue(inputMap, fieldNotNullable);
      fail("Expected a RuntimeException for invalid key type");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized key format"));
    }

    // Null key
    try {
      inputMap.clear();
      inputMap.put(null, 1);
      inputMap.put(8L, 2);
      RowUtils.toRowValue(inputMap, fieldNotNullable);
      fail("Expected a RuntimeException for null key");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized key format"));
    }

    // Valid 'null' value
    inputMap.clear();
    inputMap.put(9L, null);
    inputMap.put(8L, 2);
    assertEquals("Invalid 'null' value", expectedNulls, RowUtils.toRowValue(inputMap, fieldNullable));
  }

  @Test
  public void testToRowValueMapRow(
      final @Mocked Row inputRow,
      final @Mocked StructType rowSchema
  ) {
    DataType fieldNotNullable = DataTypes.createMapType(DataTypes.StringType, DataTypes.LongType, false);
    DataType fieldNullable = DataTypes.createMapType(DataTypes.StringType, DataTypes.LongType, true);

    Map<Object, Object> expectedValues = Maps.newHashMap();
    expectedValues.put("field1", 1L);
    expectedValues.put("field2", 2L);

    Map<Object, Object> expectedNulls = Maps.newHashMap();
    expectedNulls.put("field1", null);
    expectedNulls.put("field2", 2L);

    new Expectations() {{
      inputRow.schema(); result = rowSchema;

      rowSchema.fieldNames(); result = new String[] {"field1", "field2"};
      inputRow.get(0); returns(1L, 1L, ByteBuffer.allocate(1), null);
      inputRow.get(1); returns(2L, "2", 2L);
    }};

    // Straight values
    assertEquals("Invalid map of values", expectedValues, RowUtils.toRowValue(inputRow, fieldNotNullable));

    // Converted values
    assertEquals("Invalid map of values", expectedValues, RowUtils.toRowValue(inputRow, fieldNotNullable));

    // Invalid type
    try {
      RowUtils.toRowValue(ByteBuffer.allocate(1), fieldNotNullable);
      fail("Expected a RuntimeException for type");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized input format"));
    }

    // Invalid value
    try {
      RowUtils.toRowValue(inputRow, fieldNotNullable);
      fail("Expected a RuntimeException for invalid value type");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized value format"));
    }

    // Null value
    try {
      RowUtils.toRowValue(inputRow, fieldNotNullable);
      fail("Expected a RuntimeException for 'null' value");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Value cannot be 'null'"));
    }

    // Valid Null value
    assertEquals("Invalid null value", expectedNulls, RowUtils.toRowValue(inputRow, fieldNullable));

    // Missing schema
    try {
      RowUtils.toRowValue(RowFactory.create(""), fieldNotNullable);
      fail("Expected a RuntimeException for a missing schema");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid Row format, no schema found"));
    }
  }

  @Test
  public void testToRowValueMapNested() {
    DataType field = DataTypes.createMapType(DataTypes.StringType,
        DataTypes.createMapType(DataTypes.LongType, DataTypes.IntegerType, true)
    );

    Map<Object, Object> expectedInnerMap = Maps.newHashMap();
    expectedInnerMap.put(9L, 1);
    expectedInnerMap.put(8L, 2);

    Map<Object, Object> expectedOuterMap = Maps.newHashMap();
    expectedOuterMap.put("outer", expectedInnerMap);

    Map<Object, Object> innerMap = Maps.newHashMap();
    innerMap.put(9L, 1);
    innerMap.put(8L, 2);

    Map<Object, Object> outerMap = Maps.newHashMap();
    outerMap.put("outer", innerMap);

    assertEquals("Invalid list of values", expectedOuterMap, RowUtils.toRowValue(outerMap, field));
  }

  @Test
  public void testToRowValueMapRowNested(
      final @Mocked Row inputRow,
      final @Mocked StructType innerSchema,
      final @Mocked StructType outerSchema
  ) {
    DataType field = DataTypes.createMapType(DataTypes.StringType,
        DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, true)
    );

    Map<Object, Object> expectedInnerMap = Maps.newHashMap();
    expectedInnerMap.put("field1", 1);
    expectedInnerMap.put("field2", 2);

    Map<Object, Object> expectedOuterMap = Maps.newHashMap();
    expectedOuterMap.put("outer", expectedInnerMap);

    new Expectations() {{
      inputRow.schema(); returns(outerSchema, innerSchema);

      outerSchema.fieldNames(); result = new String[] {"outer"};
      innerSchema.fieldNames(); result = new String[] {"field1", "field2"};

      inputRow.get(0); returns(inputRow, 1);
      inputRow.get(1); result = 2;
    }};

    assertEquals("Invalid list of values", expectedOuterMap, RowUtils.toRowValue(inputRow, field));
  }

  @Test
  public void testToRowValueStruct() {
    DataType field = DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("field1", DataTypes. LongType, true),
        DataTypes.createStructField("field2", DataTypes.IntegerType, false)
    ));

    Row expectedValues = RowFactory.create(9L, 2);
    Row expectedNulls = RowFactory.create(null, 2);

    //
    // Lists
    //

    // Straight values
    assertEquals("Invalid list of values", expectedValues, RowUtils.toRowValue(Lists.<Object>newArrayList(9L, 2), field));

    // Conversion values
    assertEquals("Invalid list of values", expectedValues, RowUtils.toRowValue(Lists.<Object>newArrayList("9", 2L), field));

    // Invalid length (lt)
    try {
      RowUtils.toRowValue(Lists.newArrayList(9L), field);
      fail("Expected a RuntimeException for invalid length ");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid size of input List"));
    }

    // Invalid length (gt)
    try {
      RowUtils.toRowValue(Lists.<Object>newArrayList(9L, 2, 3), field);
      fail("Expected a RuntimeException for invalid length ");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid size of input List"));
    }

    // Invalid conversion, nullable
    try {
      RowUtils.toRowValue(Lists.newArrayList(ByteBuffer.allocate(1), 2), field);
      fail("Expected a RuntimeException for invalid type on nullable field");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized element format"));
    }

    // Invalid conversion, non-nullable
    try {
      RowUtils.toRowValue(Lists.newArrayList(9L, ByteBuffer.allocate(1)), field);
      fail("Expected a RuntimeException for invalid type on non-nullable field");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized element format"));
    }

    // Valid 'null' value
    assertEquals("Invalid list of nulls", expectedNulls, RowUtils.toRowValue(Lists.newArrayList(null, 2), field));

    // Invalid 'null' value
    try {
      RowUtils.toRowValue(Lists.newArrayList(9L, null), field);
      fail("Expected a RuntimeException for invalid nullable field");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Element cannot be 'null'"));
    }

    //
    // Map
    //

    Map<Object, Object> inputMap = Maps.newHashMap();
    inputMap.put("field1", 9L);
    inputMap.put("field2", 2);

    // Straight values
    assertEquals("Invalid list of values", expectedValues, RowUtils.toRowValue(inputMap, field));

    // Conversion values
    inputMap.clear();
    inputMap.put("field1", "9");
    inputMap.put("field2", 2L);
    assertEquals("Invalid list of values", expectedValues, RowUtils.toRowValue(inputMap, field));

    // Invalid or missing key
    try {
      inputMap.clear();
      inputMap.put("field1".getBytes(), 9L);
      inputMap.put("field2", 2);
      RowUtils.toRowValue(inputMap, field);
      fail("Expected a RuntimeException for invalid or missing key");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Key not found on input"));
    }

    // Invalid conversion on nullable field
    try {
      inputMap.clear();
      inputMap.put("field1", "One"); // will not convert properly
      inputMap.put("field2", 2);
      RowUtils.toRowValue(inputMap, field);
      fail("Expected a RuntimeException for invalid type on nullable field");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized value format"));
    }

    // Invalid conversion on non-nullable field
    try {
      inputMap.clear();
      inputMap.put("field1", "One");
      inputMap.put("field2", ByteBuffer.allocate(1));
      RowUtils.toRowValue(inputMap, field);
      fail("Expected a RuntimeException for invalid type on non-nullable field");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized value format"));
    }

    // Valid 'null' value
    inputMap.clear();
    inputMap.put("field1", null);
    inputMap.put("field2", 2);
    assertEquals("Invalid list of nulls", expectedNulls, RowUtils.toRowValue(inputMap, field));

    // Invalid 'null' value
    try {
      inputMap.clear();
      inputMap.put("field1", "One");
      inputMap.put("field2", null);
      RowUtils.toRowValue(inputMap, field);
      fail("Expected a RuntimeException for invalid nullable field");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized value format"));
    }

    //
    // Row
    //

    // Straight values
    assertEquals("Invalid list of values", expectedValues, RowUtils.toRowValue(RowFactory.create(9L, 2), field));

    // Conversion values
    assertEquals("Invalid list of values", expectedValues, RowUtils.toRowValue(RowFactory.create("9", 2L), field));

    // Invalid length (lt)
    try {
      RowUtils.toRowValue(RowFactory.create(9L), field);
      fail("Expected a RuntimeException for invalid length (lt)");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid size of input Row"));
    }

    // Invalid length (gt)
    try {
      RowUtils.toRowValue(RowFactory.create(9L, 2, 3), field);
      fail("Expected a RuntimeException for invalid length (gt)");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid size of input Row"));
    }

    // Invalid conversion, nullable
    try {
      RowUtils.toRowValue(RowFactory.create(ByteBuffer.allocate(1), 2), field);
      fail("Expected a RuntimeException for invalid type on nullable field");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized value format"));
    }

    // Invalid conversion, non-nullable
    try {
      RowUtils.toRowValue(RowFactory.create(9L, ByteBuffer.allocate(1)), field);
      fail("Expected a RuntimeException for invalid type on non-nullable");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Invalid or unrecognized value format"));
    }

    // Valid 'null' value
    assertEquals("Invalid list of nulls", expectedNulls, RowUtils.toRowValue(RowFactory.create(null, 2), field));

    // Invalid 'null' value
    try {
      RowUtils.toRowValue(RowFactory.create(9L, null), field);
      fail("Expected a RuntimeException for invalid null on nullable field");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Value cannot be 'null'"));
    }
  }

  @Test
  public void testToRowValueStructNested() {
    DataType field = DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("outer",
            DataTypes.createStructType(Lists.newArrayList(
                DataTypes.createStructField("field1", DataTypes.LongType, true),
                DataTypes.createStructField("field2", DataTypes.IntegerType, false)
            )),
            true)
    ));

    Row expectedInnerValues = RowFactory.create(9L, 2);
    Row expectedValues = RowFactory.create(expectedInnerValues);

    Map<Object, Object> innerMap = Maps.newHashMap();
    innerMap.put("field1", 9L);
    innerMap.put("field2", 2);

    List<?> innerList = Lists.<Object>newArrayList(9L, 2);

    Row innerRow = RowFactory.create(9L, 2);

    // Nested Map -> Map
    Map<Object, Object> outerMap = Maps.newHashMap();
    outerMap.put("outer", innerMap);
    assertEquals("Invalid list of values", expectedValues, RowUtils.toRowValue(outerMap, field));

    // Nested Map -> List
    outerMap.put("outer", innerList);
    assertEquals("Invalid list of values", expectedValues, RowUtils.toRowValue(outerMap, field));

    // Nested Map -> Row
    outerMap.put("outer", innerRow);
    assertEquals("Invalid list of values", expectedValues, RowUtils.toRowValue(outerMap, field));

    // Nested List -> Map
    List<Object> outerList = new ArrayList<>();
    outerList.add(innerMap);
    assertEquals("Invalid list of values", expectedValues, RowUtils.toRowValue(outerList, field));

    // Nested List -> List
    outerList.clear();
    outerList.add(innerList);
    assertEquals("Invalid list of values", expectedValues, RowUtils.toRowValue(outerList, field));

    // Nested List -> Row
    outerList.clear();
    outerList.add(innerRow);
    assertEquals("Invalid list of values", expectedValues, RowUtils.toRowValue(outerList, field));

    // Nested Row -> Map
    Row outerRow = RowFactory.create(innerMap); // Pretty sure this cannot happen, but hey, why not
    assertEquals("Invalid list of values", expectedValues, RowUtils.toRowValue(outerRow, field));

    // Nested Row -> List
    outerRow = RowFactory.create(innerList); // Same same
    assertEquals("Invalid list of values", expectedValues, RowUtils.toRowValue(outerRow, field));

    // Nested Row -> Row
    outerRow = RowFactory.create(innerRow);
    assertEquals("Invalid list of values", expectedValues, RowUtils.toRowValue(outerRow, field));

  }

}
