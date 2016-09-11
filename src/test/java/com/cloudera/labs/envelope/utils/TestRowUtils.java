package com.cloudera.labs.envelope.utils;

import static org.junit.Assert.*;

import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.google.common.collect.Lists;

public class TestRowUtils {

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
    public void testAppend() {
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
        
        assert(RowUtils.different(row1, row2, Lists.newArrayList("field1", "field2", "field3")));
        assert(!RowUtils.different(row1, row2, Lists.newArrayList("field1")));
    }
    
    @Test
    public void testPrecedingTimestamp() {
        assertEquals((long)RowUtils.precedingTimestamp(10000L), 9999L);
    }
    
    @Test
    public void testBefore() {
        StructField field1 = DataTypes.createStructField("field1", DataTypes.StringType, true);
        StructField field2 = DataTypes.createStructField("field2", DataTypes.LongType, true);
        StructField field3 = DataTypes.createStructField("field3", DataTypes.FloatType, true);
        StructType schema = DataTypes.createStructType(Lists.newArrayList(field1, field2, field3));
        
        Row row1 = new RowWithSchema(schema, "hello", 1L, 2.0);
        Row row2 = new RowWithSchema(schema, "hello", 10L, -2.0);
        Row row3 = new RowWithSchema(schema, "world", 1L, -2000.0);
        
        assert(RowUtils.before(row1, row2, "field2"));
        assert(!RowUtils.before(row2, row1, "field2"));
        assert(!RowUtils.before(row1, row3, "field2"));
    }
    
    @Test
    public void testAfter() {
        StructField field1 = DataTypes.createStructField("field1", DataTypes.StringType, true);
        StructField field2 = DataTypes.createStructField("field2", DataTypes.LongType, true);
        StructField field3 = DataTypes.createStructField("field3", DataTypes.FloatType, true);
        StructType schema = DataTypes.createStructType(Lists.newArrayList(field1, field2, field3));
        
        Row row1 = new RowWithSchema(schema, "hello", 1L, 2.0);
        Row row2 = new RowWithSchema(schema, "hello", 10L, -2.0);
        Row row3 = new RowWithSchema(schema, "world", 1L, -2000.0);
        
        assert(!RowUtils.after(row1, row2, "field2"));
        assert(RowUtils.after(row2, row1, "field2"));
        assert(!RowUtils.after(row1, row3, "field2"));
    }
    
    @Test
    public void testSimultaneous() {
        StructField field1 = DataTypes.createStructField("field1", DataTypes.StringType, true);
        StructField field2 = DataTypes.createStructField("field2", DataTypes.LongType, true);
        StructField field3 = DataTypes.createStructField("field3", DataTypes.FloatType, true);
        StructType schema = DataTypes.createStructType(Lists.newArrayList(field1, field2, field3));
        
        Row row1 = new RowWithSchema(schema, "hello", 1L, 2.0);
        Row row2 = new RowWithSchema(schema, "hello", 10L, -2.0);
        Row row3 = new RowWithSchema(schema, "world", 1L, -2000.0);
        
        assert(!RowUtils.simultaneous(row1, row2, "field2"));
        assert(!RowUtils.simultaneous(row2, row1, "field2"));
        assert(RowUtils.simultaneous(row1, row3, "field2"));
    }
    
    @Test
    public void testCompareTimestamp() {
        StructField field1 = DataTypes.createStructField("field1", DataTypes.StringType, true);
        StructField field2 = DataTypes.createStructField("field2", DataTypes.LongType, true);
        StructField field3 = DataTypes.createStructField("field3", DataTypes.FloatType, true);
        StructType schema = DataTypes.createStructType(Lists.newArrayList(field1, field2, field3));
        
        Row row1 = new RowWithSchema(schema, "hello", 1L, 2.0);
        Row row2 = new RowWithSchema(schema, "hello", 10L, -2.0);
        Row row3 = new RowWithSchema(schema, "world", 1L, -2000.0);
        
        assertEquals(RowUtils.compareTimestamp(row1, row2, "field2"), -1);
        assertEquals(RowUtils.compareTimestamp(row2, row1, "field2"), 1);
        assertEquals(RowUtils.compareTimestamp(row1, row3, "field2"), 0);
    }
    
}
