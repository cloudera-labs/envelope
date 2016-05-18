package com.cloudera.fce.envelope.utils;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.google.common.collect.Lists;

/**
 * Convenience utility methods for converting between Spark SQL and Avro serializations.
 */
public class SparkSQLAvroUtils {

    /**
     * The equivalent Avro schema for the given Spark SQL schema.
     * @param structType The Spark SQL schema.
     * @return The equivalent Avro schema.
     */
    public static Schema schemaForStructType(StructType structType) {
        List<String> fieldNames = Lists.newArrayList();
        List<String> fieldTypes = Lists.newArrayList();
        
        for (StructField field : structType.fields()) {
            fieldNames.add(field.name());
            
            DataType fieldType = field.dataType();
            if (fieldType.equals(DataTypes.StringType)) { 
                fieldTypes.add("string");
            }
            else if (fieldType.equals(DataTypes.FloatType)) { 
                fieldTypes.add("float");
            }
            else if (fieldType.equals(DataTypes.DoubleType)) { 
                fieldTypes.add("double");
            }
            else if (fieldType.equals(DataTypes.IntegerType)) { 
                fieldTypes.add("int");
            }
            else if (fieldType.equals(DataTypes.LongType)) { 
                fieldTypes.add("long");
            }
            else if (fieldType.equals(DataTypes.BooleanType)) {
                fieldTypes.add("boolean");
            }
            else {
                throw new RuntimeException("Unsupported Spark SQL field type: " + fieldType);
            }
        }
        
        return RecordUtils.schemaFor(fieldNames, fieldTypes);
    }
    
    /**
     * The equivalent Spark SQL schema for the given Avro schema.
     * @param schema The Avro schema.
     * @return The equivalent Spark SQL schema.
     */
    public static StructType structTypeForSchema(Schema schema) {
        List<StructField> fields = Lists.newArrayList();
        
        for (Field field : schema.getFields()) {
            Type fieldType = field.schema().getType();
            
            if (fieldType.equals(Type.UNION)) {
                fieldType = field.schema().getTypes().get(1).getType();
            }
            
            switch (fieldType) {
                case DOUBLE:
                    fields.add(DataTypes.createStructField(field.name(), DataTypes.DoubleType, true));
                    break;
                case FLOAT:
                    fields.add(DataTypes.createStructField(field.name(), DataTypes.FloatType, true));
                    break;
                case INT:
                    fields.add(DataTypes.createStructField(field.name(), DataTypes.IntegerType, true));
                    break;
                case LONG:
                    fields.add(DataTypes.createStructField(field.name(), DataTypes.LongType, true));
                    break;
                case STRING:
                    fields.add(DataTypes.createStructField(field.name(), DataTypes.StringType, true));
                    break;
                case BOOLEAN:
                    fields.add(DataTypes.createStructField(field.name(), DataTypes.BooleanType, true));
                    break;
                default:
                    throw new RuntimeException("Unsupported Avro field type: " + fieldType);
            }
        }
        
        return DataTypes.createStructType(fields);
    }
    
    /**
     * The equivalent RDD of Rows for the given RDD of Avro records.
     * @param records The RDD of Avro records.
     * @return The equivalent RDD of Rows.
     */
    @SuppressWarnings("serial")
    public static JavaRDD<Row> rowsForRecords(JavaRDD<GenericRecord> records) {
        return records.map(new Function<GenericRecord, Row>() {
            @Override
            public Row call(GenericRecord record) throws Exception {
                List<Object> values = Lists.newArrayList();
                
                for (Field field : record.getSchema().getFields()) {
                    Object value = record.get(field.name());
                    
                    Type fieldType = field.schema().getType();
                    if (fieldType.equals(Type.UNION)) {
                        fieldType = field.schema().getTypes().get(1).getType();
                    }
                    // Avro returns Utf8s for strings, which Spark SQL doesn't know how to use
                    if (fieldType.equals(Type.STRING) && value != null) {
                        value = value.toString();
                    }
                    
                    values.add(value);
                }
                
                return RowFactory.create(values.toArray());
            }
        });
    }
    
    /**
     * The equivalent RDD of Avro records for the given RDD of Rows.
     * @param rows The RDD of Rows.
     * @return The equivalent RDD of Avro records.
     */
    @SuppressWarnings("serial")
    public static JavaRDD<GenericRecord> recordsForRows(JavaRDD<Row> rows) {
        return rows.map(new Function<Row, GenericRecord>() {
            Schema schema;
            @Override
            public GenericRecord call(Row row) throws Exception {
                if (schema == null) {
                    schema = schemaForStructType(row.schema());
                }
                
                GenericRecord record = new GenericData.Record(schema);
                
                for (Field field : record.getSchema().getFields()) {
                    record.put(field.name(), row.get(row.fieldIndex(field.name())));
                }
                
                return record;
            }
        });
    }
    
    /**
     * The equivalent DataFrame for the given RDD of Avro records.
     * @param records The RDD of Avro records.
     * @param schema The Avro schema of the records in the RDD.
     * @param sqlc The SQLContext to use to create the DataFrame.
     * @return The equivalent DataFrame.
     */
    public static DataFrame dataFrameForRecords(JavaRDD<GenericRecord> records, Schema schema, SQLContext sqlc)
    {
        JavaRDD<Row> rows = rowsForRecords(records);
        StructType structType = structTypeForSchema(schema);
        DataFrame dataFrame = sqlc.createDataFrame(rows, structType);
        
        return dataFrame;
    }
    
}
