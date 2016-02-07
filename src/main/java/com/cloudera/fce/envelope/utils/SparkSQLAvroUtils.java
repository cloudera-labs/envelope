package com.cloudera.fce.envelope.utils;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.google.common.collect.Lists;

public class SparkSQLAvroUtils {

    public static Schema schemaForStructType(StructType structType) throws RuntimeException {
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
            else {
                throw new RuntimeException("Unsupported Spark SQL field type: " + fieldType);
            }
        }
        
        return RecordUtils.schemaFor(fieldNames, fieldTypes);
    }
    
    public static StructType structTypeForSchema(Schema schema) throws RuntimeException {
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
                default:
                    throw new RuntimeException("Unsupported Avro field type: " + fieldType);
            }
        }
        
        return DataTypes.createStructType(fields);
    }
    
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
                    if (fieldType.equals(Type.STRING)) {
                        value = value.toString();
                    }
                    
                    values.add(value);
                }
                
                return RowFactory.create(values.toArray());
            }
        });
    }
    
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
    
}
