package com.cloudera.fce.envelope.utils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.google.common.collect.Maps;

public class RecordUtils {
    
    public static Schema subsetSchema(Schema schema, List<String> fieldNames) {
        return subsetSchema(schema, fieldNames, null);
    }
    
    public static Schema subsetSchema(Schema schema, List<String> fieldNames, Map<String, String> renames) {
        FieldAssembler<Schema> assembler = SchemaBuilder.record("s").fields();
        
        for (Field field : schema.getFields()) {
            if (fieldNames.contains(field.name())) {
                String originalFieldName = field.name();
                String newFieldName = originalFieldName;
                
                if (renames != null && renames.containsKey(originalFieldName)) {
                    newFieldName = renames.get(originalFieldName);
                }
                
                Type fieldType = field.schema().getType();
                
                if (fieldType.equals(Type.UNION)) {
                    fieldType = field.schema().getTypes().get(1).getType();
                }
                
                switch (fieldType) {
                    case DOUBLE:
                        assembler = assembler.optionalDouble(newFieldName);
                        break;
                    case FLOAT:
                        assembler = assembler.optionalFloat(newFieldName);
                        break;
                    case INT:
                        assembler = assembler.optionalInt(newFieldName);
                        break;
                    case LONG:
                        assembler = assembler.optionalLong(newFieldName);
                        break;
                    case STRING:
                        assembler = assembler.optionalString(newFieldName);
                        break;
                    case BOOLEAN:
                        assembler = assembler.optionalBoolean(newFieldName);
                        break;
                    default:
                        throw new RuntimeException("Unsupported Avro field type: " + fieldType);
                }
            }
        }
        
        return assembler.endRecord();
    }

    public static GenericRecord subsetRecord(GenericRecord record, Schema subsetSchema) {
        return subsetRecord(record, subsetSchema, null);
    }
    
    public static GenericRecord subsetRecord(GenericRecord record, Schema subsetSchema, Map<String, String> renames) {
        GenericRecord subRecord = new GenericData.Record(subsetSchema);
        
        for (Field field : subRecord.getSchema().getFields()) {
            String originalFieldName = field.name();
            String newFieldName = originalFieldName;
            
            if (renames != null && renames.containsKey(originalFieldName)) {
                newFieldName = renames.get(originalFieldName);
            }
            
            subRecord.put(newFieldName, record.get(originalFieldName));
        }
        
        return subRecord;
    }
    
    public static String compatibleFieldName(String original) {
        if (!original.matches("^[A-Za-z_].*")) {
            return "field_" + original;
        }
        else {
            return original;
        }
    }
    
    public static Schema schemaFor(List<String> fieldNames, List<String> fieldTypes) {
        FieldAssembler<Schema> assembler = SchemaBuilder.record("t").fields();
        
        for (int i = 0; i < fieldNames.size(); i++) {
            String fieldName = compatibleFieldName(fieldNames.get(i));
            
            String fieldType = fieldTypes.get(i);
            
            switch (fieldType) {
                case "string":
                    assembler = assembler.optionalString(fieldName);
                    break;
                case "float":
                    assembler = assembler.optionalFloat(fieldName);
                    break;
                case "double":
                    assembler = assembler.optionalDouble(fieldName);
                    break;
                case "int":
                    assembler = assembler.optionalInt(fieldName);
                    break;
                case "long":
                    assembler = assembler.optionalLong(fieldName);
                    break;
                case "boolean":
                    assembler = assembler.optionalBoolean(fieldName);
                    break;
                default:
                    throw new RuntimeException("Unsupported provided field type: " + fieldType);
            }
        }
        
        return assembler.endRecord();
    }
    
    public static Map<GenericRecord, List<GenericRecord>> recordsByKey(List<GenericRecord> records, List<String> keyFieldNames) {
        Map<GenericRecord, List<GenericRecord>> recordsByKey = Maps.newHashMap();
        
        if (records.size() > 0) {
            Schema keySchema = RecordUtils.subsetSchema(records.get(0).getSchema(), keyFieldNames);
            
            for (GenericRecord record : records) {
                
                GenericRecord key = RecordUtils.subsetRecord(record, keySchema);
                
                if (!recordsByKey.containsKey(key)) {
                    recordsByKey.put(key, new ArrayList<GenericRecord>());
                }
                
                List<GenericRecord> rowsForKey = recordsByKey.get(key);
                rowsForKey.add(record);
            }
        }
        
        return recordsByKey;
    }
    
    public static boolean different(GenericRecord first, GenericRecord second, List<String> valueFieldNames) {
        boolean differenceFound = false;
        
        for (int i = 0; i < first.getSchema().getFields().size(); i++) {
            if (valueFieldNames.contains(first.getSchema().getFields().get(i).name())) {
                Object firstValue = first.get(i);
                Object secondValue = second.get(i);
                
                if (firstValue != null && secondValue != null &&
                    !firstValue.equals(secondValue))
                {
                    differenceFound = true;
                }
                
                if ((firstValue != null && secondValue == null) ||
                    (firstValue == null && secondValue != null))
                {
                    differenceFound = true;
                }
            }
        }
        
        return differenceFound;
    }
    
    public static Long precedingTimestamp(Long timestamp) {
        return timestamp - 1;
    }
    
    public static boolean before(GenericRecord first, GenericRecord second, String timestampFieldName) {
        return compareTimestamp(first, second, timestampFieldName) == -1;
    }
    
    public static boolean after(GenericRecord first, GenericRecord second, String timestampFieldName) {
        return compareTimestamp(first, second, timestampFieldName) == 1;
    }
    
    public static boolean simultaneous(GenericRecord first, GenericRecord second, String timestampFieldName) {
        return compareTimestamp(first, second, timestampFieldName) == 0;
    }
    
    public static int compareTimestamp(GenericRecord r1, GenericRecord r2, String timestampFieldName) {
        Long ts1 = (Long)r1.get(timestampFieldName);
        Long ts2 = (Long)r2.get(timestampFieldName);
        if      (ts1 < ts2) return -1;
        else if (ts1 > ts2) return 1;
        else return 0;
    }
    
    public static class TimestampComparator implements Comparator<GenericRecord> {
        String timestampFieldName;
        
        public TimestampComparator(String timestampFieldName) {
            this.timestampFieldName = timestampFieldName;
        }
        
        @Override
        public int compare(GenericRecord r1, GenericRecord r2) {
            return compareTimestamp(r1, r2, timestampFieldName);
        }
    }
    
}
