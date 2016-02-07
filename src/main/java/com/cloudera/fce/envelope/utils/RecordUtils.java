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

    public static GenericRecord subsetRecord(GenericRecord record, List<String> fieldNames) throws RuntimeException {
        FieldAssembler<Schema> assembler = SchemaBuilder.record("s").fields();
        
        for (Field field : record.getSchema().getFields()) {
            if (fieldNames.contains(field.name())) {
                Type fieldType = field.schema().getType();
                
                if (fieldType.equals(Type.UNION)) {
                    fieldType = field.schema().getTypes().get(1).getType();
                }
                
                switch (fieldType) {
                    case DOUBLE:
                        assembler = assembler.optionalDouble(field.name());
                        break;
                    case FLOAT:
                        assembler = assembler.optionalFloat(field.name());
                        break;
                    case INT:
                        assembler = assembler.optionalInt(field.name());
                        break;
                    case LONG:
                        assembler = assembler.optionalLong(field.name());
                        break;
                    case STRING:
                        assembler = assembler.optionalString(field.name());
                        break;
                    default:
                        throw new RuntimeException("Unsupported Avro field type: " + fieldType);
                }
            }
        }
        
        Schema schema = assembler.endRecord();
        
        GenericRecord subRecord = new GenericData.Record(schema);
        
        for (Field field : subRecord.getSchema().getFields()) {
            subRecord.put(field.name(), record.get(field.name()));
        }
        
        return subRecord;
    }
    
    public static Schema schemaFor(List<String> fieldNames, List<String> fieldTypes) throws RuntimeException {
        FieldAssembler<Schema> assembler = SchemaBuilder.record("t").fields();
        
        for (int i = 0; i < fieldNames.size(); i++) {
            String fieldName = fieldNames.get(i);
            
            if (!fieldName.matches("^[A-Za-z_].*")) {
                fieldName = "_" + fieldName;
            }
            
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
                default:
                    throw new RuntimeException("Unsupported provided field type: " + fieldType);
            }
        }
        
        return assembler.endRecord();
    }
    
    public static Map<GenericRecord, List<GenericRecord>> recordsByKey(List<GenericRecord> records, List<String> keyFieldNames) throws RuntimeException {
        Map<GenericRecord, List<GenericRecord>> recordsByKey = Maps.newHashMap();
        
        for (GenericRecord record : records) {
            GenericRecord key = RecordUtils.subsetRecord(record, keyFieldNames);
            
            if (!recordsByKey.containsKey(key)) {
                recordsByKey.put(key, new ArrayList<GenericRecord>());
            }
            
            List<GenericRecord> rowsForKey = recordsByKey.get(key);
            rowsForKey.add(record);
        }
        
        return recordsByKey;
    }
    
    public static boolean different(GenericRecord first, GenericRecord second, List<String> valueFieldNames) throws RuntimeException {
        boolean differenceFound = false;
        
        GenericRecord firstValues = RecordUtils.subsetRecord(first, valueFieldNames);
        GenericRecord secondValues = RecordUtils.subsetRecord(second, valueFieldNames);
        
        for (int i = 0; i < firstValues.getSchema().getFields().size(); i++) {
            Object firstValue = firstValues.get(i);
            Object secondValue = secondValues.get(i);
            
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
