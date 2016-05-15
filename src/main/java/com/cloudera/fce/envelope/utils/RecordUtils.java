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

/**
 * Convenience utility methods for working with Avro records in Envelope.
 */
public class RecordUtils {
    
    /**
     * The subset of the original schema that only has the given fields.
     * @param schema The schema to take the subset from.
     * @param fieldNames The names of the fields to be included in the subset.
     * @return The subset schema.
     */
    public static Schema subsetSchema(Schema schema, List<String> fieldNames) {
        return subsetSchema(schema, fieldNames, null);
    }
    
    /**
     * The subset of the original schema that only has the given fields and with fields renamed.
     * @param schema The schema to take the subset from.
     * @param fieldNames The names of the fields to be included in the subset.
     * @param renames The mapping of original schema field names to subset schema field names.
     * @return The subset schema with fields renamed.
     */
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

    /**
     * The subset of the original record that contains the values for the given subset schema.
     * @param record The original record.
     * @param subsetSchema The schema of the fields whose values are included in the subset record.
     * @return The subset record.
     */
    public static GenericRecord subsetRecord(GenericRecord record, Schema subsetSchema) {
        return subsetRecord(record, subsetSchema, null);
    }
    
    /**
     * The subset of the original record that contains the values for the given subset schema and
     * with fields renamed.
     * @param record The original record.
     * @param subsetSchema The schema of the fields whose values are included in the subset record.
     * @param renames The mapping of the original record field names to subset record field names.
     * @return The subset record with fields renamed.
     */
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
    
    /**
     * The original field name adjusted, if necessary, to conform to Avro's field name requirements.
     * If the original field name does not conform then it is prefixed with "field_". 
     * @param original The original field name.
     * @return The adjusted-if-necessary field name.
     */
    public static String compatibleFieldName(String original) {
        if (!original.matches("^[A-Za-z_].*")) {
            return "field_" + original;
        }
        else {
            return original;
        }
    }
    
    /**
     * The Avro schema for the given field names and types. All fields are marked as optional.
     * @param fieldNames The field names for the schema, added in provided order.
     * @param fieldTypes The field types for the schema, mapped to the field names by the same order.
     * @return The Avro schema.
     */
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
    
    /**
     * The records grouped by their key, where the key is defined by a subset of field names.
     * @param records The records to be grouped.
     * @param keyFieldNames The field names that constitute the key of the records.
     * @return The records grouped by their key, where the key is a subset record of the original record.
     */
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
    
    /**
     * Whether the two records are considered different, based on the values of the fields that have
     * been provided to check for differences.
     * @param first The first record to compare.
     * @param second The second record to compare.
     * @param valueFieldNames The list of field names to check for different values across the records.
     * @return True if the records are considered different, or false otherwise.
     */
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
    
    /**
     * The immediately preceding instant of the given timestamp. Envelope defines timestamps as
     * unit-less longs, so the preceding timestamp is the given timestamp minus one. 
     * @param timestamp The given timestamp.
     * @return The immediately preceding instant.
     */
    public static Long precedingTimestamp(Long timestamp) {
        return timestamp - 1;
    }
    
    /**
     * Whether the first record was at a point in time before that of the second record.
     * @param first The first record.
     * @param second The second record.
     * @param timestampFieldName The timestamp field name of the records.
     * @return True if the first record was before the second record, false otherwise.
     */
    public static boolean before(GenericRecord first, GenericRecord second, String timestampFieldName) {
        return compareTimestamp(first, second, timestampFieldName) == -1;
    }
    
    /**
     * Whether the first record was at a point in time after that of the second record.
     * @param first The first record.
     * @param second The second record.
     * @param timestampFieldName The timestamp field name of the records.
     * @return True if the first record was after the second record, false otherwise.
     */
    public static boolean after(GenericRecord first, GenericRecord second, String timestampFieldName) {
        return compareTimestamp(first, second, timestampFieldName) == 1;
    }
    
    /**
     * Whether the first record was at the same point in time as that of the second record.
     * @param first The first record.
     * @param second The second record.
     * @param timestampFieldName The timestamp field name of the records.
     * @return True if the first record was at the same time of the second record, false otherwise.
     */
    public static boolean simultaneous(GenericRecord first, GenericRecord second, String timestampFieldName) {
        return compareTimestamp(first, second, timestampFieldName) == 0;
    }
    
    /**
     * Compare the two timestamps of the records.
     * @param first The first record to compare.
     * @param second The second record to compare.
     * @param timestampFieldName The timestamp field name of the records.
     * @return 0 if the records are at the same point in time, -1 if the first record is before the
     * second record, or 1 if the first record is after the second record. 
     */
    public static int compareTimestamp(GenericRecord first, GenericRecord second, String timestampFieldName) {
        Long ts1 = (Long)first.get(timestampFieldName);
        Long ts2 = (Long)second.get(timestampFieldName);
        if      (ts1 < ts2) return -1;
        else if (ts1 > ts2) return 1;
        else return 0;
    }
    
    /**
     * A Comparator implementation for Envelope record timestamps. 
     */
    public static class TimestampComparator implements Comparator<GenericRecord> {
        private String timestampFieldName;
        
        public TimestampComparator(String timestampFieldName) {
            this.timestampFieldName = timestampFieldName;
        }
        
        @Override
        public int compare(GenericRecord r1, GenericRecord r2) {
            return compareTimestamp(r1, r2, timestampFieldName);
        }
    }
    
}
