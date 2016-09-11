package com.cloudera.labs.envelope.utils;

import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.google.common.collect.Lists;
import com.google.common.collect.ObjectArrays;

import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.runtime.AbstractFunction1;

public class RowUtils {
    
    public static StructType subsetSchema(StructType schema, final List<String> fieldNames) {
        Seq<StructField> fieldSeq = schema.toTraversable().filter(new AbstractFunction1<StructField, Object>() {
            @Override
            public Object apply(StructField field) {
                return fieldNames.contains(field.name());
            }
        }).toSeq();
        
        StructType subset = DataTypes.createStructType(JavaConversions.seqAsJavaList(fieldSeq));
        
        return subset;
    }
    
    public static Row subsetRow(Row row, StructType subsetSchema) {
        Object[] values = new Object[subsetSchema.length()];
        
        int i = 0;
        for (String fieldName : subsetSchema.fieldNames()) {
            values[i] = row.get(row.fieldIndex(fieldName));
            i++;
        }
        
        Row subset = new RowWithSchema(subsetSchema, values);
        
        return subset;
    }
    
    public static Object get(Row row, String fieldName) {
        return row.get(row.fieldIndex(fieldName));
    }
    
    public static Row set(Row row, String fieldName, Object replacement) {
        Object[] values = new Object[row.length()];
        
        for (int i = 0; i < row.schema().fields().length; i++) {
            if (i == row.fieldIndex(fieldName)) {
                values[i] = replacement;
            }
            else {
                values[i] = row.get(i);
            }
        }
        
        Row replacedRow = new RowWithSchema(row.schema(), values);
        
        return replacedRow;
    }
    
    public static Row append(Row row, String fieldName, DataType fieldType, Object value) {
        StructType appendedSchema = row.schema().add(fieldName, fieldType);
        Object[] appendedValues = ObjectArrays.concat(valuesFor(row), value);
        Row appendedRow = new RowWithSchema(appendedSchema, appendedValues);
        
        return appendedRow;
    }
    
    public static StructType appendFields(StructType from, List<StructField> fields) {
        StructType to = DataTypes.createStructType(from.fields());
        
        for (StructField field : fields) {
            to = to.add(field);
        }
        
        return to;
    }
    
    public static Object[] valuesFor(Row row) {
        Object[] values = new Object[row.length()];
        
        for (int i = 0; i < row.schema().fields().length; i++) {
            values[i] = row.get(i);
        }
        
        return values;
    }
    
    public static StructType structTypeFor(List<String> fieldNames, List<String> fieldTypes) {
        List<StructField> fields = Lists.newArrayList();
        
        for (int i = 0; i < fieldNames.size(); i++) {
            String fieldName = fieldNames.get(i);
            String fieldType = fieldTypes.get(i);
            
            StructField field;
            switch (fieldType) {
                case "string":
                    field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
                    break;
                case "float":
                    field = DataTypes.createStructField(fieldName, DataTypes.FloatType, true);
                    break;
                case "double":
                    field = DataTypes.createStructField(fieldName, DataTypes.DoubleType, true);
                    break;
                case "int":
                    field = DataTypes.createStructField(fieldName, DataTypes.IntegerType, true);
                    break;
                case "long":
                    field = DataTypes.createStructField(fieldName, DataTypes.LongType, true);
                    break;
                case "boolean":
                    field = DataTypes.createStructField(fieldName, DataTypes.BooleanType, true);
                    break;
                default:
                    throw new RuntimeException("Unsupported provided field type: " + fieldType);
            }
          
            fields.add(field);
        }
        
        StructType schema = DataTypes.createStructType(fields);
        
        return schema;
    }
    
    public static boolean different(Row first, Row second, List<String> valueFieldNames) {
        for (String valueFieldName : valueFieldNames) {
            Object firstValue = first.get(first.fieldIndex(valueFieldName));
            Object secondValue = second.get(first.fieldIndex(valueFieldName));
            
            if (firstValue != null && secondValue != null && !firstValue.equals(secondValue)) {
                return true;
            }
            
            if ((firstValue != null && secondValue == null) || (firstValue == null && secondValue != null)) {
                return true;
            }
        }
        
        return false;
    }
    
    public static Long precedingTimestamp(Long timestamp) {
        return timestamp - 1;
    }
    
    public static boolean before(Row first, Row second, String timestampFieldName) {
        return compareTimestamp(first, second, timestampFieldName) == -1;
    }
    
    public static boolean after(Row first, Row second, String timestampFieldName) {
        return compareTimestamp(first, second, timestampFieldName) == 1;
    }
    
    public static boolean simultaneous(Row first, Row second, String timestampFieldName) {
        return compareTimestamp(first, second, timestampFieldName) == 0;
    }
    
    public static int compareTimestamp(Row first, Row second, String timestampFieldName) {
        Long ts1 = (Long)first.get(first.fieldIndex(timestampFieldName));
        Long ts2 = (Long)second.get(second.fieldIndex(timestampFieldName));
        if      (ts1 < ts2) return -1;
        else if (ts1 > ts2) return 1;
        else return 0;
    }
    
}
