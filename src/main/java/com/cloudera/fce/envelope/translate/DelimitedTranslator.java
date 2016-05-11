package com.cloudera.fce.envelope.translate;

import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.cloudera.fce.envelope.utils.PropertiesUtils;
import com.cloudera.fce.envelope.utils.RecordUtils;

public class DelimitedTranslator extends Translator {
    
    private String delimiter;
    private List<String> fieldNames;
    private List<String> fieldTypes;
    private Schema schema;
    
    public DelimitedTranslator(Properties props) {
        super(props);
        
        delimiter = resolveDelimiter(props.getProperty("translator.delimited.delimiter"));
        fieldNames = PropertiesUtils.propertyAsList(props, "translator.delimited.field.names");
        fieldTypes = PropertiesUtils.propertyAsList(props, "translator.delimited.field.types");
        schema = RecordUtils.schemaFor(fieldNames, fieldTypes);
    }
    
    @Override
    public GenericRecord translate(String key, String message) {
        String[] values = message.split(Pattern.quote(delimiter));
        
        GenericRecord record = new GenericData.Record(schema);
        
        for (int valuePos = 0; valuePos < values.length; valuePos++) {
            String fieldName = fieldNames.get(valuePos);
            String fieldValue = values[valuePos];
            
            switch (fieldTypes.get(valuePos)) {
                case "string":
                    record.put(fieldName, fieldValue);
                    break;
                case "float":
                    record.put(fieldName, Float.parseFloat(fieldValue));
                    break;
                case "double":
                    record.put(fieldName, Double.parseDouble(fieldValue));
                    break;
                case "int":
                    record.put(fieldName, Integer.parseInt(fieldValue));
                    break;
                case "long":
                    record.put(fieldName, Long.parseLong(fieldValue));
                    break;
                case "boolean":
                    record.put(fieldName, Boolean.parseBoolean(fieldValue));
                    break;
                default:
                    throw new RuntimeException("Unsupported delimited field type: " + fieldTypes.get(valuePos));
            }
        }
        
        return record;
    }
    
    @Override
    public Schema getSchema() {
        return schema;
    }
    
    private String resolveDelimiter(String delimiterArg) {
        if (delimiterArg.startsWith("chars:")) {
            String[] codePoints = delimiterArg.substring("chars:".length()).split(",");
            
            StringBuilder delimiter = new StringBuilder();
            for (String codePoint : codePoints) {
                delimiter.append(Character.toChars(Integer.parseInt(codePoint)));
            }
            
            return delimiter.toString();
        }
        else {
            return delimiterArg;
        }
    }

    @Override
    public MessageEncoding acceptsType() {
        return MessageEncoding.STRING;
    }

}
