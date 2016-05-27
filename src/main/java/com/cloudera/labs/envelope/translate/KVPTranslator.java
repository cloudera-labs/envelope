package com.cloudera.labs.envelope.translate;

import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.cloudera.labs.envelope.utils.PropertiesUtils;
import com.cloudera.labs.envelope.utils.RecordUtils;

/**
 * A translator implementation for text key-value pair messages.
 */
public class KVPTranslator extends Translator {
    
    private String kvpDelimiter;
    private String fieldDelimiter;
    private List<String> fieldNames;
    private List<String> fieldTypes;
    private Schema schema;
    
    public KVPTranslator(Properties props) {
        super(props);
        
        kvpDelimiter = resolveDelimiter(props.getProperty("translator.kvp.delimiter.kvp"));
        fieldDelimiter = resolveDelimiter(props.getProperty("translator.kvp.delimiter.field"));
        fieldNames = PropertiesUtils.propertyAsList(props, "translator.kvp.field.names");
        fieldTypes = PropertiesUtils.propertyAsList(props, "translator.kvp.field.types");
        schema = RecordUtils.schemaFor(fieldNames, fieldTypes);
    }
    
    @Override
    public GenericRecord translate(String key, String message) {
        String[] kvps = message.split(Pattern.quote(kvpDelimiter));
        
        GenericRecord record = new GenericData.Record(schema);
        
        for (int kvpPos = 0; kvpPos < kvps.length; kvpPos++) {
            String[] components = kvps[kvpPos].split(Pattern.quote(fieldDelimiter));
            
            String rawKvpKey = components[0];
            String kvpKey = RecordUtils.compatibleFieldName(rawKvpKey);
            String kvpValue = components[1];
            
            switch (fieldTypes.get(fieldNames.indexOf(rawKvpKey))) {
                case "string":
                    record.put(kvpKey, kvpValue);
                    break;
                case "float":
                    record.put(kvpKey, Float.parseFloat(kvpValue));
                    break;
                case "double":
                    record.put(kvpKey, Double.parseDouble(kvpValue));
                    break;
                case "int":
                    record.put(kvpKey, Integer.parseInt(kvpValue));
                    break;
                case "long":
                    record.put(kvpKey, Long.parseLong(kvpValue));
                    break;
                case "boolean":
                    record.put(kvpKey, Boolean.parseBoolean(kvpValue));
                    break;
                default:
                    throw new RuntimeException("Unsupported KVP field type: " + fieldTypes.get(kvpPos));
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

}
