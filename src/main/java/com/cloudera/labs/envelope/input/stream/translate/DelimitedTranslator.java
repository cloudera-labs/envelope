package com.cloudera.labs.envelope.input.stream.translate;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;

import com.cloudera.labs.envelope.utils.RowUtils;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

/**
 * A translator implementation for plain delimited text messages, e.g. CSV.
 */
public class DelimitedTranslator extends Translator<String> {
    
    private String delimiter;
    private List<String> fieldNames;
    private List<String> fieldTypes;
    private StructType schema;
    private List<Object> values = Lists.newArrayList();
    
    public static final String DELIMITER_CONFIG_NAME = "delimiter";
    public static final String FIELD_NAMES_CONFIG_NAME = "field.names";
    public static final String FIELD_TYPES_CONFIG_NAME = "field.types";
    
    public DelimitedTranslator(Config config) {
        super(config);
        
        delimiter = resolveDelimiter(config.getString(DELIMITER_CONFIG_NAME));
        fieldNames = config.getStringList(FIELD_NAMES_CONFIG_NAME);
        fieldTypes = config.getStringList(FIELD_TYPES_CONFIG_NAME);
        schema = RowUtils.structTypeFor(fieldNames, fieldTypes);
    }
    
    @Override
    public Row translate(String key, String message) {
        String[] stringValues = message.split(Pattern.quote(delimiter));
        values.clear();
        
        for (int valuePos = 0; valuePos < stringValues.length; valuePos++) {
            String fieldValue = stringValues[valuePos];
            
            switch (fieldTypes.get(valuePos)) {
                case "string":
                    values.add(fieldValue);
                    break;
                case "float":
                    values.add(Float.parseFloat(fieldValue));
                    break;
                case "double":
                    values.add(Double.parseDouble(fieldValue));
                    break;
                case "int":
                    values.add(Integer.parseInt(fieldValue));
                    break;
                case "long":
                    values.add(Long.parseLong(fieldValue));
                    break;
                case "boolean":
                    values.add(Boolean.parseBoolean(fieldValue));
                    break;
                default:
                    throw new RuntimeException("Unsupported delimited field type: " + fieldTypes.get(valuePos));
            }
        }
        
        Row row = RowFactory.create(values.toArray());
        
        return row;
    }
    
    @Override
    public StructType getSchema() {
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
