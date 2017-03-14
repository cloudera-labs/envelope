package com.cloudera.labs.envelope.input.translate;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.cloudera.labs.envelope.utils.RowUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;

/**
 * A translator implementation for text key-value pair messages.
 */
public class KVPTranslator implements Translator<String> {

  private String kvpDelimiter;
  private String fieldDelimiter;
  private List<String> fieldNames;
  private List<String> fieldTypes;
  private StructType schema;
  private List<Object> values = Lists.newArrayList();
  private Map<String, String> kvpMap = Maps.newHashMap();

  public static final String KVP_DELIMITER_CONFIG_NAME = "delimiter.kvp";
  public static final String FIELD_DELIMITER_CONFIG_NAME = "delimiter.field";
  public static final String FIELD_NAMES_CONFIG_NAME = "field.names";
  public static final String FIELD_TYPES_CONFIG_NAME = "field.types";

  @Override
  public void configure(Config config) {
    kvpDelimiter = resolveDelimiter(config.getString(KVP_DELIMITER_CONFIG_NAME));
    fieldDelimiter = resolveDelimiter(config.getString(FIELD_DELIMITER_CONFIG_NAME));
    fieldNames = config.getStringList(FIELD_NAMES_CONFIG_NAME);
    fieldTypes = config.getStringList(FIELD_TYPES_CONFIG_NAME);
    schema = RowUtils.structTypeFor(fieldNames, fieldTypes);
  }

  @Override
  public Iterable<Row> translate(String key, String message) {
    kvpMap.clear();
    values.clear();

    String[] kvps = message.split(Pattern.quote(kvpDelimiter));
    for (String kvp : kvps) {
      String[] components = kvp.split(Pattern.quote(fieldDelimiter));
      String kvpKey = components[0];
      String kvpValue = components[1];

      kvpMap.put(kvpKey, kvpValue);
    }

    for (StructField field : schema.fields()) {
      String fieldName = field.name();
      if (kvpMap.containsKey(fieldName)) {
        String kvpValue = kvpMap.get(fieldName);

        if (field.dataType().equals(DataTypes.StringType)) {
          values.add(kvpValue);
        }
        else if (field.dataType().equals(DataTypes.FloatType)) {
          values.add(Float.parseFloat(kvpValue));
        }
        else if (field.dataType().equals(DataTypes.DoubleType)) {
          values.add(Double.parseDouble(kvpValue));
        }
        else if (field.dataType().equals(DataTypes.IntegerType)) {
          values.add(Integer.parseInt(kvpValue));
        }
        else if (field.dataType().equals(DataTypes.LongType)) {
          values.add(Long.parseLong(kvpValue));
        }
        else if (field.dataType().equals(DataTypes.BooleanType)) {
          values.add(Boolean.parseBoolean(kvpValue));
        }
        else {
          throw new RuntimeException("Unsupported KVP field type: " + field.dataType());
        }
      }
      else {
        values.add(null);
      }
    }

    Row row = RowFactory.create(values.toArray());

    return Collections.singleton(row);
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
