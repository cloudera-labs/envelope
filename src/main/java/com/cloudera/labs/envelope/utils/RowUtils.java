/**
 * Copyright © 2016-2017 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.utils;

import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.google.common.collect.Lists;
import com.google.common.collect.ObjectArrays;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.runtime.AbstractFunction1;

public class RowUtils {

  private static final DateTimeFormatter dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd").withZoneUTC();
  private static final DateTimeFormatter timestampFormat = ISODateTimeFormat.dateTime().withZoneUTC();

  /**
   * <p>Converts a Java object (simple or compound, e.g. Maps and Arrays) or Row object (for Arrays, Maps, and
   * StructType) into a {@link Row} compatible value or values. In the latter case for Maps, the incoming Row must have
   * an associated schema and results in a MapType<String, Object>.</p>
   * <p>NOTE: If there is a conversion error with a complex type (Map, Array, StructType), the function will set
   * the returned value for the sub-element to 'null' if the DataType allows it, i.e. {@link ArrayType#containsNull()}.
   * </p>
   * <p>NOTE: Does not handle the following DataTypes:</p>
   * <ul>
   * <li>{@link org.apache.spark.sql.types.DecimalType}</li>
   * <li>{@link org.apache.spark.sql.types.UserDefinedType}</li>
   * <li>{@link org.apache.spark.sql.types.CalendarIntervalType}</li>
   * </ul>
   *
   * @param item The value for conversion
   * @param type The DataType of the field
   * @return A Row-compatible value
   */
  public static Object toRowValue(Object item, DataType type) {

    String typeName = type.typeName();
    // Binary, Boolean, Byte, Date, Null, Decimal, Double, Float, Integer, Long, Short, String, Timestamp
    // Array, Map, CalendarInterval, Struct, UserDefined

    switch (typeName) {
      case "binary":
        if (item instanceof ByteBuffer) {
          return item;
        } else if (item instanceof byte[]) {
          return ByteBuffer.wrap((byte[]) item);
        } else {
          throw new RuntimeException(String.format("Type[%s] - Invalid or unrecognized input format", type));
        }
      case "boolean":
        if (item instanceof Boolean) {
          return item;
        }
        String str = item.toString();
        if ("true".equals(str)) {
          return Boolean.TRUE;
        } else if ("false".equals(str)) {
          return Boolean.FALSE;
        } else {
          throw new RuntimeException(String.format("Type[%s] - Invalid or unrecognized input format: %s", type, item));
        }
      case "date":
        if (item instanceof Long) {
          return dateFormat.print((Long) item);
        } else if (item instanceof String) {
          return dateFormat.print(DateTime.parse((String) item));
        } else if (item instanceof Date) {
          return dateFormat.print(new DateTime((Date) item));
        } else if (item instanceof DateTime) {
          return dateFormat.print((DateTime) item);
        } else {
          throw new RuntimeException(String.format("Type[%s] - Invalid or unrecognized input format: %s", type, item));
        }
      case "timestamp":
        if (item instanceof Long) {
          return timestampFormat.print((Long) item);
        } else if (item instanceof String) {
          return timestampFormat.print(DateTime.parse((String) item));
        } else if (item instanceof Date) {
          return timestampFormat.print(new DateTime((Date) item));
        } else if (item instanceof DateTime) {
          return timestampFormat.print((DateTime) item);
        } else {
          throw new RuntimeException(String.format("Type[%s] - Invalid or unrecognized input format: %s", type, item));
        }
      case "double":
        if (item instanceof Double) {
          return item;
        } else if (item instanceof Number) {
          return ((Number) item).doubleValue();
        } else {
          try {
            return Double.parseDouble((String) item);
          } catch (Exception ex) {
            throw new RuntimeException(String.format("Type[%s] - Invalid or unrecognized input format: %s", type, item));
          }
        }
      case "float":
        if (item instanceof Float) {
          return item;
        } else if (item instanceof Number) {
          return ((Number) item).floatValue();
        } else {
          try {
            return Float.parseFloat((String) item);
          } catch (Exception e) {
            throw new RuntimeException(String.format("Type[%s] - Invalid or unrecognized input format: %s", type, item));
          }
        }
      case "integer":
        if (item instanceof Integer) {
          return item;
        } else if (item instanceof Number) {
          return ((Number) item).intValue();
        } else {
          try {
            return Integer.parseInt((String) item);
          } catch (Exception e) {
            throw new RuntimeException(String.format("Type[%s] - Invalid or unrecognized input format: %s", type, item));
          }
        }
      case "long":
        if (item instanceof Long) {
          return item;
        } else if (item instanceof Number) {
          return ((Number) item).longValue();
        } else {
          try {
            return Long.parseLong((String) item);
          } catch (Exception e) {
            throw new RuntimeException(String.format("Type[%s] - Invalid or unrecognized input format: %s", type, item));
          }
        }
      case "null":
        if (item == null) {
          return null;
        } else {
          throw new RuntimeException(String.format("Type[%s] - Invalid or unrecognized input format: %s", type, item));
        }
      case "byte":
      case "short":
        if (item instanceof Byte) {
          return item;
        } else if (item instanceof Number) {
          return ((Number) item).byteValue();
        } else {
          try {
            return Byte.parseByte((String) item);
          } catch (Exception e) {
            throw new RuntimeException(String.format("Type[%s] - Invalid or unrecognized input format: %s", type, item));
          }
        }
      case "string":
        return item.toString();
      case "array":
        DataType elementType = ((ArrayType) type).elementType();
        ArrayList<Object> arrayList = new ArrayList<>();

        if (item instanceof List) {
          for (Object value : (List<?>) item) {

            if (null != value) {
              try {
                value = toRowValue(value, elementType);
              } catch (Exception e) {
                throw new RuntimeException(String.format("Type[%s] - Invalid or unrecognized element format: %s[%s]",
                    type, elementType, value));
              }
            } else if (!((ArrayType) type).containsNull()) {
              throw new RuntimeException(String.format("Type[%s] - Element cannot be 'null': %s[null]",
                  type, elementType));
            }

            arrayList.add(value);
          }

          return arrayList;
        } else if (item instanceof Row) {
          Row row = (Row) item;

          for (int i = 0; i < row.length(); i++) {
            Object value = row.get(i);

            if (null != value) {
              try {
                value = toRowValue(value, elementType);
              } catch (Exception e) {
                throw new RuntimeException(String.format("Type[%s] - Invalid or unrecognized value format: %s[%s]",
                    type, elementType, value));
              }
            } else if (!((ArrayType) type).containsNull()) {
              throw new RuntimeException(String.format("Type[%s] - Value cannot be 'null': %s[null]",
                  type, elementType));

            }

            arrayList.add(value);
          }

          return arrayList;
        } else {
          throw new RuntimeException(String.format("Type[%s] - Invalid or unrecognized input format: %s", type, item));
        }
      case "map":
        MapType mapType = (MapType) type;
        DataType keyType = mapType.keyType();
        DataType valueType = mapType.valueType();
        HashMap<Object, Object> hashMap = new HashMap<>();

        if (item instanceof Map) {
          Map<?, ?> map = (Map<?, ?>) item;

          for (Object k : map.keySet()) {
            Object key, value;

            try {
              key = toRowValue(k, keyType);
            } catch (Exception e) {
              throw new RuntimeException(String.format("Type[%s] - Invalid or unrecognized key format: %s[%s]", type,
                  keyType, k));
            }

            value = map.get(k);

            if (null != value) {
              try {
                value = toRowValue(value, valueType);
              } catch (Exception e) {
                throw new RuntimeException(String.format("Type[%s] - Invalid or unrecognized value format: %s[%s]",
                    type, valueType, value));
              }
            } else if (!mapType.valueContainsNull()) {
              throw new RuntimeException(String.format("Type[%s] - Value cannot be 'null': %s[null]",
                  type, valueType));
            }

            hashMap.put(key, value);
          }

          return hashMap;
        } else if (item instanceof Row) {
          // Convert a Row into a Map; only if there is a schema and only if the values can convert
          Row row = (Row) item;

          StructType schema = row.schema();
          if (null == schema) {
            throw new RuntimeException(String.format("Type[%s] - Invalid Row format, no schema found: Row[%s]", type,
                item));
          }

          String[] fieldNames = schema.fieldNames();
          for (int i = 0; i < fieldNames.length; i++) {
            Object value = row.get(i);

            if (null != value) {
              try {
                value = toRowValue(value, valueType);
              } catch (Exception e) {
                throw new RuntimeException(String.format("Type[%s] - Invalid or unrecognized value format: %s[%s]",
                    type, valueType, value));
              }
            } else if (!mapType.valueContainsNull()) {
              throw new RuntimeException(String.format("Type[%s] - Value cannot be 'null': %s[null]",
                  type, valueType));
            }

            hashMap.put(fieldNames[i], value);
          }

          return hashMap;
        } else {
          throw new RuntimeException(String.format("Type[%s] - Invalid or unrecognized input format: %s", type, item));
        }
      case "struct":
        ArrayList<Object> valueList = new ArrayList<>();

        if (item instanceof Map) {
          // Keys must be Strings and match the names of the fields
          // Values must convert to field DataTypes

          @SuppressWarnings("unchecked")
          Map<Object, Object> input = (Map<Object, Object>) item;

          for (StructField f : ((StructType) type).fields()) {
            if (input.containsKey(f.name())) {
              Object value = input.get(f.name());

              if (null != value) {
                try {
                  value = toRowValue(value, f.dataType());
                } catch (Exception e) {
                  throw new RuntimeException(String.format("Type[%s] - Invalid or unrecognized value format: %s[%s]",
                      type, f.dataType(), value));
                }
              } else if (!f.nullable()) {
                throw new RuntimeException(String.format("Type[%s] - Value cannot be 'null': %s[null]",
                    type, f.dataType()));
              }

              valueList.add(value);
            } else {
              throw new RuntimeException(String.format("Type[%s] - Key not found on input: %s[%s]", type, f.name(),
                  f.dataType()));
            }
          }

          return valueList;
        } else if (item instanceof List) {
          // An exact count, in-order list of values for the StructType
          // Values must convert to the field DataTypes

          @SuppressWarnings("unchecked")
          List<Object> input = (List<Object>) item;
          StructField[] fields = ((StructType) type).fields();

          if (fields.length != input.size()) {
            throw new RuntimeException(String.format("Type[%s] - Invalid size of input List: %s", type, item));
          }

          for (int i = 0; i < fields.length; i++) {
            Object value = input.get(i);

            if (null != value) {
              try {
                value = toRowValue(value, fields[i].dataType());
              } catch (Exception e) {
                throw new RuntimeException(String.format("Type[%s] - Invalid or unrecognized element format: %s[%s]", type,
                    fields[i].dataType(), value));
              }
            } else if (!fields[i].nullable()) {
              throw new RuntimeException(String.format("Type[%s] - Element cannot be 'null': %s[null]",
                  type, fields[i].dataType()));

            }

            valueList.add(value);
          }

          return valueList;
        } else if (item instanceof Row) {
          // An exact count, in-order list of values for the StructType
          // Values must convert to the field DataTypes

          Row input = (Row) item;
          StructField[] fields = ((StructType) type).fields();

          if (fields.length != input.size()) {
            throw new RuntimeException(String.format("Type[%s] - Invalid size of input Row: %s", type, item));
          }

          for (int i = 0; i < fields.length; i++) {
            Object value = input.get(i);

            if (null != value) {
              try {
                value = toRowValue(value, fields[i].dataType());
              } catch (Exception e) {
                throw new RuntimeException(String.format("Type[%s] - Invalid or unrecognized value format: %s[%s]",
                    type, fields[i].dataType(), value));
              }
            } else if (!fields[i].nullable()) {
              throw new RuntimeException(String.format("Type[%s] - Value cannot be 'null': %s[null]",
                  type, fields[i].dataType()));
            }

            valueList.add(value);
          }

          return valueList;
        } else {
          throw new RuntimeException(String.format("Type[%s] - Invalid or unrecognized input format: %s", type, item));
        }
      default:
        throw new RuntimeException(String.format("Type[%s] - StructField DataType unrecognized or not yet implemented",
            type));
    }
  }

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

  public static <T> T getAs(Class<T> clazz, Row row, String fieldName) {
    return row.getAs(row.fieldIndex(fieldName));
  }

  public static Row set(Row row, String fieldName, Object replacement) {
    Object[] values = new Object[row.length()];

    for (int i = 0; i < row.schema().fields().length; i++) {
      if (i == row.fieldIndex(fieldName)) {
        values[i] = replacement;
      } else {
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
    Long ts1 = (Long) first.get(first.fieldIndex(timestampFieldName));
    Long ts2 = (Long) second.get(second.fieldIndex(timestampFieldName));
    if (ts1 < ts2) {
      return -1;
    } else if (ts1 > ts2) {
      return 1;
    } else {
      return 0;
    }
  }

}
