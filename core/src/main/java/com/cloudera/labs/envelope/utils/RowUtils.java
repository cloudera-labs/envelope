/*
 * Copyright (c) 2015-2018, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.labs.envelope.utils;

import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.google.common.collect.Lists;
import com.google.common.collect.ObjectArrays;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.joda.time.DateTime;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.runtime.AbstractFunction1;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RowUtils {

  /**
   * <p>Converts a Java object (simple or compound, e.g. Maps and Arrays) or Row object (for Arrays, Maps, and
   * StructType) into a {@link Row} compatible value or values. In the latter case for Maps, the incoming Row must have
   * an associated schema and results in a MapType<String, Object>.</p>
   * <p>NOTE: If there is a conversion error with a complex type (Map, Array, StructType), the function will set
   * the returned value for the sub-element to 'null' if the DataType allows it, i.e. {@link ArrayType#containsNull()}.
   * </p>
   * <p>NOTE: Does not handle the following DataTypes:</p>
   * <ul>
   * <li>{@link org.apache.spark.sql.types.UserDefinedType}</li>
   * <li>{@link org.apache.spark.sql.types.CalendarIntervalType}</li>
   * </ul>
   * <p>Details for accepted conversion values.</p>
   * <dl>
   *   <dt>BinaryType</dt>
   *   <dd>
   *     <ul>
   *       <li>ByteBuffer</li>
   *       <li>byte[]</li>
   *     </ul>
   *   </dd>
   *   <dt>BooleanType</dt>
   *   <dd>
   *     <ul>
   *       <li>Boolean</li>
   *       <li>Case-insensitive String value of <code>true</code> or <code>false</code></li>
   *     </ul>
   *   </dd>
   *   <dt>DateType</dt>
   *   <dd>
   *     <ul>
   *      <li>Long value of milliseconds since epoch</li>
   *      <li>String value of milliseconds since epoch</li>
   *      <li>{@link java.util.Date} value</li>
   *      <li>{@link org.joda.time.DateTime} value</li>
   *      <li>or the following String value:<br/>
   *      <pre>
   datetime          = time | date-opt-time
   time              = 'T' time-element [offset]
   date-opt-time     = date-element ['T' [time-element] [offset]]
   date-element      = std-date-element | ord-date-element | week-date-element
   std-date-element  = yyyy ['-' MM ['-' dd]]
   ord-date-element  = yyyy ['-' DDD]
   week-date-element = xxxx '-W' ww ['-' e]
   time-element      = HH [minute-element] | [fraction]
   minute-element    = ':' mm [second-element] | [fraction]
   second-element    = ':' ss [fraction]
   fraction          = ('.' | ',') digit+
   offset            = 'Z' | (('+' | '-') HH [':' mm [':' ss [('.' | ',') SSS]]])
          </pre>
   *      </li>
   *     </ul>
   *   </dd>
   *   <dt>TimestampType</dt>
   *   <dd>
   *     <ul>
   *      <li>Long value of milliseconds since epoch</li>
   *      <li>String value of milliseconds since epoch</li>
   *      <li>{@link java.util.Date} value</li>
   *      <li>{@link org.joda.time.DateTime} value</li>
   *      <li>or the following String value:<br/>
   *      <pre>
   datetime          = time | date-opt-time
   time              = 'T' time-element [offset]
   date-opt-time     = date-element ['T' [time-element] [offset]]
   date-element      = std-date-element | ord-date-element | week-date-element
   std-date-element  = yyyy ['-' MM ['-' dd]]
   ord-date-element  = yyyy ['-' DDD]
   week-date-element = xxxx '-W' ww ['-' e]
   time-element      = HH [minute-element] | [fraction]
   minute-element    = ':' mm [second-element] | [fraction]
   second-element    = ':' ss [fraction]
   fraction          = ('.' | ',') digit+
   offset            = 'Z' | (('+' | '-') HH [':' mm [':' ss [('.' | ',') SSS]]])
   </pre>
   *      </li>
   *     </ul>
   *     <p><em>Note:</em> The precision is limited to milliseconds; nanosecond resolution is not supported.</p>
   *   </dd>
   *   <dt>DoubleType</dt>
   *   <dd>
   *     <ul>
   *       <li>Double</li>
   *       <li>Number</li>
   *       <li>String value of a Double</li>
   *     </ul>
   *   </dd>
   *   <dt>FloatType</dt>
   *   <dd>
   *     <ul>
   *       <li>Float</li>
   *       <li>Number</li>
   *       <li>String value of a Float</li>
   *     </ul>
   *   </dd>
   *   <dt>IntegerType</dt>
   *   <dd>
   *     <ul>
   *       <li>Integer</li>
   *       <li>Number</li>
   *       <li>String value of an Integer</li>
   *     </ul>
   *   </dd>
   *   <dt>LongType</dt>
   *   <dd>
   *     <ul>
   *       <li>Long</li>
   *       <li>Number</li>
   *       <li>String value of a Long</li>
   *     </ul>
   *   </dd>
   *   <dt>NullType</dt>
   *   <dd>
   *     <ul>
   *       <li><code>null</code></li>
   *     </ul>
   *   </dd>
   *   <dt>ByteType</dt>
   *   <dd>
   *     <ul>
   *       <li>Byte</li>
   *       <li>Number</li>
   *       <li>String value of a Byte</li>
   *     </ul>
   *   </dd>
   *   <dt>ShortType</dt>
   *   <dd>
   *     <ul>
   *       <li>Short</li>
   *       <li>Number</li>
   *       <li>String value of a Short</li>
   *     </ul>
   *   </dd>
   *   <dt>DecimalType</dt>
   *   <dd>
   *     <ul>
   *       <li>Long </li>
   *       <li>Double</li>
   *       <li>String value of a BigDecimal</li>
   *       <li>BigInteger</li>
   *       <li>BigDecimal</li>
   *     </ul>
   *     <p><em>Note:</em> For Double and String conversions, the parsed value will be rounded down, per
   *     {@link RoundingMode#HALF_DOWN} convention.</p>
   *   </dd>
   *   <dt>StringType</dt>
   *   <dd>
   *     <ul>
   *       <li>String</li>
   *     </ul>
   *   </dd>
   *   <dt>ArrayType</dt>
   *   <dd>
   *     <ul>
   *       <li>List of supported DataTypes</li>
   *       <li>Row of supported DataTypes</li>
   *     </ul>
   *   </dd>
   *   <dt>MapType</dt>
   *   <dd>
   *     <ul>
   *       <li>Map with key/value pairs of supported DataTypes</li>
   *       <li>Row of supported DataTypes. <em>NOTE:</em> The Row must have an associated schema or conversion will fail.</li>
   *     </ul>
   *   </dd>
   *   <dt>StructType</dt>
   *   <dd>
   *     <ul>
   *       <li>Map with the following restrictions: keys must be Strings and match the names of the fields in the StructType,
   *       values must be supported DataTypes.</li>
   *       <li>List of supported DataTypes; list must have an exact count and in-order list of values for the StructType</li>
   *       <li>Row of supported DataTypes; Row must have an exact count and in-order list of values for the StructType</li>
   *     </ul>
   *   </dd>
   * </dl>
   * @param item The value for conversion
   * @param type The DataType of the field
   * @return A Row-compatible value
   * @see <a href="https://spark.apache.org/docs/latest/sql-programming-guide.html#data-types">Spark SQL Data Types</a>
   */
  public static Object toRowValue(Object item, DataType type) {

    String typeName = type.typeName();
    // Binary, Boolean, Byte, Date, Null, Decimal, Double, Float, Integer, Long, Short, String, Timestamp
    // Array, Map, CalendarInterval, Struct, UserDefined

    switch (typeName) {
      case "binary":
        // byte[]
        if (item instanceof ByteBuffer) {
          return ((ByteBuffer) item).array();
        } else if (item instanceof byte[]) {
          return item;
        } else {
          throw new RuntimeException(String.format("Type[%s] - Invalid or unrecognized input format", type));
        }
      case "boolean":
        // boolean or Boolean
        if (item instanceof Boolean) {
          return item;
        }
        String str = item.toString();
        if ("true".equals(str.toLowerCase())) {
          return Boolean.TRUE;
        } else if ("false".equals(str.toLowerCase())) {
          return Boolean.FALSE;
        } else {
          throw new RuntimeException(String.format("Type[%s] - Invalid or unrecognized input format: %s", type, item));
        }
      case "date":
        // java.sql.Date
        if (item instanceof Long) {
          return new java.sql.Date((Long) item);
        } else if (item instanceof String) {
          return new java.sql.Date(DateTime.parse((String) item).getMillis());
        } else if (item instanceof Date) {
          return new java.sql.Date(((Date) item).getTime());
        } else if (item instanceof DateTime) {
          return new java.sql.Date(((DateTime) item).getMillis());
        } else {
          throw new RuntimeException(String.format("Type[%s] - Invalid or unrecognized input format: %s", type, item));
        }
      case "timestamp":
        // java.sql.Timestamp
        if (item instanceof Long) {
          return new Timestamp((Long) item);
        } else if (item instanceof String) {
          return new Timestamp(DateTime.parse((String) item).getMillis());
        } else if (item instanceof Date) {
          return new Timestamp(((Date) item).getTime());
        } else if (item instanceof DateTime) {
          return new Timestamp(((DateTime) item).getMillis());
        } else {
          throw new RuntimeException(String.format("Type[%s] - Invalid or unrecognized input format: %s", type, item));
        }
      case "double":
        // double or Double
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
        // float or Float
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
        // int or Integer
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
        // long or Long
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
        // byte or Byte
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
      case "short":
        // short or Short
        if (item instanceof Short) {
          return item;
        } else if (item instanceof Number) {
          return ((Number) item).shortValue();
        } else {
          try {
            return Short.parseShort((String) item);
          } catch (Exception e) {
            throw new RuntimeException(String.format("Type[%s] - Invalid or unrecognized input format: %s", type, item));
          }
        }
      case "string":
        // String
        return item.toString();
      case "array":
        // List
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
        // Map
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
        // Row
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
        } else {
          throw new RuntimeException(String.format("Type[%s] - Invalid or unrecognized input format: %s", type, item));
        }
        return RowFactory.create(valueList.toArray());
      default:
        if (type.typeName().startsWith("decimal")) {
          // java.math.BigDecimal
          if (item instanceof Long) {
            return BigDecimal.valueOf((Long) item, ((DecimalType) type).scale());
          } else if (item instanceof Double) {
            return BigDecimal.valueOf((Double) item).setScale(((DecimalType) type).scale(), RoundingMode.HALF_DOWN);
          } else if (item instanceof String) {
            return new BigDecimal((String) item).setScale(((DecimalType) type).scale(), RoundingMode.HALF_DOWN);
          } else if (item instanceof BigDecimal) {
            return item;
          } else if (item instanceof BigInteger) {
            return new BigDecimal((BigInteger) item, ((DecimalType) type).scale());
          } else {
            throw new RuntimeException(String.format("Type[%s] - Invalid or unrecognized input format: %s", type, item));
          }
        } else {
          throw new RuntimeException(String.format("Type[%s] - StructField DataType unrecognized or not yet implemented",
              type));
        }
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
  
  public static StructType subtractSchema(StructType schema, List<String> subtractFieldNames) {
    List<String> fieldNames = Lists.newArrayList();
    
    for (StructField schemaField : schema.fields()) {
      if (!subtractFieldNames.contains(schemaField.name())) {
        fieldNames.add(schemaField.name());
      }
    }
    
    StructType subtracted = subsetSchema(schema, fieldNames);
    
    return subtracted;
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
      } else {
        values[i] = row.get(i);
      }
    }

    return new RowWithSchema(row.schema(), values);
  }
  
  public static Row append(Row row, Object value) {
    Object[] appendedValues = ObjectArrays.concat(valuesFor(row), value);
    Row appendedRow = RowFactory.create(appendedValues);
    
    return appendedRow;
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
  
  public static Row remove(Row row, String fieldName) {
    List<StructField> removedFields = Lists.newArrayList(row.schema().fields());
    removedFields.remove(row.fieldIndex(fieldName));
    StructType removedSchema = new StructType(removedFields.toArray(new StructField[removedFields.size()]));
    
    List<Object> removedValues = Lists.newArrayList(RowUtils.valuesFor(row));
    removedValues.remove(row.fieldIndex(fieldName));
    
    return new RowWithSchema(removedSchema, removedValues.toArray());
  }

  public static Object[] valuesFor(Row row) {
    Object[] values = new Object[row.length()];

    for (int i = 0; i < row.length(); i++) {
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
        case "byte":
          field = DataTypes.createStructField(fieldName, DataTypes.ByteType, true);
          break;
        case "short":
          field = DataTypes.createStructField(fieldName, DataTypes.ShortType, true);
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
        case "binary":
          field = DataTypes.createStructField(fieldName, DataTypes.BinaryType, true);
          break;
        case "timestamp":
          field = DataTypes.createStructField(fieldName, DataTypes.TimestampType, true);
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
      Object secondValue = second.get(second.fieldIndex(valueFieldName));

      if (firstValue != null && secondValue != null && !firstValue.equals(secondValue)) {
        return true;
      }

      if ((firstValue != null && secondValue == null) || (firstValue == null && secondValue != null)) {
        return true;
      }
    }

    return false;
  }

  public static Column[] toColumnArray(List<String> columnList) {
    Column[] columnArray = new Column[columnList.size()];
    for (int i = 0; i < columnList.size(); i++) {
      columnArray[i] = new Column(columnList.get(i));
    }
    return columnArray;
  }

}
