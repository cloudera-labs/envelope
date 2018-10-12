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

import com.google.common.collect.Lists;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.avro.Schema.Field;
import static org.apache.avro.Schema.Type.NULL;
import static org.apache.avro.Schema.Type.RECORD;
import static org.apache.avro.Schema.Type.UNION;

/**
 *
 * No reliance on the 'spark-avro' library.
 * @see <a href="http://spark.apache.org/docs/latest/sql-programming-guide.html#data-types">Spark SQL and DataFrames: Data Types</a>
 */
public class AvroUtils {
  private static final Logger LOG = LoggerFactory.getLogger(AvroUtils.class);

  /**
   *
   * @param structType The DataFrame StructType
   * @return Avro Schema.Record for the given StructType
   */
  public static Schema schemaFor(StructType structType) {
    return schemaFor(structType, null, null);
  }

  /**
   *
   * @param structType The DataFrame StructType
   * @param record A name for the generated Schema.Record
   * @param namespace A namespace for the generated Schema.Record
   * @return Avro Schema.Record for the given StructType
   */
  public static Schema schemaFor(StructType structType, String record, String namespace) {
    return schemaFor(structType, record, namespace, 0);
  }

  private static Schema schemaFor(StructType structType, String record, String namespace, int recordCount) {

    // Increment after using the recordCount
    record = (null == record) ? "record" + recordCount++ : record;

    LOG.debug("Converting {} to Avro Record schema [{}:{}]", structType, record, namespace);
    SchemaBuilder.RecordBuilder<Schema> schema = SchemaBuilder.record(record);

    if (null != namespace) {
      schema.namespace(namespace);
    }

    schema.doc("Auto-generated from Spark DataFrame");

    SchemaBuilder.FieldAssembler<Schema> assembler = schema.fields();
    StructField[] structFields = structType.fields();

    for (StructField f : structFields) {
      assembler.name(f.name()).type(typeFor(f.dataType(), f.nullable(), recordCount)).noDefault();
    }

    return assembler.endRecord();
  }

  /**
   *
   * @param dataType The DataFrame DataType
   * @return An optional Avro Schema.Type for the given DataType
   */
  public static Schema typeFor(DataType dataType) {
    return typeFor(dataType, true);
  }

  /**
   *
   * @param dataType The DataFrame DataType
   * @param isOptional Flag indicating if resulting Schema.Type is constructed as optional
   * @return An Avro Schema.Type for the given DataType
   */
  public static Schema typeFor(DataType dataType, boolean isOptional) {
    return typeFor(dataType, isOptional, 0);
  }

  private static Schema typeFor(DataType dataType, boolean isOptional, int recordCount) {
    LOG.trace("Converting {} to Avro, optional[{}]", dataType, isOptional);

    Schema typeSchema;
    SchemaBuilder.BaseTypeBuilder<Schema> typeBuilder = SchemaBuilder.builder();

    switch (dataType.typeName()) {
      case "binary":
        // bytes
        typeSchema = typeBuilder.bytesType();
        break;
      case "boolean":
        typeSchema = typeBuilder.booleanType();
        break;
      case "date":
        // int (logical)
        typeSchema = LogicalTypes.date().addToSchema(typeBuilder.intType());
        break;
      case "timestamp":
        // long (logical)
        typeSchema = LogicalTypes.timestampMillis().addToSchema(typeBuilder.longType());
        break;
      case "double":
        typeSchema = typeBuilder.doubleType();
        break;
      case "float":
        typeSchema = typeBuilder.floatType();
        break;
      case "integer":
      case "byte":
      case "short":
        typeSchema = typeBuilder.intType();
        break;
      case "long":
        typeSchema = typeBuilder.longType();
        break;
      case "null":
        typeSchema = typeBuilder.nullType();
        break;
      case "string":
        typeSchema = typeBuilder.stringType();
        break;
      case "array":
        ArrayType arrayType = (ArrayType) dataType;
        typeSchema = typeBuilder.array().items(typeFor(arrayType.elementType(), arrayType.containsNull(), recordCount));
        break;
      case "map":
        MapType mapType = (MapType) dataType;
        // Keys must be strings: mapType.keyType()
        typeSchema = typeBuilder.map().values(typeFor(mapType.valueType(), mapType.valueContainsNull(), recordCount));
        break;
      case "struct":
        StructType structType = (StructType) dataType;
        // Nested "anonymous" records
        typeSchema = schemaFor(structType, null, null, recordCount);
        break;
      default:
        if (dataType.typeName().startsWith("decimal")) {
          // byte (logical)
          DecimalType decimalType = (DecimalType) dataType;
          typeSchema = LogicalTypes.decimal(decimalType.precision(), decimalType.scale()).addToSchema(typeBuilder.bytesType());
        } else {
          throw new RuntimeException(String.format("DataType[%s] - DataType unrecognized or not yet implemented",
              dataType));
        }
    }

    if (isOptional && !typeSchema.getType().equals(NULL)) {
      return SchemaBuilder.builder().nullable().type(typeSchema);
    }

    return typeSchema;
  }

  /**
   * Convert an Avro Record schema into a StructType.
   *
   * TODO : Add conversion hints to Avro schema to handle MapType key conversions, since Avro will brute force them into Strings
   *
   * @param schema The Avro Record schema
   * @return StructType conversion
   */
  public static StructType structTypeFor(Schema schema) {
    LOG.debug("Converting Avro schema to StructType");

    if (!schema.getType().equals(RECORD)) {
      throw new AvroRuntimeException("Unable to convert to Schema to StructType: Schema must be a Record");
    }

    return (StructType) dataTypeFor(schema);
  }

  /**
   * Convert Avro Types into their associated DataType.
   *
   * @param schemaType Avro Schema.Type
   * @return DataType representation
   */
  public static DataType dataTypeFor(Schema schemaType) {
    LOG.trace("Converting Schema[{}] to DataType", schemaType);

    // Unwrap "optional" unions to the base type
    boolean isOptional = isNullable(schemaType);

    if (isOptional) {
      // if only 2 items in the union, then "unwrap," otherwise, it's a full union and should be rendered as such
      if (schemaType.getTypes().size() == 2) {
        LOG.trace("Unwrapping simple 'optional' union for {}", schemaType);
        for (Schema s : schemaType.getTypes()) {
          if (s.getType().equals(NULL)) {
            continue;
          }
          // Unwrap
          schemaType = s;
          break;
        }
      }
    }

    // Convert supported LogicalTypes
    if (null != schemaType.getLogicalType()) {
      LogicalType logicalType = schemaType.getLogicalType();
      switch (logicalType.getName()) {
        case "date" :
          return DataTypes.DateType;
        case "timestamp-millis" :
          return DataTypes.TimestampType;
        case "decimal" :
          LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType;
          return DataTypes.createDecimalType(decimal.getPrecision(), decimal.getScale());
        default:
          // Pass-thru
          LOG.warn("Unsupported LogicalType[{}], continuing with underlying base type", logicalType.getName());
      }
    }

    switch (schemaType.getType()) {
      case RECORD:
        // StructType
        List<StructField> structFieldList = Lists.newArrayListWithCapacity(schemaType.getFields().size());
        for (Field f : schemaType.getFields()) {
          structFieldList.add(DataTypes.createStructField(f.name(), dataTypeFor(f.schema()), isNullable(f.schema())));
        }
        return DataTypes.createStructType(structFieldList);
      case ARRAY:
        Schema elementType = schemaType.getElementType();
        return DataTypes.createArrayType(dataTypeFor(elementType), isNullable(elementType));
      case MAP:
        Schema valueType = schemaType.getValueType();
        return DataTypes.createMapType(DataTypes.StringType, dataTypeFor(valueType), isNullable(valueType));
      case UNION:
        // StructType of members
        List<StructField> unionFieldList = Lists.newArrayListWithCapacity(schemaType.getTypes().size());
        int m = 0;
        for (Schema u : schemaType.getTypes()) {
          unionFieldList.add(DataTypes.createStructField("member" + m++, dataTypeFor(u), isNullable(u)));
        }
        return DataTypes.createStructType(unionFieldList);
      case FIXED:
      case BYTES:
        return DataTypes.BinaryType;
      case ENUM:
      case STRING:
        return DataTypes.StringType;
      case INT:
        return DataTypes.IntegerType;
      case LONG:
        return DataTypes.LongType;
      case FLOAT:
        return DataTypes.FloatType;
      case DOUBLE:
        return DataTypes.DoubleType;
      case BOOLEAN:
        return DataTypes.BooleanType;
      case NULL:
        return DataTypes.NullType;
      default:
        throw new RuntimeException(String.format("Unrecognized or unsupported Avro Type conversion: %s", schemaType));
    }
  }

  private static boolean isNullable(Schema schema) {
    if (schema.getType().equals(UNION)) {
      for (Schema s : schema.getTypes()){
        if (s.getType().equals(NULL)) {
          return true;
        }
      }
    }
    return false;
  }
}
