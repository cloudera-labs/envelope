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
package com.cloudera.labs.envelope.input.translate;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;

import com.cloudera.labs.envelope.utils.RowUtils;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

/**
 * A translator implementation for binary Apache Avro generic record messages.
 */
public class AvroTranslator implements Translator<byte[], byte[]> {

  private List<String> fieldNames;
  private List<String> fieldTypes;
  private StructType schema;
  private Schema avroSchema;

  public static final String FIELD_NAMES_CONFIG_NAME = "field.names";
  public static final String FIELD_TYPES_CONFIG_NAME = "field.types";

  @Override
  public void configure(Config config) {
    fieldNames = config.getStringList(FIELD_NAMES_CONFIG_NAME);
    fieldTypes = config.getStringList(FIELD_TYPES_CONFIG_NAME);
    schema = RowUtils.structTypeFor(fieldNames, fieldTypes);
    avroSchema = schemaFor(fieldNames, fieldTypes);
  }

  @Override
  public Iterator<Row> translate(byte[] key, byte[] message) throws Exception {
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(avroSchema);
    Decoder decoder = DecoderFactory.get().binaryDecoder(message, null);
    GenericRecord record = reader.read(null, decoder);
    Row row = rowForRecord(record);

    return Collections.singleton(row).iterator();
  }

  @Override
  public StructType getSchema() {
    return schema;
  }

  private Schema schemaFor(List<String> fieldNames, List<String> fieldTypes) {
    FieldAssembler<Schema> assembler = SchemaBuilder.record("t").fields();

    for (int i = 0; i < fieldNames.size(); i++) {
      String fieldName = fieldNames.get(i);
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

  private static Row rowForRecord(GenericRecord record) {
    List<Object> values = Lists.newArrayList();

    for (Field field : record.getSchema().getFields()) {
      Object value = record.get(field.name());

      Type fieldType = field.schema().getType();
      if (fieldType.equals(Type.UNION)) {
        fieldType = field.schema().getTypes().get(1).getType();
      }
      // Avro returns Utf8s for strings, which Spark SQL doesn't know how to use
      if (fieldType.equals(Type.STRING) && value != null) {
        value = value.toString();
      }

      values.add(value);
    }

    return RowFactory.create(values.toArray());
  }

}
