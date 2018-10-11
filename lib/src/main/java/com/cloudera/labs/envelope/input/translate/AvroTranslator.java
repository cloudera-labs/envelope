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

package com.cloudera.labs.envelope.input.translate;

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.utils.AvroUtils;
import com.cloudera.labs.envelope.utils.FilesystemUtils;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.cloudera.labs.envelope.utils.TranslatorUtils;
import com.cloudera.labs.envelope.validate.AvroSchemaLiteralValidation;
import com.cloudera.labs.envelope.validate.AvroSchemaPathValidation;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.List;

/**
 * A translator implementation for binary Apache Avro generic record messages.
 */
public class AvroTranslator implements Translator<byte[], byte[]>, ProvidesAlias, ProvidesValidations {

  private Schema avroSchema;
  private StructType schema;
  private boolean doesAppendRaw;
  private GenericDatumReader<GenericRecord> reader;

  public static final String AVRO_LITERAL_CONFIG = "schema.literal";
  public static final String AVRO_PATH_CONFIG = "schema.path";

  @Override
  public void configure(Config config) {
    String avroSchemaLiteral;
    if (config.hasPath(AVRO_LITERAL_CONFIG)) {
      avroSchemaLiteral = config.getString(AVRO_LITERAL_CONFIG);
    }
    else {
      String avroSchemaPath = config.getString(AVRO_PATH_CONFIG);
      try {
        avroSchemaLiteral = FilesystemUtils.filesystemPathContents(avroSchemaPath);
      }
      catch (Exception e) {
        // At this point we have already validated the filesystem call,
        // but we need to throw something "just in case" this time breaks
        throw new RuntimeException(e);
      }
    }
    avroSchema = new Schema.Parser().parse(avroSchemaLiteral);
    schema = AvroUtils.structTypeFor(avroSchema);
    
    doesAppendRaw = TranslatorUtils.doesAppendRaw(config);
    if (doesAppendRaw) {
      List<StructField> rawFields = Lists.newArrayList(
          DataTypes.createStructField(
              TranslatorUtils.getAppendRawKeyFieldName(config), DataTypes.BinaryType, false),
          DataTypes.createStructField(
              TranslatorUtils.getAppendRawValueFieldName(config), DataTypes.BinaryType, false));
      schema = RowUtils.appendFields(schema, rawFields);
    }
    
    reader = new GenericDatumReader<GenericRecord>(avroSchema);
  }

  @Override
  public Iterable<Row> translate(byte[] key, byte[] value) throws Exception {
    Decoder decoder = DecoderFactory.get().binaryDecoder(value, null);
    GenericRecord record = reader.read(null, decoder);
    Row row = rowForRecord(record);
    
    if (doesAppendRaw) {
      row = RowUtils.append(row, key);
      row = RowUtils.append(row, value);
    }

    return Collections.singleton(row);
  }

  @Override
  public StructType getSchema() {
    return schema;
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
  
  @Override
  public String getAlias() {
    return "avro";
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .exactlyOnePathExists(AVRO_LITERAL_CONFIG, AVRO_PATH_CONFIG)
        .ifPathExists(AVRO_PATH_CONFIG, new AvroSchemaPathValidation(AVRO_PATH_CONFIG))
        .ifPathExists(AVRO_LITERAL_CONFIG, new AvroSchemaLiteralValidation(AVRO_LITERAL_CONFIG))
        .addAll(TranslatorUtils.APPEND_RAW_VALIDATIONS)
        .build();
  }

}
