/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.input.translate;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.cloudera.labs.envelope.utils.AvroUtils;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.cloudera.labs.envelope.utils.TranslatorUtils;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
import com.typesafe.config.Config;

/**
 * A translator implementation for binary Apache Avro generic record messages.
 */
public class AvroTranslator implements Translator<byte[], byte[]> {

  private Schema avroSchema;
  private StructType schema;
  private boolean doesAppendRaw;
  private GenericDatumReader<GenericRecord> reader;

  public static final String AVRO_LITERAL_CONFIG = "schema.literal";
  public static final String AVRO_PATH_CONFIG = "schema.path";

  @Override
  public void configure(Config config) {
    if (!(config.hasPath(AVRO_PATH_CONFIG) ^ config.hasPath(AVRO_LITERAL_CONFIG))) {
      throw new RuntimeException("Avro translator must specify either a '" + AVRO_PATH_CONFIG + "' or a '" + AVRO_LITERAL_CONFIG + "' configuration.");
    }
    
    String avroSchemaLiteral;
    if (config.hasPath(AVRO_LITERAL_CONFIG)) {
      avroSchemaLiteral = config.getString(AVRO_LITERAL_CONFIG);
    }
    else {
      String avroSchemaPath = config.getString(AVRO_PATH_CONFIG);
      avroSchemaLiteral = hdfsFileAsString(avroSchemaPath);
    }
    avroSchema = new Schema.Parser().parse(avroSchemaLiteral);
    schema = AvroUtils.structTypeFor(avroSchema);
    
    doesAppendRaw = TranslatorUtils.doesAppendRaw(config);
    if (doesAppendRaw) {
      List<StructField> rawFields = Lists.newArrayList(
          DataTypes.createStructField(TranslatorUtils.getAppendRawKeyFieldName(config), DataTypes.BinaryType, false),
          DataTypes.createStructField(TranslatorUtils.getAppendRawValueFieldName(config), DataTypes.BinaryType, false));
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
  
  private String hdfsFileAsString(String hdfsFile) {
    String contents = null;

    try {
      FileSystem fs = FileSystem.get(new Configuration());
      InputStream stream = fs.open(new Path(hdfsFile));
      InputStreamReader reader = new InputStreamReader(stream, Charsets.UTF_8);
      contents = CharStreams.toString(reader);
      reader.close();
      stream.close();
    } catch (Exception e) {
      throw new RuntimeException("Avro translator was unable to open schema path: " + hdfsFile, e);
    }

    return contents;
  }
}
