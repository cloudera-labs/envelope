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

package com.cloudera.labs.envelope.kafka.serde;

import com.cloudera.labs.envelope.utils.RowUtils;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;

public class AvroSerializer implements Serializer<Row> {

  public static final String SCHEMA_PATH_CONFIG_NAME = "schema.path";
  
  private static Logger LOG = LoggerFactory.getLogger(AvroSerializer.class);
  
  private Schema schema;
  private DatumWriter<GenericRecord> datumWriter;
  private Set<Type> unsupportedTypes = Sets.newHashSet(Type.ARRAY, Type.ENUM, Type.FIXED, Type.MAP, Type.RECORD);
  
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    String schemaLocation = configs.get(SCHEMA_PATH_CONFIG_NAME).toString();
    
    Schema schema = parseAvroSchemaFile(schemaLocation);
    validateSchemaIsSupported(schema);
    
    this.schema = schema;
    this.datumWriter = new GenericDatumWriter<GenericRecord>(schema);
    
    LOG.info("Kafka output Avro serializer configured");
  }

  @Override
  public byte[] serialize(String topic, Row data) {
    if (data == null) {
      return null;
    }
    
    GenericRecord record = recordForRow(data, schema);
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    try {
      datumWriter.write(record, encoder);
      encoder.flush();
      out.close();
    } catch (IOException e) {
      throw new RuntimeException("Avro serializer for Kafka output could not serialize row", e);
    }
    
    return out.toByteArray();
  }

  @Override
  public void close() {
    // Nothing to do
  }
  
  private Schema parseAvroSchemaFile(String schemaLocation) {
    Schema schema;
    InputStream stream;
    
    try {
      FileSystem fs = FileSystem.get(new Configuration());
      stream = fs.open(new Path(schemaLocation));
    } catch (Exception e) {
      throw new RuntimeException("Avro serializer for Kafka output could not open Avro schema location", e);
    }
    
    try {
      schema = new Schema.Parser().parse(stream);
      stream.close();
    } catch (Exception e) {
      throw new RuntimeException("Avro serializer for Kafka output could not parse Avro schema", e);
    }
    
    return schema;
  }
  
  private GenericRecord recordForRow(Row row, Schema schema) {
    GenericRecord record = new GenericData.Record(schema);
        
    Object[] values = RowUtils.valuesFor(row);
    for (int valueNum = 0; valueNum < values.length; valueNum++) {
      record.put(valueNum, values[valueNum]);
    }
    
    return record;
  }
  
  private void validateSchemaIsSupported(Schema schema) {
    for (Field field : schema.getFields()) {
      Type type = field.schema().getType();
      if (unsupportedTypes.contains(type)) {
        throw new RuntimeException("Avro serializer for Kafka output does not support Avro schema type: " + type);
      }
    }
  }

}
