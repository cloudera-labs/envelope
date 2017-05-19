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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.Test;

import com.cloudera.labs.envelope.utils.TranslatorUtils;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class TestAvroTranslator {

  @Test
  public void testAvroTranslation() throws Exception {
    Schema schema = SchemaBuilder.record("test").fields()
        .optionalString("field1")
        .optionalInt("field2")
        .endRecord();
    Record record = new Record(schema);
    record.put("field1", "hello");
    record.put("field2", 100);
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    DatumWriter<Record> writer = new SpecificDatumWriter<Record>(schema);
    writer.write(record, encoder);
    encoder.flush();
    out.close();
    byte[] a = out.toByteArray();
    
    Config config = ConfigFactory.empty()
        .withValue(AvroTranslator.FIELD_NAMES_CONFIG_NAME, ConfigValueFactory.fromIterable(Lists.newArrayList("field1", "field2")))
        .withValue(AvroTranslator.FIELD_TYPES_CONFIG_NAME, ConfigValueFactory.fromIterable(Lists.newArrayList("string", "int")));
    
    Translator<byte[], byte[]> t = new AvroTranslator();
    t.configure(config);
    
    Row r = t.translate(null, a).iterator().next();
    
    assertEquals(r, RowFactory.create("hello", 100));
  }
  
  @Test
  public void testAppendRaw() throws Exception {
    Schema schema = SchemaBuilder.record("test").fields()
        .optionalString("field1")
        .optionalInt("field2")
        .endRecord();
    Record record = new Record(schema);
    record.put("field1", "hello");
    record.put("field2", 100);
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    DatumWriter<Record> writer = new SpecificDatumWriter<Record>(schema);
    writer.write(record, encoder);
    encoder.flush();
    out.close();
    byte[] a = out.toByteArray();
    
    Config config = ConfigFactory.empty()
        .withValue(AvroTranslator.FIELD_NAMES_CONFIG_NAME, ConfigValueFactory.fromIterable(Lists.newArrayList("field1", "field2")))
        .withValue(AvroTranslator.FIELD_TYPES_CONFIG_NAME, ConfigValueFactory.fromIterable(Lists.newArrayList("string", "int")))
        .withValue(TranslatorUtils.APPEND_RAW_ENABLED_CONFIG_NAME, ConfigValueFactory.fromAnyRef(true));
    
    Translator<byte[], byte[]> t = new AvroTranslator();
    t.configure(config);
    
    Row r = t.translate(null, a).iterator().next();
    
    assertEquals(r.length(), 4);
    assertEquals(r.get(0), "hello");
    assertEquals(r.get(1), 100);
    assertEquals(r.get(2), null);
    
    assertTrue(r.get(3) instanceof byte[]);
    byte[] value = (byte[])r.get(3);
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
    Decoder decoder = DecoderFactory.get().binaryDecoder(value, null);
    assertEquals(reader.read(null, decoder), record);
  }
  
}
