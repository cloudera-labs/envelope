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

import com.cloudera.labs.envelope.utils.TranslatorUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.Test;

import java.io.ByteArrayOutputStream;

import static com.cloudera.labs.envelope.validate.ValidationAssert.assertNoValidationFailures;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestAvroTranslator {

  @Test
  public void testAvroTranslationWithLiteralSchema() throws Exception {
    Schema schema = SchemaBuilder.record("test").fields()
        .optionalString("field1")
        .optionalInt("field2")
        .endRecord();
    Record record = new Record(schema);
    record.put("field1", "hello");
    record.put("field2", 100);
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    DatumWriter<Record> writer = new GenericDatumWriter<Record>(schema);
    writer.write(record, encoder);
    encoder.flush();
    byte[] a = out.toByteArray();
    out.close();

    Config config = ConfigFactory.empty()
        .withValue(AvroTranslator.AVRO_LITERAL_CONFIG, ConfigValueFactory.fromAnyRef(schema.toString()));

    AvroTranslator t = new AvroTranslator();
    assertNoValidationFailures(t, config);
    t.configure(config);
    
    Row r = t.translate(null, a).iterator().next();
    
    assertEquals(r, RowFactory.create("hello", 100));
  }

  @Test
  public void testAvroTranslationWithPathToSchema() throws Exception {
    Schema schema = SchemaBuilder.record("test").fields()
        .requiredString("field1")
        .requiredInt("field2")
        .endRecord();
    Record record = new Record(schema);
    record.put("field1", "hello");
    record.put("field2", 100);
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    DatumWriter<Record> writer = new GenericDatumWriter<Record>(schema);
    writer.write(record, encoder);
    encoder.flush();
    byte[] a = out.toByteArray();
    out.close();

    Config config = ConfigFactory.empty()
        .withValue(AvroTranslator.AVRO_PATH_CONFIG, 
            ConfigValueFactory.fromAnyRef(getClass().getResource("/translator/avro-translator-test.avsc").getFile()));

    AvroTranslator t = new AvroTranslator();
    assertNoValidationFailures(t, config);
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
    DatumWriter<Record> writer = new GenericDatumWriter<Record>(schema);
    writer.write(record, encoder);
    encoder.flush();
    out.close();
    byte[] a = out.toByteArray();
    
    Config config = ConfigFactory.empty()
        .withValue(AvroTranslator.AVRO_LITERAL_CONFIG, ConfigValueFactory.fromAnyRef(schema.toString()))
        .withValue(TranslatorUtils.APPEND_RAW_ENABLED_CONFIG_NAME, ConfigValueFactory.fromAnyRef(true));

    AvroTranslator t = new AvroTranslator();
    assertNoValidationFailures(t, config);
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
