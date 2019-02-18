/*
 * Copyright (c) 2015-2019, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.labs.envelope.translate;

import com.cloudera.labs.envelope.schema.AvroSchema;
import com.cloudera.labs.envelope.schema.SchemaFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

import static com.cloudera.labs.envelope.validate.ValidationAssert.assertNoValidationFailures;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestAvroTranslator {

  private static final String AVRO_FILE = "/translator/avro-translator-test.avsc";

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
    DatumWriter<Record> writer = new GenericDatumWriter<>(schema);
    writer.write(record, encoder);
    encoder.flush();
    byte[] a = out.toByteArray();
    out.close();

    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(AvroTranslator.SCHEMA_CONFIG + "." + SchemaFactory.TYPE_CONFIG_NAME, "avro");
    paramMap.put(AvroTranslator.SCHEMA_CONFIG + "." + AvroSchema.AVRO_FILE_CONFIG, 
        AvroTranslator.class.getResource(AVRO_FILE).getPath());
    Config config = ConfigFactory.parseMap(paramMap);

    AvroTranslator t = new AvroTranslator();
    assertNoValidationFailures(t, config);
    t.configure(config);
    
    Row r = t.translate(TestingMessageFactory.get(a, DataTypes.BinaryType)).iterator().next();
    
    assertEquals(r, RowFactory.create("hello", 100));
  }
  
}
