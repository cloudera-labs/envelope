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

import com.cloudera.labs.envelope.spark.Contexts;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.ByteBuffer;

import static com.cloudera.labs.envelope.validate.ValidationAssert.assertNoValidationFailures;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestAvroTranslator {

  private static final String AVRO_FILE = "/translator/avro-translator-test.avsc";

  @Test
  public void testAvroTranslationWithLiteralSchema() throws Exception {
    Config config = ConfigFactory.empty()
        .withValue("schema.type", ConfigValueFactory.fromAnyRef("avro"))
        .withValue("schema.literal", ConfigValueFactory.fromAnyRef(getSchema().toString()));

    performAvroTranslation(config);
  }

  @Test
  public void testAvroTranslationWithPathToSchema() throws Exception {
    Config config = ConfigFactory.empty()
        .withValue("schema.type", ConfigValueFactory.fromAnyRef("avro"))
        .withValue("schema.filepath",
            ConfigValueFactory.fromAnyRef(getClass().getResource(AVRO_FILE).getFile()));

    performAvroTranslation(config);
  }

  private Schema getSchema() throws Exception {
    return new Schema.Parser().parse(new File(
        getClass().getResource(AVRO_FILE).getFile()));
  }

  private void performAvroTranslation(Config config) throws Exception {
    Schema schema = getSchema();

    Record record = new Record(schema);
    record.put("field1", "hello");
    record.put("field2", true);
    record.put("field3", ByteBuffer.wrap("world".getBytes()));
    record.put("field4", 1.0d);
    record.put("field5", 1);
    record.put("field6", 1.0f);
    record.put("field7", 1L);
    for (int i = 8; i <= 14; i++) {
      record.put("field" + i, null);
    }

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    DatumWriter<Record> writer = new GenericDatumWriter<>(schema);
    writer.write(record, encoder);
    encoder.flush();
    byte[] a = out.toByteArray();
    out.close();

    AvroTranslator t = new AvroTranslator();
    assertNoValidationFailures(t, config);
    t.configure(config);

    Row translated = t.translate(TestingMessageFactory.get(a, DataTypes.BinaryType)).iterator().next();

    Row validated = Contexts.getSparkSession().createDataFrame(
        Lists.newArrayList(translated), translated.schema()).collectAsList().get(0);

    assertEquals("hello", validated.getAs("field1"));
    assertEquals(true, validated.getAs("field2"));
    assertEquals("world", new String(validated.<byte[]>getAs("field3")));
    assertEquals(1.0d, validated.getAs("field4"));
    assertEquals(1, validated.getAs("field5"));
    assertEquals(1.0f, validated.getAs("field6"));
    assertEquals(1L, validated.getAs("field7"));
    for (int i = 8; i < 14; i++) {
      assertNull(validated.getAs("field" + i));
    }
  }
  
}
