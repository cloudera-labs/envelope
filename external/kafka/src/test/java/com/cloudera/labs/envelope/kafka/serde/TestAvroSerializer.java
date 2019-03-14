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

package com.cloudera.labs.envelope.kafka.serde;

import com.cloudera.labs.envelope.spark.Contexts;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.spark.sql.Row;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestAvroSerializer {

  @Test
  public void testAvroSerialization() throws IOException {
    Row row = Contexts.getSparkSession().sql("SELECT " +
        "'hello' field1, " +
        "true field2, " +
        "BINARY('world') field3, " +
        "CAST(1.0 AS DOUBLE) field4, " +
        "CAST(1 AS INT) field5, " +
        "CAST(1.0 AS FLOAT) field6, " +
        "CAST(1 AS BIGINT) field7, " +
        "NULL field8, NULL field9, NULL field10, NULL field11, NULL field12, NULL field13, NULL field14"
    ).collectAsList().get(0);
    
    Map<String, String> configs = Maps.newHashMap();
    configs.put(AvroSerializer.SCHEMA_PATH_CONFIG_NAME, getClass().getResource("/kafka/serde/avro-serialization-test.avsc").getFile());
    Serializer<Row> serializer = new AvroSerializer();
    serializer.configure(configs, false);
    
    byte[] serialized = serializer.serialize("test", row);
    serializer.close();
    
    Schema schema = new Schema.Parser().parse(new File(getClass().getResource("/kafka/serde/avro-serialization-test.avsc").getFile()));
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
    Decoder decoder = DecoderFactory.get().binaryDecoder(serialized, null);
    GenericRecord deserialized = reader.read(null, decoder);

    assertEquals("hello", deserialized.get("field1").toString());
    assertEquals(true, deserialized.get("field2"));
    assertEquals("world", new String(((ByteBuffer) deserialized.get("field3")).array()));
    assertEquals(1.0d, deserialized.get("field4"));
    assertEquals(1, deserialized.get("field5"));
    assertEquals(1.0f, deserialized.get("field6"));
    assertEquals(1L, deserialized.get("field7"));
    for (int i = 8; i <= 14; i++) {
      assertNull(deserialized.get("field" + i));
    }
  }
  
}
