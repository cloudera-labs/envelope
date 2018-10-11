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

import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestAvroSerializer {

  @Test
  public void testAvroSerialization() throws IOException {
    StructType structType = RowUtils.structTypeFor(
        Lists.newArrayList("field1", "field2", "field3"),
        Lists.newArrayList("string", "int", "boolean"));
    Row row = new RowWithSchema(structType, "hello", 1, false);
    
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
    assertEquals(deserialized.get("field1").toString(), "hello"); // Avro encodes strings with Utf8 class
    assertEquals(deserialized.get("field2"), 1);
    assertEquals(deserialized.get("field3"), false);
  }
  
}
