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
import org.apache.kafka.common.serialization.Serializer;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestDelimitedSerializer {

  @Test
  public void testDelimitedSerialization() {
    StructType structType = RowUtils.structTypeFor(
        Lists.newArrayList("field1", "field2", "field3"),
        Lists.newArrayList("string", "int", "boolean"));
    Row row = new RowWithSchema(structType, "hello", 1, false);
    
    Map<String, String> configs = Maps.newHashMap();
    configs.put(DelimitedSerializer.FIELD_DELIMITER_CONFIG_NAME, "||");
    Serializer<Row> serializer = new DelimitedSerializer();
    serializer.configure(configs, false);
    
    byte[] serialized = serializer.serialize("test", row);
    serializer.close();
    
    assertEquals(new String(serialized), "hello||1||false");
  }

  @Test
  public void testDelimitedWithNullSerialization() {
    StructType structType = RowUtils.structTypeFor(
        Lists.newArrayList("field1", "field2", "field3"),
        Lists.newArrayList("string", "int", "boolean"));
    Row row = new RowWithSchema(structType, null, 1, false);

    Map<String, String> configs = Maps.newHashMap();
    configs.put(DelimitedSerializer.FIELD_DELIMITER_CONFIG_NAME, "||");
    configs.put(DelimitedSerializer.USE_FOR_NULL_CONFIG_NAME, "BANG");
    Serializer<Row> serializer = new DelimitedSerializer();
    serializer.configure(configs, false);

    byte[] serialized = serializer.serialize("test", row);
    serializer.close();

    assertEquals(new String(serialized), "BANG||1||false");
  }

  @Test
  public void testDelimitedWithDefaultNullSerialization() {
    StructType structType = RowUtils.structTypeFor(
        Lists.newArrayList("field1", "field2", "field3"),
        Lists.newArrayList("string", "int", "boolean"));
    Row row = new RowWithSchema(structType, null, 1, false);

    Map<String, String> configs = Maps.newHashMap();
    configs.put(DelimitedSerializer.FIELD_DELIMITER_CONFIG_NAME, "||");
    Serializer<Row> serializer = new DelimitedSerializer();
    serializer.configure(configs, false);

    byte[] serialized = serializer.serialize("test", row);
    serializer.close();

    assertEquals(new String(serialized), DelimitedSerializer.USE_FOR_NULL_DEFAULT_VALUE + "||1||false");
  }
  
}
