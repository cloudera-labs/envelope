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

import com.cloudera.labs.envelope.schema.ConfigurationDataTypes;
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.Test;

import java.util.Map;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestDelimitedSerializer {

  @Test
  public void testDelimitedSerialization() {
    List<StructField> fields = Lists.newArrayList(
        DataTypes.createStructField("field1", DataTypes.StringType, true),
        DataTypes.createStructField("field2", DataTypes.IntegerType, true),
        DataTypes.createStructField("field3", DataTypes.BooleanType, true)
    );
    Row row = new RowWithSchema(DataTypes.createStructType(fields), "hello", 1, false);
    
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
    List<StructField> fields = Lists.newArrayList(
        DataTypes.createStructField("field1", DataTypes.StringType, true),
        DataTypes.createStructField("field2", DataTypes.IntegerType, true),
        DataTypes.createStructField("field3", DataTypes.BooleanType, true)
    );
    Row row = new RowWithSchema(DataTypes.createStructType(fields), null, 1, false);

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
    List<StructField> fields = Lists.newArrayList(
        DataTypes.createStructField("field1", DataTypes.StringType, true),
        DataTypes.createStructField("field2", DataTypes.IntegerType, true),
        DataTypes.createStructField("field3", DataTypes.BooleanType, true)
    );
    Row row = new RowWithSchema(DataTypes.createStructType(fields), null, 1, false);

    Map<String, String> configs = Maps.newHashMap();
    configs.put(DelimitedSerializer.FIELD_DELIMITER_CONFIG_NAME, "||");
    Serializer<Row> serializer = new DelimitedSerializer();
    serializer.configure(configs, false);

    byte[] serialized = serializer.serialize("test", row);
    serializer.close();

    assertEquals(new String(serialized), DelimitedSerializer.USE_FOR_NULL_DEFAULT_VALUE + "||1||false");
  }
  
}
