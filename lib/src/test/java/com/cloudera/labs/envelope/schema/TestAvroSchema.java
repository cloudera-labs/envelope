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

package com.cloudera.labs.envelope.schema;

import com.cloudera.labs.envelope.component.ComponentFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.cloudera.labs.envelope.validate.ValidationAssert.assertNoValidationFailures;
import static com.cloudera.labs.envelope.validate.ValidationAssert.assertValidationFailures;
import static org.junit.Assert.assertEquals;

public class TestAvroSchema {

  public static final String AVRO_SCHEMA_DATA = "/schema/sample-avro-schema.avsc";

  private Config config = ConfigFactory.empty();

  @Test
  public void validLiteralSchema() {
    StringBuilder avroLiteral = new StringBuilder()
      .append("{ \"type\" : \"record\", \"name\" : \"example\", \"fields\" : [")
      .append("{ \"name\" : \"A_Long\", \"type\" : \"long\" },")
      .append("{ \"name\" : \"An_Int\", \"type\" : \"int\" },")
      .append("{ \"name\" : \"A_String\", \"type\" : \"string\" },")
      .append("{ \"name\" : \"Another_String\", \"type\" : \"string\" }")
      .append("] }");
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(ComponentFactory.TYPE_CONFIG_NAME, "avro");
    paramMap.put(AvroSchema.AVRO_LITERAL_CONFIG, avroLiteral.toString());
    config = ConfigFactory.parseMap(paramMap);
    AvroSchema avroSchema = new AvroSchema(); 
    assertNoValidationFailures(avroSchema, config);
    avroSchema.configure(config);
    StructType schema = avroSchema.getSchema();
    assertEquals(schema.fields().length, 4);
    assertEquals(schema.fields()[0].name(), "A_Long");
    assertEquals(schema.fields()[1].name(), "An_Int");
    assertEquals(schema.fields()[2].name(), "A_String");
    assertEquals(schema.fields()[3].name(), "Another_String");
  }

  @Test
  public void validFileSchema() {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(ComponentFactory.TYPE_CONFIG_NAME, "avro");
    paramMap.put(AvroSchema.AVRO_FILE_CONFIG, AvroSchema.class.getResource(AVRO_SCHEMA_DATA).getPath());
    config = ConfigFactory.parseMap(paramMap);
    AvroSchema avroSchema = new AvroSchema(); 
    assertNoValidationFailures(avroSchema, config);
    avroSchema.configure(config);
    StructType schema = avroSchema.getSchema();
    assertEquals(schema.fields().length, 4);
    assertEquals(schema.fields()[0].name(), "A_Long");
    assertEquals(schema.fields()[1].name(), "An_Int");
    assertEquals(schema.fields()[2].name(), "A_String");
    assertEquals(schema.fields()[3].name(), "Another_String");
  }

  @Test
  public void missingConfig() {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(ComponentFactory.TYPE_CONFIG_NAME, "avro");
    config = ConfigFactory.parseMap(paramMap);
    AvroSchema avroSchema = new AvroSchema(); 
    assertValidationFailures(avroSchema, config);
  }

  @Test
  public void missingLiteral() {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(ComponentFactory.TYPE_CONFIG_NAME, "avro");
    paramMap.put(AvroSchema.AVRO_LITERAL_CONFIG, "");
    config = ConfigFactory.parseMap(paramMap);
    AvroSchema avroSchema = new AvroSchema(); 
    assertValidationFailures(avroSchema, config);
  }

  @Test
  public void missingTypes() {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(ComponentFactory.TYPE_CONFIG_NAME, "avro");
    paramMap.put(AvroSchema.AVRO_FILE_CONFIG, "");
    config = ConfigFactory.parseMap(paramMap);
    AvroSchema avroSchema = new AvroSchema(); 
    assertValidationFailures(avroSchema, config);
  }

}
