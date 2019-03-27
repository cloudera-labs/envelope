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
import com.cloudera.labs.envelope.utils.TestProtobufUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.cloudera.labs.envelope.validate.ValidationAssert.assertNoValidationFailures;
import static com.cloudera.labs.envelope.validate.ValidationAssert.assertValidationFailures;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class TestProtobufSchema {
  
  private static final String SINGLE_EXAMPLE = "/protobuf/protobuf_single_message.desc";
  private static final String MULTIPLE_EXAMPLE = "/protobuf/protobuf_multiple_message.desc";

  @Test
  public void getSchema() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(ComponentFactory.TYPE_CONFIG_NAME, "protobuf");
    configMap.put(ProtobufSchema.DESCRIPTOR_FILEPATH_CONFIG,
        TestProtobufSchema.class.getResource(SINGLE_EXAMPLE).getPath());
    Config config = ConfigFactory.parseMap(configMap);

    ProtobufSchema schema = new ProtobufSchema();
    assertNoValidationFailures(schema, config);
    schema.configure(config);

    assertThat(schema.getSchema(), is(TestProtobufUtils.SINGLE_SCHEMA));
  }

  @Test
  public void configureMissingFilepath() {
    Config config = ConfigFactory.empty();

    ProtobufSchema schema = new ProtobufSchema();
    assertValidationFailures(schema, config);
  }

  @Test
  public void configureWrongTypeFilepath() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(ComponentFactory.TYPE_CONFIG_NAME, "protobuf");
    configMap.put(ProtobufSchema.DESCRIPTOR_FILEPATH_CONFIG, new HashMap<>());
    Config config = ConfigFactory.parseMap(configMap);

    ProtobufSchema schema = new ProtobufSchema();
    assertValidationFailures(schema, config);
  }

  @Test(expected = RuntimeException.class)
  public void configureIllegalFilepath() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(ComponentFactory.TYPE_CONFIG_NAME, "protobuf");
    configMap.put(ProtobufSchema.DESCRIPTOR_FILEPATH_CONFIG, "not found");
    Config config = ConfigFactory.parseMap(configMap);

    ProtobufSchema schema = new ProtobufSchema();
    assertNoValidationFailures(schema, config);
    schema.configure(config);
  }

  @Test
  public void configureBlankFilepath() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(ComponentFactory.TYPE_CONFIG_NAME, "protobuf");
    configMap.put(ProtobufSchema.DESCRIPTOR_FILEPATH_CONFIG, "");
    Config config = ConfigFactory.parseMap(configMap);

    ProtobufSchema schema = new ProtobufSchema();
    assertValidationFailures(schema, config);
  }

  @Test (expected = RuntimeException.class)
  public void configMultipleNoDesignation() {
    String descPath = TestProtobufSchema.class.getResource(MULTIPLE_EXAMPLE).getPath();

    Map<String, Object> configMap = new HashMap<>();
    configMap.put(ComponentFactory.TYPE_CONFIG_NAME, "protobuf");
    configMap.put(ProtobufSchema.DESCRIPTOR_FILEPATH_CONFIG, descPath);
    Config config = ConfigFactory.parseMap(configMap);

    ProtobufSchema schema = new ProtobufSchema();
    assertNoValidationFailures(schema, config);
    schema.configure(config);
  }

  @Test
  public void configMultipleWrongTypeDesignation() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(ComponentFactory.TYPE_CONFIG_NAME, "protobuf");
    configMap.put(ProtobufSchema.DESCRIPTOR_FILEPATH_CONFIG, new HashMap<>());
    Config config = ConfigFactory.parseMap(configMap);

    ProtobufSchema schema = new ProtobufSchema();
    assertValidationFailures(schema, config);
  }

  @Test
  public void configMultipleBlankDesignation() {
    String descPath = TestProtobufSchema.class.getResource(MULTIPLE_EXAMPLE).getPath();

    Map<String, Object> configMap = new HashMap<>();
    configMap.put(ComponentFactory.TYPE_CONFIG_NAME, "protobuf");
    configMap.put(ProtobufSchema.DESCRIPTOR_FILEPATH_CONFIG, descPath);
    configMap.put(ProtobufSchema.DESCRIPTOR_MESSAGE_CONFIG, "");
    Config config = ConfigFactory.parseMap(configMap);

    ProtobufSchema schema = new ProtobufSchema();
    assertValidationFailures(schema, config);
  }

}
