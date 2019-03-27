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

import com.cloudera.labs.envelope.component.ComponentFactory;
import com.cloudera.labs.envelope.schema.FlatSchema;
import com.cloudera.labs.envelope.utils.MorphlineUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import mockit.Expectations;
import mockit.Mocked;
import mockit.integration.junit4.JMockit;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Compiler;

import java.io.File;
import java.util.Map;

@RunWith(JMockit.class)
public class TestMorphlineTranslator {

  private static final String MORPHLINE_FILE = "/morphline.conf";

  private Translator translator;

  private String getResourcePath(String resource) {
    return TestMorphlineTranslator.class.getResource(resource).getPath();
  }

  @Before
  public void setup() {
    translator = new MorphlineTranslator();
  }

  @Test
  public void getSchema() throws Exception {

    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(MorphlineTranslator.ENCODING_KEY, "UTF-8");
    configMap.put(MorphlineTranslator.ENCODING_MSG, "UTF-8");
    configMap.put(MorphlineTranslator.MORPHLINE, getResourcePath(MORPHLINE_FILE));
    configMap.put(MorphlineTranslator.MORPHLINE_ID, "default");
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + ComponentFactory.TYPE_CONFIG_NAME, "flat");
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_NAMES_CONFIG,
        Lists.newArrayList("bar", "foo"));
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_TYPES_CONFIG,
        Lists.newArrayList("integer", "string"));
    Config config = ConfigFactory.parseMap(configMap);

    translator.configure(config);
    StructType schema = translator.getProvidingSchema();

    Assert.assertEquals("Invalid number of SchemaFields", 2, schema.fields().length);
    Assert.assertEquals("Invalid DataType", DataTypes.IntegerType, schema.fields()[0].dataType());
    Assert.assertEquals("Invalid DataType", DataTypes.StringType, schema.fields()[1].dataType());
  }

  @Test (expected = RuntimeException.class)
  public void getSchemaInvalidDataType() throws Exception {

    // Relies on RowUtils.structTypeFor()

    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(MorphlineTranslator.ENCODING_KEY, "UTF-8");
    configMap.put(MorphlineTranslator.ENCODING_MSG, "UTF-8");
    configMap.put(MorphlineTranslator.MORPHLINE, getResourcePath(MORPHLINE_FILE));
    configMap.put(MorphlineTranslator.MORPHLINE_ID, "default");
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + ComponentFactory.TYPE_CONFIG_NAME, "flat");
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_NAMES_CONFIG,
        Lists.newArrayList("bar", "foo"));
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_TYPES_CONFIG,
        Lists.newArrayList("integer", "boom"));
    Config config = ConfigFactory.parseMap(configMap);

    translator.configure(config);
  }

  //@Ignore
  @Test (expected = MorphlineCompilationException.class)
  public void morphlineCompilationError(
      final @Mocked Compiler compiler
  ) throws Exception {
    new Expectations() {{
      compiler.compile((File) any, anyString, (MorphlineContext) any, (Command) any); 
      result = new Exception("Compilation exception");
    }};

    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(MorphlineTranslator.ENCODING_KEY, "UTF-8");
    configMap.put(MorphlineTranslator.ENCODING_MSG, "UTF-8");
    configMap.put(MorphlineTranslator.MORPHLINE, getResourcePath(MORPHLINE_FILE));
    configMap.put(MorphlineTranslator.MORPHLINE_ID, "compiler-exception");
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + ComponentFactory.TYPE_CONFIG_NAME, "flat");
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_NAMES_CONFIG,
        Lists.newArrayList("bar"));
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_TYPES_CONFIG,
        Lists.newArrayList("integer"));
    Config config = ConfigFactory.parseMap(configMap);

    translator = new MorphlineTranslator();
    translator.configure(config);
    Row raw = TestingMessageFactory.get("The Key", DataTypes.StringType, "The Message", DataTypes.StringType);
    translator.translate(raw);
  }

  @Test (expected = RuntimeException.class)
  public void conversionError() throws Exception {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(MorphlineTranslator.ENCODING_KEY, "UTF-8");
    configMap.put(MorphlineTranslator.ENCODING_MSG, "UTF-8");
    configMap.put(MorphlineTranslator.MORPHLINE, getResourcePath(MORPHLINE_FILE));
    configMap.put(MorphlineTranslator.MORPHLINE_ID, "default");
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + ComponentFactory.TYPE_CONFIG_NAME, "flat");
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_NAMES_CONFIG,
        Lists.newArrayList("int", "str", "float"));
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_TYPES_CONFIG,
        Lists.newArrayList("integer", "string", "boolean"));
    Config config = ConfigFactory.parseMap(configMap);

    translator.configure(config);
    Row raw = TestingMessageFactory.get("The Key", DataTypes.StringType, "The Message", DataTypes.StringType);
    translator.translate(raw);
  }

  @Test
  public void conversionSuccess() throws Exception {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(MorphlineTranslator.ENCODING_KEY, "UTF-8");
    configMap.put(MorphlineTranslator.ENCODING_MSG, "UTF-8");
    configMap.put(MorphlineTranslator.MORPHLINE, getResourcePath(MORPHLINE_FILE));
    configMap.put(MorphlineTranslator.MORPHLINE_ID, "default");
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + ComponentFactory.TYPE_CONFIG_NAME, "flat");
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_NAMES_CONFIG,
        Lists.newArrayList("int", "str", "float"));
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_TYPES_CONFIG,
        Lists.newArrayList("integer", "string", "float"));
    Config config = ConfigFactory.parseMap(configMap);

    translator.configure(config);
    Row raw = TestingMessageFactory.get("The Key".getBytes(), DataTypes.BinaryType, 
        "The Message".getBytes(), DataTypes.BinaryType);
    Iterable<Row> result = translator.translate(raw);
    Row row = result.iterator().next();

    Assert.assertNotNull("Row is null", result);
    Assert.assertEquals("Invalid number of fields", 3, row.length());
    Assert.assertEquals("Invalid field value", 123, row.get(0)); // "int"
    Assert.assertEquals("Invalid field value", "The Message", row.get(1)); // "str"
    Assert.assertEquals("Invalid field value", 234F, row.get(2)); // "float"
  }

  @Test
  public void messageValid() throws Exception {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(MorphlineTranslator.ENCODING_KEY, "UTF-8");
    configMap.put(MorphlineTranslator.ENCODING_MSG, "UTF-16");
    configMap.put(MorphlineTranslator.MORPHLINE, getResourcePath(MORPHLINE_FILE));
    configMap.put(MorphlineTranslator.MORPHLINE_ID, "encoding-message");
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + ComponentFactory.TYPE_CONFIG_NAME, "flat");
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_NAMES_CONFIG,
        Lists.newArrayList("int", "str", "float"));
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_TYPES_CONFIG,
        Lists.newArrayList("integer", "string", "float"));
    Config config = ConfigFactory.parseMap(configMap);

    translator.configure(config);
    String message = "\u16b7";
    Row raw = TestingMessageFactory.get(
        "The Key".getBytes("UTF-8"), DataTypes.BinaryType,
        message.getBytes("UTF-16"), DataTypes.BinaryType);
    Iterable<Row> result = translator.translate(raw);
    Row row = result.iterator().next();

    Assert.assertNotNull("Row is null", result);
    Assert.assertEquals("Invalid number of fields", 3, row.length());
    Assert.assertEquals("Invalid field value", 123, row.get(0)); // "int"
    Assert.assertEquals("Invalid field value", message, row.get(1)); // "str"
    Assert.assertEquals("Invalid field value", 234F, row.get(2)); // "float"
  }

  @Test
  public void messageInvalid() throws Exception {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(MorphlineTranslator.ENCODING_KEY, "UTF-8");
    configMap.put(MorphlineTranslator.ENCODING_MSG, "US-ASCII");
    configMap.put(MorphlineTranslator.MORPHLINE, getResourcePath(MORPHLINE_FILE));
    configMap.put(MorphlineTranslator.MORPHLINE_ID, "encoding-message");
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + ComponentFactory.TYPE_CONFIG_NAME, "flat");
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_NAMES_CONFIG,
        Lists.newArrayList("int", "str", "float"));
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_TYPES_CONFIG,
        Lists.newArrayList("integer", "string", "float"));
    Config config = ConfigFactory.parseMap(configMap);

    translator.configure(config);
    String message = "\u16b7";
    Row raw = TestingMessageFactory.get(
        "The Key".getBytes("UTF-8"), DataTypes.BinaryType,
        message.getBytes("UTF-16"), DataTypes.BinaryType);
    Iterable<Row> result = translator.translate(raw);
    Row row = result.iterator().next();

    Assert.assertNotNull("Row is null", result);
    Assert.assertEquals("Invalid number of fields", 3, row.length());
    Assert.assertEquals("Invalid field value", 123, row.get(0)); // "int"
    Assert.assertFalse("Invalid encoded field value", message.equals(row.get(1))); // "str"
    Assert.assertEquals("Invalid field value", 234F, row.get(2)); // "float"
  }

  // TODO : Consider part of MorphlineUtils.executePipeline? (And produce via mocks?)
  @Test (expected = MorphlineRuntimeException.class)
  public void noRecordReturned() throws Exception {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(MorphlineTranslator.ENCODING_KEY, "UTF-8");
    configMap.put(MorphlineTranslator.ENCODING_MSG, "UTF-8");
    configMap.put(MorphlineTranslator.MORPHLINE, getResourcePath(MORPHLINE_FILE));
    configMap.put(MorphlineTranslator.MORPHLINE_ID, "no-return");
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + ComponentFactory.TYPE_CONFIG_NAME, "flat");
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_NAMES_CONFIG,
        Lists.newArrayList("int", "str", "float"));
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_TYPES_CONFIG,
        Lists.newArrayList("integer", "string", "float"));
    Config config = ConfigFactory.parseMap(configMap);

    translator.configure(config);
    Row raw = TestingMessageFactory.get("The Key", DataTypes.StringType, 
        "The Message", DataTypes.StringType);
    translator.translate(raw);
  }

  // TODO : Consider part of MorphlineUtils.executePipeline? (And produce via mocks?)
  // Invalid command
  @Test (expected = MorphlineCompilationException.class)
  public void invalidCommand() throws Exception {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(MorphlineTranslator.ENCODING_KEY, "UTF-8");
    configMap.put(MorphlineTranslator.ENCODING_MSG, "UTF-8");
    configMap.put(MorphlineTranslator.MORPHLINE, getResourcePath(MORPHLINE_FILE));
    configMap.put(MorphlineTranslator.MORPHLINE_ID, "invalid-command");
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + ComponentFactory.TYPE_CONFIG_NAME, "flat");
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_NAMES_CONFIG,
        Lists.newArrayList("int", "str", "float"));
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_TYPES_CONFIG,
        Lists.newArrayList("integer", "string", "float"));
    Config config = ConfigFactory.parseMap(configMap);

    translator.configure(config);
    Row raw = TestingMessageFactory.get("The Key", DataTypes.StringType,
        "The Message", DataTypes.StringType);
    translator.translate(raw);
  }

  // TODO : Consider part of MorphlineUtils.executePipeline? (And produce via mocks?)
  // Failed process
  @Test (expected = MorphlineRuntimeException.class)
  public void failedProcess() throws Exception {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(MorphlineTranslator.ENCODING_KEY, "UTF-8");
    configMap.put(MorphlineTranslator.ENCODING_MSG, "UTF-8");
    configMap.put(MorphlineTranslator.MORPHLINE, getResourcePath(MORPHLINE_FILE));
    configMap.put(MorphlineTranslator.MORPHLINE_ID, "failed-process");
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + ComponentFactory.TYPE_CONFIG_NAME, "flat");
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_NAMES_CONFIG,
        Lists.newArrayList("int", "str", "float"));
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_TYPES_CONFIG,
        Lists.newArrayList("integer", "string", "float"));
    Config config = ConfigFactory.parseMap(configMap);

    translator.configure(config);
    Row raw = TestingMessageFactory.get("The Key", DataTypes.StringType, 
        "The Message", DataTypes.StringType);
    translator.translate(raw);
  }

  @Test
  public void messageOnlyValid() throws Exception {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(MorphlineTranslator.ENCODING_KEY, "UTF-8");
    configMap.put(MorphlineTranslator.ENCODING_MSG, "UTF-16");
    configMap.put(MorphlineTranslator.MORPHLINE, getResourcePath(MORPHLINE_FILE));
    configMap.put(MorphlineTranslator.MORPHLINE_ID, "encoding-message");
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + ComponentFactory.TYPE_CONFIG_NAME, "flat");
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_NAMES_CONFIG,
        Lists.newArrayList("int", "str", "float"));
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_TYPES_CONFIG,
        Lists.newArrayList("integer", "string", "float"));
    Config config = ConfigFactory.parseMap(configMap);

    translator.configure(config);
    String message = "\u16b7";
    Row raw = TestingMessageFactory.get(message.getBytes("UTF-16"), DataTypes.BinaryType);
    Iterable<Row> result = translator.translate(raw);
    Row row = result.iterator().next();

    Assert.assertNotNull("Row is null", result);
    Assert.assertEquals("Invalid number of fields", 3, row.length());
    Assert.assertEquals("Invalid field value", 123, row.get(0)); // "int"
    Assert.assertEquals("Invalid field value", message, row.get(1)); // "str"
    Assert.assertEquals("Invalid field value", 234F, row.get(2)); // "float"
  }

  @Test
  public void messageOnlyInvalid() throws Exception {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(MorphlineTranslator.ENCODING_KEY, "UTF-8");
    configMap.put(MorphlineTranslator.ENCODING_MSG, "US-ASCII");
    configMap.put(MorphlineTranslator.MORPHLINE, getResourcePath(MORPHLINE_FILE));
    configMap.put(MorphlineTranslator.MORPHLINE_ID, "encoding-message");
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + ComponentFactory.TYPE_CONFIG_NAME, "flat");
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_NAMES_CONFIG,
        Lists.newArrayList("int", "str", "float"));
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_TYPES_CONFIG,
        Lists.newArrayList("integer", "string", "float"));
    Config config = ConfigFactory.parseMap(configMap);

    translator.configure(config);
    String message = "\u16b7";
    Row raw = TestingMessageFactory.get(message.getBytes("UTF-16"), DataTypes.BinaryType);
    Iterable<Row> result = translator.translate(raw);
    Row row = result.iterator().next();

    Assert.assertNotNull("Row is null", result);
    Assert.assertEquals("Invalid number of fields", 3, row.length());
    Assert.assertEquals("Invalid field value", 123, row.get(0)); // "int"
    Assert.assertFalse("Invalid encoded field value", message.equals(row.get(1))); // "str"
    Assert.assertEquals("Invalid field value", 234F, row.get(2)); // "float"
  }

  @Test
  public void multipleRecords(
      final @Mocked MorphlineUtils.Collector collector
  ) throws Exception {

    final Record record1 = new Record();
    record1.put("foo", 123);

    final Record record2 = new Record();
    record2.put("foo", 234);

    Iterable<Row> expectedRows = Lists.newArrayList(
        RowFactory.create(123),
        RowFactory.create(234)
    );

    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(MorphlineTranslator.ENCODING_KEY, "UTF-8");
    configMap.put(MorphlineTranslator.ENCODING_MSG, "UTF-8");
    configMap.put(MorphlineTranslator.MORPHLINE, getResourcePath(MORPHLINE_FILE));
    configMap.put(MorphlineTranslator.MORPHLINE_ID, "multi-record");
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + ComponentFactory.TYPE_CONFIG_NAME, "flat");
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_NAMES_CONFIG,
        Lists.newArrayList("foo"));
    configMap.put(MorphlineTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_TYPES_CONFIG,
        Lists.newArrayList("integer"));
    Config config = ConfigFactory.parseMap(configMap);

    new Expectations() {{
      collector.getRecords(); result = Lists.newArrayList(record1, record2);
      collector.process((Record) any); result = true;
    }};

    translator.configure(config);
    Row raw = TestingMessageFactory.get("The Message".getBytes(), DataTypes.BinaryType);
    Iterable<Row> result = Lists.newArrayList(translator.translate(raw));

    Assert.assertThat("Invalid Iterator<Row> contents", result, CoreMatchers.is(expectedRows));

  }

}
