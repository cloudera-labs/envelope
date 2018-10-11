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

import com.cloudera.labs.envelope.utils.MorphlineUtils;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import mockit.Expectations;
import mockit.Mocked;
import mockit.integration.junit4.JMockit;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.hamcrest.CoreMatchers;
import org.junit.After;
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

/**
 *
 */
@RunWith(JMockit.class)
public class TestMorphlineTranslator {

  private static final String MORPHLINE_FILE = "/morphline.conf";

  private @Mocked Config config;

  private Translator<String, String> stringMorphline;
  private Translator<byte[], byte[]> byteMorphline;

  private String getResourcePath(String resource) {
    return TestMorphlineTranslator.class.getResource(resource).getPath();
  }

  @Before
  public void setup() {
    stringMorphline = new MorphlineTranslator<>();
    byteMorphline = new MorphlineTranslator<>();
  }

  @After
  public void teardown() {
    stringMorphline = null;
    byteMorphline = null;
    config = null;
  }

  @Test
  public void getSchema() throws Exception {

    // Relies on RowUtils.structTypeFor()

    new Expectations() {{
      config.getString(MorphlineTranslator.MORPHLINE); result = getResourcePath(MORPHLINE_FILE);
      config.getStringList(MorphlineTranslator.FIELD_NAMES); result = Lists.newArrayList("bar", "foo");
      config.getStringList(MorphlineTranslator.FIELD_TYPES); result = Lists.newArrayList("int", "string");
    }};

    stringMorphline.configure(config);
    StructType schema = stringMorphline.getSchema();

    Assert.assertEquals("Invalid number of SchemaFields", 2, schema.fields().length);
    Assert.assertEquals("Invalid DataType", DataTypes.IntegerType, schema.fields()[0].dataType());
    Assert.assertEquals("Invalid DataType", DataTypes.StringType, schema.fields()[1].dataType());
  }

  @Test (expected = RuntimeException.class)
  public void getSchemaInvalidDataType() throws Exception {

    // Relies on RowUtils.structTypeFor()

    new Expectations() {{
      config.getString(MorphlineTranslator.MORPHLINE); result = getResourcePath(MORPHLINE_FILE);
      config.getStringList(MorphlineTranslator.FIELD_NAMES); result = Lists.newArrayList("bar", "foo");
      config.getStringList(MorphlineTranslator.FIELD_TYPES); result = Lists.newArrayList("int", "boom");
    }};

    stringMorphline.configure(config);
  }

  @Test (expected = MorphlineCompilationException.class)
  public void morphlineCompilationError(
      final @Mocked Compiler compiler
      ) throws Exception {

    new Expectations() {{
      config.getString(MorphlineTranslator.MORPHLINE); result = getResourcePath(MORPHLINE_FILE);
      config.getStringList(MorphlineTranslator.FIELD_NAMES); result = Lists.newArrayList("bar");
      config.getStringList(MorphlineTranslator.FIELD_TYPES); result = Lists.newArrayList("int");

      compiler.compile((File) any, anyString, (MorphlineContext) any, (Command) any); result = new Exception("Compilation exception");
    }};

    stringMorphline.configure(config);
    stringMorphline.translate("The Key", "The Message");
  }

  @Test (expected = RuntimeException.class)
  public void conversionError() throws Exception {
    new Expectations() {{
      config.getString(MorphlineTranslator.ENCODING_KEY); result = "UTF-8";
      config.getString(MorphlineTranslator.ENCODING_MSG); result = "UTF-8";
      config.getString(MorphlineTranslator.MORPHLINE); result = getResourcePath(MORPHLINE_FILE);
      config.getString(MorphlineTranslator.MORPHLINE_ID); result = "default";
      config.getStringList(MorphlineTranslator.FIELD_NAMES); result = Lists.newArrayList("int", "str", "float");
      config.getStringList(MorphlineTranslator.FIELD_TYPES); result = Lists.newArrayList("int", "string", "boolean");
    }};

    stringMorphline.configure(config);
    stringMorphline.translate("The Key", "The Message");
  }

  @Test
  public void conversionSuccess() throws Exception {
    new Expectations() {{
      config.getString(MorphlineTranslator.ENCODING_KEY); result = "UTF-8";
      config.getString(MorphlineTranslator.ENCODING_MSG); result = "UTF-8";
      config.getString(MorphlineTranslator.MORPHLINE); result = getResourcePath(MORPHLINE_FILE);
      config.getString(MorphlineTranslator.MORPHLINE_ID); result = "default";
      config.getStringList(MorphlineTranslator.FIELD_NAMES); result = Lists.newArrayList("int", "str", "float");
      config.getStringList(MorphlineTranslator.FIELD_TYPES); result = Lists.newArrayList("int", "string", "float");
    }};

    stringMorphline.configure(config);
    Iterable<Row> result = stringMorphline.translate("The Key", "The Message");
    Row row = result.iterator().next();

    Assert.assertNotNull("Row is null", result);
    Assert.assertEquals("Invalid number of fields", 3, row.length());
    Assert.assertEquals("Invalid field value", 123, row.get(0)); // "int"
    Assert.assertEquals("Invalid field value", "The Message", row.get(1)); // "str"
    Assert.assertEquals("Invalid field value", 234F, row.get(2)); // "float"
  }

  @Test
  public void stringMessageValid() throws Exception {
    new Expectations() {{
      config.getString(MorphlineTranslator.ENCODING_KEY); result = "UTF-8";
      config.getString(MorphlineTranslator.ENCODING_MSG); result = "UTF-16";
      config.getString(MorphlineTranslator.MORPHLINE); result = getResourcePath(MORPHLINE_FILE);
      config.getString(MorphlineTranslator.MORPHLINE_ID); result = "encoding-message";
      config.getStringList(MorphlineTranslator.FIELD_NAMES); result = Lists.newArrayList("int", "str", "float");
      config.getStringList(MorphlineTranslator.FIELD_TYPES); result = Lists.newArrayList("int", "string", "float");
    }};

    stringMorphline.configure(config);
    String message = "\u16b7";
    Iterable<Row> result = stringMorphline.translate("The Key", message);
    Row row = result.iterator().next();

    Assert.assertNotNull("Row is null", result);
    Assert.assertEquals("Invalid number of fields", 3, row.length());
    Assert.assertEquals("Invalid field value", 123, row.get(0)); // "int"
    Assert.assertEquals("Invalid field value", message, row.get(1)); // "str"
    Assert.assertEquals("Invalid field value", 234F, row.get(2)); // "float"
  }

  @Test
  public void stringMessageInvalid() throws Exception {
    new Expectations() {{
      config.getString(MorphlineTranslator.ENCODING_KEY); result = "UTF-8";
      config.getString(MorphlineTranslator.ENCODING_MSG); result = "US-ASCII";
      config.getString(MorphlineTranslator.MORPHLINE); result = getResourcePath(MORPHLINE_FILE);
      config.getString(MorphlineTranslator.MORPHLINE_ID); result = "encoding-message";
      config.getStringList(MorphlineTranslator.FIELD_NAMES); result = Lists.newArrayList("int", "str", "float");
      config.getStringList(MorphlineTranslator.FIELD_TYPES); result = Lists.newArrayList("int", "string", "float");
    }};

    stringMorphline.configure(config);
    String message = "\u16b7";
    Iterable<Row> result = stringMorphline.translate("The Key", message);
    Row row = result.iterator().next();

    Assert.assertNotNull("Row is null", result);
    Assert.assertEquals("Invalid number of fields", 3, row.length());
    Assert.assertEquals("Invalid field value", 123, row.get(0)); // "int"
    Assert.assertFalse("Invalid encoded field value", message.equals(row.get(1))); // "str"
    Assert.assertEquals("Invalid field value", 234F, row.get(2)); // "float"
  }

  @Test
  public void byteMessageValid() throws Exception {
    new Expectations() {{
      config.getString(MorphlineTranslator.ENCODING_KEY); result = "UTF-8";
      config.getString(MorphlineTranslator.ENCODING_MSG); result = "UTF-16";
      config.getString(MorphlineTranslator.MORPHLINE); result = getResourcePath(MORPHLINE_FILE);
      config.getString(MorphlineTranslator.MORPHLINE_ID); result = "encoding-message";
      config.getStringList(MorphlineTranslator.FIELD_NAMES); result = Lists.newArrayList("int", "str", "float");
      config.getStringList(MorphlineTranslator.FIELD_TYPES); result = Lists.newArrayList("int", "string", "float");
    }};

    byteMorphline.configure(config);
    String message = "\u16b7";
    Iterable<Row> result = byteMorphline.translate("The Key".getBytes("UTF-8"), message.getBytes("UTF-16"));
    Row row = result.iterator().next();

    Assert.assertNotNull("Row is null", result);
    Assert.assertEquals("Invalid number of fields", 3, row.length());
    Assert.assertEquals("Invalid field value", 123, row.get(0)); // "int"
    Assert.assertEquals("Invalid field value", message, row.get(1)); // "str"
    Assert.assertEquals("Invalid field value", 234F, row.get(2)); // "float"
  }

  @Test
  public void byteMessageInvalid() throws Exception {
    new Expectations() {{
      config.getString(MorphlineTranslator.ENCODING_KEY); result = "UTF-8";
      config.getString(MorphlineTranslator.ENCODING_MSG); result = "US-ASCII";
      config.getString(MorphlineTranslator.MORPHLINE); result = getResourcePath(MORPHLINE_FILE);
      config.getString(MorphlineTranslator.MORPHLINE_ID); result = "encoding-message";
      config.getStringList(MorphlineTranslator.FIELD_NAMES); result = Lists.newArrayList("int", "str", "float");
      config.getStringList(MorphlineTranslator.FIELD_TYPES); result = Lists.newArrayList("int", "string", "float");
    }};

    byteMorphline.configure(config);
    String message = "\u16b7";
    Iterable<Row> result = byteMorphline.translate("The Key".getBytes("UTF-8"), message.getBytes("UTF-16"));
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
    new Expectations() {{
      config.getString(MorphlineTranslator.ENCODING_KEY); result = "UTF-8";
      config.getString(MorphlineTranslator.ENCODING_MSG); result = "UTF-8";
      config.getString(MorphlineTranslator.MORPHLINE); result = getResourcePath(MORPHLINE_FILE);
      config.getString(MorphlineTranslator.MORPHLINE_ID); result = "no-return";
      config.getStringList(MorphlineTranslator.FIELD_NAMES); result = Lists.newArrayList("int", "str", "float");
      config.getStringList(MorphlineTranslator.FIELD_TYPES); result = Lists.newArrayList("int", "string", "float");
    }};

    stringMorphline.configure(config);
    stringMorphline.translate("The Key", "The Message");
  }

  // TODO : Consider part of MorphlineUtils.executePipeline? (And produce via mocks?)
  // Invalid command
  @Test (expected = MorphlineCompilationException.class)
  public void invalidCommand() throws Exception {
    new Expectations() {{
      config.getString(MorphlineTranslator.ENCODING_KEY); result = "UTF-8";
      config.getString(MorphlineTranslator.ENCODING_MSG); result = "UTF-8";
      config.getString(MorphlineTranslator.MORPHLINE); result = getResourcePath(MORPHLINE_FILE);
      config.getString(MorphlineTranslator.MORPHLINE_ID); result = "invalid-command";
      config.getStringList(MorphlineTranslator.FIELD_NAMES); result = Lists.newArrayList("int", "str", "float");
      config.getStringList(MorphlineTranslator.FIELD_TYPES); result = Lists.newArrayList("int", "string", "float");
    }};

    stringMorphline.configure(config);
    stringMorphline.translate("The Key", "The Message");
  }

  // TODO : Consider part of MorphlineUtils.executePipeline? (And produce via mocks?)
  // Failed process
  @Test (expected = MorphlineRuntimeException.class)
  public void failedProcess() throws Exception {
    new Expectations() {{
      config.getString(MorphlineTranslator.ENCODING_KEY); result = "UTF-8";
      config.getString(MorphlineTranslator.ENCODING_MSG); result = "UTF-8";
      config.getString(MorphlineTranslator.MORPHLINE); result = getResourcePath(MORPHLINE_FILE);
      config.getString(MorphlineTranslator.MORPHLINE_ID); result = "failed-process";
      config.getStringList(MorphlineTranslator.FIELD_NAMES); result = Lists.newArrayList("int", "str", "float");
      config.getStringList(MorphlineTranslator.FIELD_TYPES); result = Lists.newArrayList("int", "string", "float");
    }};

    stringMorphline.configure(config);
    stringMorphline.translate("The Key", "The Message");
  }

  @Test
  public void stringMessageOnlyValid() throws Exception {
    new Expectations() {{
      config.getString(MorphlineTranslator.ENCODING_MSG); result = "UTF-16";
      config.getString(MorphlineTranslator.MORPHLINE); result = getResourcePath(MORPHLINE_FILE);
      config.getString(MorphlineTranslator.MORPHLINE_ID); result = "encoding-message";
      config.getStringList(MorphlineTranslator.FIELD_NAMES); result = Lists.newArrayList("int", "str", "float");
      config.getStringList(MorphlineTranslator.FIELD_TYPES); result = Lists.newArrayList("int", "string", "float");
    }};

    stringMorphline.configure(config);
    String message = "\u16b7";
    Iterable<Row> result = stringMorphline.translate(null, message);
    Row row = result.iterator().next();

    Assert.assertNotNull("Row is null", result);
    Assert.assertEquals("Invalid number of fields", 3, row.length());
    Assert.assertEquals("Invalid field value", 123, row.get(0)); // "int"
    Assert.assertEquals("Invalid field value", message, row.get(1)); // "str"
    Assert.assertEquals("Invalid field value", 234F, row.get(2)); // "float"
  }

  @Test
  public void stringMessageOnlyInvalid() throws Exception {
    new Expectations() {{
      config.getString(MorphlineTranslator.ENCODING_MSG); result = "US-ASCII";
      config.getString(MorphlineTranslator.MORPHLINE); result = getResourcePath(MORPHLINE_FILE);
      config.getString(MorphlineTranslator.MORPHLINE_ID); result = "encoding-message";
      config.getStringList(MorphlineTranslator.FIELD_NAMES); result = Lists.newArrayList("int", "str", "float");
      config.getStringList(MorphlineTranslator.FIELD_TYPES); result = Lists.newArrayList("int", "string", "float");
    }};

    stringMorphline.configure(config);
    String message = "\u16b7";
    Iterable<Row> result = stringMorphline.translate(null, message);
    Row row = result.iterator().next();

    Assert.assertNotNull("Row is null", result);
    Assert.assertEquals("Invalid number of fields", 3, row.length());
    Assert.assertEquals("Invalid field value", 123, row.get(0)); // "int"
    Assert.assertFalse("Invalid encoded field value", message.equals(row.get(1))); // "str"
    Assert.assertEquals("Invalid field value", 234F, row.get(2)); // "float"
  }

  @Test
  public void byteMessageOnlyValid() throws Exception {
    new Expectations() {{
      config.getString(MorphlineTranslator.ENCODING_MSG); result = "UTF-16";
      config.getString(MorphlineTranslator.MORPHLINE); result = getResourcePath(MORPHLINE_FILE);
      config.getString(MorphlineTranslator.MORPHLINE_ID); result = "encoding-message";
      config.getStringList(MorphlineTranslator.FIELD_NAMES); result = Lists.newArrayList("int", "str", "float");
      config.getStringList(MorphlineTranslator.FIELD_TYPES); result = Lists.newArrayList("int", "string", "float");
    }};

    byteMorphline.configure(config);
    String message = "\u16b7";
    Iterable<Row> result = byteMorphline.translate(null, message.getBytes("UTF-16"));
    Row row = result.iterator().next();

    Assert.assertNotNull("Row is null", result);
    Assert.assertEquals("Invalid number of fields", 3, row.length());
    Assert.assertEquals("Invalid field value", 123, row.get(0)); // "int"
    Assert.assertEquals("Invalid field value", message, row.get(1)); // "str"
    Assert.assertEquals("Invalid field value", 234F, row.get(2)); // "float"
  }

  @Test
  public void byteMessageOnlyInvalid() throws Exception {
    new Expectations() {{
      config.getString(MorphlineTranslator.ENCODING_MSG); result = "US-ASCII";
      config.getString(MorphlineTranslator.MORPHLINE); result = getResourcePath(MORPHLINE_FILE);
      config.getString(MorphlineTranslator.MORPHLINE_ID); result = "encoding-message";
      config.getStringList(MorphlineTranslator.FIELD_NAMES); result = Lists.newArrayList("int", "str", "float");
      config.getStringList(MorphlineTranslator.FIELD_TYPES); result = Lists.newArrayList("int", "string", "float");
    }};

    byteMorphline.configure(config);
    String message = "\u16b7";
    Iterable<Row> result = byteMorphline.translate(null, message.getBytes("UTF-16"));
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

    new Expectations() {{
      config.getString(MorphlineTranslator.ENCODING_MSG); result = "UTF-8";
      config.getString(MorphlineTranslator.MORPHLINE); result = getResourcePath(MORPHLINE_FILE);
      config.getString(MorphlineTranslator.MORPHLINE_ID); result = "multi-record";
      config.getStringList(MorphlineTranslator.FIELD_NAMES); result = Lists.newArrayList("foo");
      config.getStringList(MorphlineTranslator.FIELD_TYPES); result = Lists.newArrayList("int");

      collector.getRecords(); result = Lists.newArrayList(record1, record2);
      collector.process((Record) any); result = true;
    }};

    stringMorphline.configure(config);
    String message = "The Message";
    Iterable<Row> result = Lists.newArrayList(stringMorphline.translate(null, message));

    Assert.assertThat("Invalid Iterator<Row> contents", result, CoreMatchers.is(expectedRows));

  }

}