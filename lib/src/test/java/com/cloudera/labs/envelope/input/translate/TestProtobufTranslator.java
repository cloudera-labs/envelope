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

import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.utils.TestProtobufUtils;
import com.google.protobuf.ByteString;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.functions;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static com.cloudera.labs.envelope.validate.ValidationAssert.assertNoValidationFailures;
import static com.cloudera.labs.envelope.validate.ValidationAssert.assertValidationFailures;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class TestProtobufTranslator {

  @ClassRule
  public static TemporaryFolder tempDir = new TemporaryFolder();

  private static final String ALIAS = "protobuf";
  private static final String SINGLE_EXAMPLE = "/protobuf/protobuf_single_message.desc";
  private static final String MULTIPLE_EXAMPLE = "/protobuf/protobuf_multiple_message.desc";

  private static File SINGLE_UNCOMPRESSED;
  private static File SINGLE_COMPRESSED;
  private static File SINGLE_REPEATING;
  private static File SINGLE_ONEOF;
  private static File MULTIPLE_UNCOMPRESSED;

  @BeforeClass
  public static void setupMessages() throws IOException {
    SINGLE_UNCOMPRESSED = createSingleMessage(tempDir.newFile());
    SINGLE_COMPRESSED = createSingleGzipMessage(tempDir.newFile());
    SINGLE_REPEATING = createSingleMessageRepeating(tempDir.newFile());
    SINGLE_ONEOF = createSingleMessageOneOf(tempDir.newFile());
    MULTIPLE_UNCOMPRESSED = createMultipleMessage(tempDir.newFile());
  }

  @Test
  public void getAlias() {
    ProtobufTranslator translator = new ProtobufTranslator();
    assertThat(translator.getAlias(), is(ALIAS));
  }

  @Test
  public void getSchema() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(ProtobufTranslator.CONFIG_DESCRIPTOR_FILEPATH,
        TestProtobufTranslator.class.getResource(SINGLE_EXAMPLE).getPath());
    Config config = ConfigFactory.parseMap(configMap);

    ProtobufTranslator translator = new ProtobufTranslator();
    assertNoValidationFailures(translator, config);
    translator.configure(config);

    assertThat(translator.getSchema(), is(TestProtobufUtils.SINGLE_SCHEMA));
  }

  @Test
  public void configure() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(ProtobufTranslator.CONFIG_DESCRIPTOR_FILEPATH,
        TestProtobufTranslator.class.getResource(SINGLE_EXAMPLE).getPath());
    Config config = ConfigFactory.parseMap(configMap);

    ProtobufTranslator translator = new ProtobufTranslator();
    assertNoValidationFailures(translator, config);
    translator.configure(config);
  }

  @Test
  public void configureMissingFilepath() {
    Config config = ConfigFactory.empty();

    ProtobufTranslator translator = new ProtobufTranslator();
    assertValidationFailures(translator, config);
  }

  @Test
  public void configureWrongTypeFilepath() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(ProtobufTranslator.CONFIG_DESCRIPTOR_FILEPATH, new HashMap<>());
    Config config = ConfigFactory.parseMap(configMap);

    ProtobufTranslator translator = new ProtobufTranslator();
    assertValidationFailures(translator, config);
  }

  @Test(expected = RuntimeException.class)
  public void configureIllegalFilepath() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(ProtobufTranslator.CONFIG_DESCRIPTOR_FILEPATH, "not found");
    Config config = ConfigFactory.parseMap(configMap);

    ProtobufTranslator translator = new ProtobufTranslator();
    assertNoValidationFailures(translator, config);
    translator.configure(config);
  }

  @Test
  public void configureBlankFilepath() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(ProtobufTranslator.CONFIG_DESCRIPTOR_FILEPATH, "");
    Config config = ConfigFactory.parseMap(configMap);

    ProtobufTranslator translator = new ProtobufTranslator();
    assertValidationFailures(translator, config);
  }

  @Test (expected = RuntimeException.class)
  public void configMultipleNoDesignation() {
    String descPath = TestProtobufTranslator.class.getResource(MULTIPLE_EXAMPLE).getPath();

    Map<String, Object> configMap = new HashMap<>();
    configMap.put(ProtobufTranslator.CONFIG_DESCRIPTOR_FILEPATH, descPath);
    Config config = ConfigFactory.parseMap(configMap);

    ProtobufTranslator translator = new ProtobufTranslator();
    assertNoValidationFailures(translator, config);
    translator.configure(config);
  }

  @Test
  public void configMultipleWrongTypeDesignation() {
    String descPath = TestProtobufTranslator.class.getResource(MULTIPLE_EXAMPLE).getPath();

    Map<String, Object> configMap = new HashMap<>();
    configMap.put(ProtobufTranslator.CONFIG_DESCRIPTOR_FILEPATH, descPath);
    configMap.put(ProtobufTranslator.CONFIG_DESCRIPTOR_MESSAGE, new HashMap<>());
    Config config = ConfigFactory.parseMap(configMap);

    ProtobufTranslator translator = new ProtobufTranslator();
    assertValidationFailures(translator, config);
  }

  @Test
  public void configMultipleBlankDesignation() {
    String descPath = TestProtobufTranslator.class.getResource(MULTIPLE_EXAMPLE).getPath();

    Map<String, Object> configMap = new HashMap<>();
    configMap.put(ProtobufTranslator.CONFIG_DESCRIPTOR_FILEPATH, descPath);
    configMap.put(ProtobufTranslator.CONFIG_DESCRIPTOR_MESSAGE, "");
    Config config = ConfigFactory.parseMap(configMap);

    ProtobufTranslator translator = new ProtobufTranslator();
    assertValidationFailures(translator, config);
  }

  @Test
  public void loader() {
    String descPath = TestProtobufTranslator.class.getResource(SINGLE_EXAMPLE).getPath();

    Map<String, Object> configMap = new HashMap<>();
    configMap.put(TranslatorFactory.TYPE_CONFIG_NAME, ALIAS);
    configMap.put(ProtobufTranslator.CONFIG_DESCRIPTOR_FILEPATH, descPath);
    Config config = ConfigFactory.parseMap(configMap);

    Translator translator = TranslatorFactory.create(config, true);
    assertThat(translator, instanceOf(Translator.class));
  }

  @Test
  public void translateSingle() throws Exception {
    String descPath = TestProtobufTranslator.class.getResource(SINGLE_EXAMPLE).getPath();

    Map<String, Object> configMap = new HashMap<>();
    configMap.put(ProtobufTranslator.CONFIG_DESCRIPTOR_FILEPATH, descPath);
    Config config = ConfigFactory.parseMap(configMap);

    ProtobufTranslator translator = new ProtobufTranslator();
    assertNoValidationFailures(translator, config);
    translator.configure(config);

    byte[] key = "foo".getBytes();
    byte[] payload = Files.readAllBytes(SINGLE_UNCOMPRESSED.toPath());

    Iterable<Row> results = translator.translate(key, payload);
    System.out.println("results = " + results);

    Iterator<Row> rowIterator = results.iterator();

    assertThat(rowIterator.hasNext(), is(true));
    Row row = rowIterator.next();
    assertThat(row.size(), is(24));

    assertThat(row.getString(0), is("single message"));
    assertThat(row.getDouble(1), is(1.1D));
    assertThat(row.getFloat(2), is(1.1F));
    assertThat(row.getInt(3), is(2));
    assertThat(row.getLong(4), is(2L));
    assertThat(row.getInt(5), is(3));
    assertThat(row.getLong(6), is(3L));
    assertThat(row.getInt(7), is(4));
    assertThat(row.getLong(8), is(4L));
    assertThat(row.getInt(9), is(5));
    assertThat(row.getLong(10), is(5L));
    assertThat(row.getInt(11), is(6));
    assertThat(row.getLong(12), is(6L));
    assertThat(row.getBoolean(13), is(true));
    assertThat((byte[]) row.get(14), is("test".getBytes()));
    assertThat(row.getString(15), is("TWO"));
    assertThat(row.getStruct(16), is(RowFactory.create("nested message")));

    assertThat(row.getJavaMap(17).size(), is(1));
    assertThat((int) row.getJavaMap(17).get("one"), is(1));

    assertThat(row.getList(18).size(), is(1));
    Row nestedMessage = (Row) row.getList(18).get(0);
    assertThat(nestedMessage.getString(0), is("nested repeating message"));

    assertThat(row.getList(19).size(), is(1));
    assertThat((int) row.getList(19).get(0), is(7));

    assertThat(row.getList(20).size(), is(1));
    assertThat((String) row.getList(20).get(0), is("THREE"));

    assertThat(row.getString(21), is("oneof string"));
    assertThat(row.get(22), nullValue());
    assertThat(row.get(23), nullValue());

    assertThat(rowIterator.hasNext(), is(false));
  }

  @Test
  public void translateSingleGzipped() throws Exception {
    String descPath = TestProtobufTranslator.class.getResource(SINGLE_EXAMPLE).getPath();

    Map<String, Object> configMap = new HashMap<>();
    configMap.put(ProtobufTranslator.CONFIG_DESCRIPTOR_FILEPATH, descPath);
    Config config = ConfigFactory.parseMap(configMap);

    ProtobufTranslator translator = new ProtobufTranslator();
    assertNoValidationFailures(translator, config);
    translator.configure(config);

    byte[] key = "foo".getBytes();
    byte[] payload = Files.readAllBytes(SINGLE_COMPRESSED.toPath());

    Iterable<Row> results = translator.translate(key, payload);
    System.out.println("results = " + results);

    assertThat(results.iterator().hasNext(), is(true));
    Row row = results.iterator().next();
    assertThat(row.getString(0), is("single gzipped message"));
    assertThat(row.getBoolean(13), is(true));
    assertThat(row.get(14), nullValue());
  }

  @Test
  public void translateSingleRepeating() throws Exception {
    String descPath = TestProtobufTranslator.class.getResource(SINGLE_EXAMPLE).getPath();

    Map<String, Object> configMap = new HashMap<>();
    configMap.put(ProtobufTranslator.CONFIG_DESCRIPTOR_FILEPATH, descPath);
    Config config = ConfigFactory.parseMap(configMap);

    ProtobufTranslator translator = new ProtobufTranslator();
    assertNoValidationFailures(translator, config);
    translator.configure(config);

    byte[] key = "foo".getBytes();
    byte[] payload = Files.readAllBytes(SINGLE_REPEATING.toPath());

    Iterable<Row> results = translator.translate(key, payload);
    System.out.println("results = " + results);

    assertThat(results.iterator().hasNext(), is(true));
    Row row = results.iterator().next();
    assertThat(row.getString(0), is("repeating message"));
    List<Row> nested = row.getList(18);
    assertThat(nested.size(), is(1));
    assertThat(nested.get(0).getString(0), is("nested"));
  }

  @Test
  public void translateSingleOneOf() throws Exception {
    String descPath = TestProtobufTranslator.class.getResource(SINGLE_EXAMPLE).getPath();

    Map<String, Object> configMap = new HashMap<>();
    configMap.put(ProtobufTranslator.CONFIG_DESCRIPTOR_FILEPATH, descPath);
    Config config = ConfigFactory.parseMap(configMap);

    ProtobufTranslator translator = new ProtobufTranslator();
    assertNoValidationFailures(translator, config);
    translator.configure(config);

    byte[] key = "foo".getBytes();
    byte[] payload = Files.readAllBytes(SINGLE_ONEOF.toPath());

    Iterable<Row> results = translator.translate(key, payload);
    System.out.println("results = " + results);

    assertThat(results.iterator().hasNext(), is(true));
    Row row = results.iterator().next();
    assertThat(row.getString(0), is("oneof message"));
    assertThat(row.getString(21), is("choose me"));
  }

  @Test
  public void translateMultiple() throws Exception {
    String descPath = TestProtobufTranslator.class.getResource(MULTIPLE_EXAMPLE).getPath();

    Map<String, Object> configMap = new HashMap<>();
    configMap.put(ProtobufTranslator.CONFIG_DESCRIPTOR_FILEPATH, descPath);
    configMap.put(ProtobufTranslator.CONFIG_DESCRIPTOR_MESSAGE, "OtherExample");
    Config config = ConfigFactory.parseMap(configMap);

    ProtobufTranslator translator = new ProtobufTranslator();
    assertNoValidationFailures(translator, config);
    translator.configure(config);

    byte[] key = "foo".getBytes();
    byte[] payload = Files.readAllBytes(MULTIPLE_UNCOMPRESSED.toPath());

    Iterable<Row> results = translator.translate(key, payload);

    assertThat(results.iterator().hasNext(), is(true));
    Row row = results.iterator().next();
    assertThat(row.getString(0), is("other"));
  }

  @Test(expected = RuntimeException.class)
  public void translateInvalidPayload() throws Exception {
    String descPath = TestProtobufTranslator.class.getResource(SINGLE_EXAMPLE).getPath();

    Map<String, Object> configMap = new HashMap<>();
    configMap.put(ProtobufTranslator.CONFIG_DESCRIPTOR_FILEPATH, descPath);
    Config config = ConfigFactory.parseMap(configMap);

    ProtobufTranslator translator = new ProtobufTranslator();
    assertNoValidationFailures(translator, config);
    translator.configure(config);

    byte[] key = "foo".getBytes();
    byte[] payload = new byte[]{};

    translator.translate(key, payload);
  }

  @Test
  public void translateIntegration() throws Exception {
    String descPath = TestProtobufTranslator.class.getResource(SINGLE_EXAMPLE).getPath();

    Map<String, Object> configMap = new HashMap<>();
    configMap.put(ProtobufTranslator.CONFIG_DESCRIPTOR_FILEPATH, descPath);
    Config config = ConfigFactory.parseMap(configMap);

    ProtobufTranslator translator = new ProtobufTranslator();
    assertNoValidationFailures(translator, config);
    translator.configure(config);

    byte[] key = "foo".getBytes();
    byte[] payload = Files.readAllBytes(SINGLE_REPEATING.toPath());

    Iterable<Row> results = translator.translate(key, payload);
    List<Row> rowList = new ArrayList<>();
    for (Row row : results) {
      rowList.add(row);
    }

    Dataset<Row> test = Contexts.getSparkSession().createDataFrame(rowList, translator.getSchema());

    assertThat(test.count(), is(1L));

    test.show(false);
    test.select(functions.explode(functions.col("repeating_message"))).printSchema();

    List<Row> select = test.selectExpr("string").collectAsList();
    assertThat(select.size(), is(1));
    assertThat(select.get(0).size(), is(1));
    assertThat(select.get(0).get(0), instanceOf(String.class));
    assertThat(select.get(0).getString(0), is("repeating message"));
  }

  private static File createSingleMessageOneOf(File temp) {
    ProtobufSingleMessage.SingleExample msg = ProtobufSingleMessage.SingleExample.newBuilder()
        .setString("oneof message")
        .setOneofString("choose me")
        .build();

    try (FileOutputStream writer = new FileOutputStream(temp)) {
      msg.writeTo(writer);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return temp;
  }

  private static File createSingleMessageRepeating(File temp) {
    ProtobufSingleMessage.SingleExample msg = ProtobufSingleMessage.SingleExample.newBuilder()
        .setString("repeating message")
        .addRepeatingMessage(ProtobufSingleMessage.SingleExample.NestedExample.newBuilder().setNested("nested"))
        .build();

    try (FileOutputStream writer = new FileOutputStream(temp)) {
      msg.writeTo(writer);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return temp;
  }

  private static File createSingleMessage(File temp) {
    ProtobufSingleMessage.SingleExample msg = ProtobufSingleMessage.SingleExample.newBuilder()
        .setString("single message")
        .setDouble(1.1D)
        .setFloat(1.1F)
        .setInt32(2)
        .setInt64(2L)
        .setUint32(3)
        .setUint64(3L)
        .setSint32(4)
        .setSint64(4L)
        .setFixed32(5)
        .setFixed64(5L)
        .setSfixed32(6)
        .setSfixed64(6L)
        .setBoolean(true)
        .setBytes(ByteString.copyFrom("test".getBytes()))
        .setEnum(ProtobufSingleMessage.SingleExample.EnumExample.TWO)
        .setNested(ProtobufSingleMessage.SingleExample.NestedExample.newBuilder()
          .setNested("nested message").build()
        )
        .putMapInt("one", 1)
        .addRepeatingMessage(ProtobufSingleMessage.SingleExample.NestedExample.newBuilder()
          .setNested("nested repeating message")
        )
        .addRepeatingInt32(7)
        .addRepeatingEnum(ProtobufSingleMessage.SingleExample.EnumExample.THREE)
        .setOneofString("oneof string")
        .build();

    try (FileOutputStream writer = new FileOutputStream(temp)) {
      msg.writeTo(writer);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return temp;
  }

  private static File createSingleGzipMessage(File temp) {
    ProtobufSingleMessage.SingleExample msg = ProtobufSingleMessage.SingleExample.newBuilder()
        .setString("single gzipped message")
        .setBoolean(true)
        .build();

    try (GZIPOutputStream writer = new GZIPOutputStream(new FileOutputStream(temp))) {
      msg.writeTo(writer);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return temp;
  }

  private static File createMultipleMessage(File temp) {
    ProtobufMultipleMessage.OtherExample otherMsg = ProtobufMultipleMessage.OtherExample.newBuilder()
        .setOther("other").build();

    try (FileOutputStream writer = new FileOutputStream(temp)) {
      otherMsg.writeTo(writer);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return temp;
  }

}