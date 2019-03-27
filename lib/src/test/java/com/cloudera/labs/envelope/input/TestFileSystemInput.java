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

package com.cloudera.labs.envelope.input;

import com.cloudera.labs.envelope.component.ComponentFactory;
import com.cloudera.labs.envelope.schema.AvroSchema;
import com.cloudera.labs.envelope.schema.FlatSchema;
import com.cloudera.labs.envelope.schema.TestAvroSchema;
import com.cloudera.labs.envelope.translate.DummyInputFormatTranslator;
import com.cloudera.labs.envelope.translate.KVPTranslator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.cloudera.labs.envelope.validate.ValidationAssert.assertNoValidationFailures;
import static com.cloudera.labs.envelope.validate.ValidationAssert.assertValidationFailures;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestFileSystemInput {

  private static final String CSV_DATA = "/filesystem/sample-fs.csv";
  private static final String JSON_DATA = "/filesystem/sample-fs.json";
  private static final String TEXT_DATA = "/filesystem/sample-fs.txt";

  private Config config = ConfigFactory.empty();

  @Test
  public void missingFormat() {
    config = ConfigFactory.parseString(FileSystemInput.FORMAT_CONFIG + ": null").withFallback(config);
    FileSystemInput fileSystemInput = new FileSystemInput();
    assertValidationFailures(fileSystemInput, config);
  }

  @Test
  public void invalidFormat() {
    config = ConfigFactory.parseString(FileSystemInput.FORMAT_CONFIG + ": WILLGOBOOM").withFallback(config);
    FileSystemInput fileSystemInput = new FileSystemInput();
    assertValidationFailures(fileSystemInput, config);
  }

  @Test
  public void missingPath() {
    config = ConfigFactory.parseString(FileSystemInput.PATH_CONFIG + ": null").withFallback(config);
    FileSystemInput fileSystemInput = new FileSystemInput();
    assertValidationFailures(fileSystemInput, config);
  }

  @Test
  public void multipleSchema() {
    config = ConfigFactory.parseString(
        "schema {\n" +
        "  type = flat\n" +
        "  field.names = [A Long, An Integer]\n" +
        "  field.types = [long, integer]\n" +
        "}\n" +
        "schema {\n" +
        "  type = avro\n" +
        "  filepath = schemafilepath.avsc\n" +
        "}");
    FileSystemInput fileSystemInput = new FileSystemInput();
    assertValidationFailures(fileSystemInput, config);
  }

  @Test
  public void readCsvNoOptions() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "csv");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(CSV_DATA).getPath());
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput csvInput = new FileSystemInput();
    assertNoValidationFailures(csvInput, config);
    csvInput.configure(config);

    Dataset<Row> dataFrame = csvInput.read();
    assertEquals(4, dataFrame.count());

    Row first = dataFrame.first();
    assertEquals("Four", first.getString(3));
  }

  @Test
  public void readCsvWithOptions() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "csv");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(CSV_DATA).getPath());
    paramMap.put(FileSystemInput.CSV_HEADER_CONFIG, "true");
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput csvInput = new FileSystemInput();
    assertNoValidationFailures(csvInput, config);
    csvInput.configure(config);

    Dataset<Row> dataFrame = csvInput.read();
    assertEquals(3, dataFrame.count());

    Row first = dataFrame.first();
    assertEquals("four", first.getString(3));
  }

  @Test
  public void readCsvWithFieldsSchema() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "csv");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(CSV_DATA).getPath());
    paramMap.put(FileSystemInput.CSV_HEADER_CONFIG, "true");
    paramMap.put(FileSystemInput.SCHEMA_CONFIG + "." + ComponentFactory.TYPE_CONFIG_NAME, "flat");
    paramMap.put(FileSystemInput.SCHEMA_CONFIG + "." + FlatSchema.FIELD_NAMES_CONFIG,
                 Lists.newArrayList("A Long", "An Int", "A String", "Another String"));
    paramMap.put(FileSystemInput.SCHEMA_CONFIG + "." + FlatSchema.FIELD_TYPES_CONFIG,
                 Lists.newArrayList("long", "integer", "string", "string"));
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput csvInput = new FileSystemInput();
    assertNoValidationFailures(csvInput, config);
    csvInput.configure(config);

    Dataset<Row> dataFrame = csvInput.read();
    assertEquals(3, dataFrame.count());

    Row first = dataFrame.first();
    assertEquals("four", first.getString(3));
    assertEquals("Another String", first.schema().fields()[3].name());
    assertEquals(1L, first.get(0));
    assertEquals(DataTypes.LongType, first.schema().fields()[0].dataType());
  }

  @Test
  public void readCsvWithAvroSchema() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "csv");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(CSV_DATA).getPath());
    paramMap.put(FileSystemInput.CSV_HEADER_CONFIG, "true");
    paramMap.put(FileSystemInput.SCHEMA_CONFIG + "." + ComponentFactory.TYPE_CONFIG_NAME, "avro");
    paramMap.put(FileSystemInput.SCHEMA_CONFIG + "." + AvroSchema.AVRO_FILE_CONFIG,
                 FileSystemInput.class.getResource(TestAvroSchema.AVRO_SCHEMA_DATA).getPath());
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput csvInput = new FileSystemInput();
    assertNoValidationFailures(csvInput, config);
    csvInput.configure(config);

    Dataset<Row> dataFrame = csvInput.read();
    assertEquals(3, dataFrame.count());

    Row first = dataFrame.first();
    assertEquals("four", first.getString(3));
    assertEquals("Another_String", first.schema().fields()[3].name());
    assertEquals(1L, first.get(0));
    assertEquals(DataTypes.LongType, first.schema().fields()[0].dataType());
  }

  @Test
  public void readJsonNoOptions() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "json");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(JSON_DATA).getPath());
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput csvInput = new FileSystemInput();
    assertNoValidationFailures(csvInput, config);
    csvInput.configure(config);

    Dataset<Row> dataFrame = csvInput.read();
    assertEquals(4, dataFrame.count());

    Row first = dataFrame.first();
    assertEquals("dog", first.getString(3));
    assertEquals("field1", first.schema().fields()[0].name());
    assertEquals(DataTypes.LongType, first.schema().fields()[0].dataType());
  }

  @Test
  public void readJsonWithSchema() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "json");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(JSON_DATA).getPath());
    paramMap.put(FileSystemInput.SCHEMA_CONFIG + "." + ComponentFactory.TYPE_CONFIG_NAME, "flat");
    paramMap.put(FileSystemInput.SCHEMA_CONFIG + "." + FlatSchema.FIELD_NAMES_CONFIG,
                 Lists.newArrayList("field1", "field2", "field3", "field4"));
    paramMap.put(FileSystemInput.SCHEMA_CONFIG + "." + FlatSchema.FIELD_TYPES_CONFIG,
                 Lists.newArrayList("integer", "string", "boolean", "string"));
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput csvInput = new FileSystemInput();
    assertNoValidationFailures(csvInput, config);
    csvInput.configure(config);

    Dataset<Row> dataFrame = csvInput.read();
    dataFrame.printSchema();
    dataFrame.show();

    assertEquals(4, dataFrame.count());

    Row first = dataFrame.first();
    assertEquals("dog", first.getString(3));
    assertEquals("field1", first.schema().fields()[0].name());
    assertEquals(DataTypes.IntegerType, first.schema().fields()[0].dataType());
  }

  @Test
  public void readInputFormatMissingInputFormat() {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "input-format");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(CSV_DATA).getPath());
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput formatInput = new FileSystemInput();
    assertValidationFailures(formatInput, config);
  }

  @Test
  public void readInputFormatMissingTranslator() {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "input-format");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(CSV_DATA).getPath());
    paramMap.put(FileSystemInput.INPUT_FORMAT_TYPE_CONFIG, TextInputFormat.class.getCanonicalName());
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput formatInput = new FileSystemInput();
    assertValidationFailures(formatInput, config);
  }

  @Test (expected = SparkException.class)
  public void readInputFormatMismatchTranslator() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "input-format");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(CSV_DATA).getPath());
    paramMap.put(FileSystemInput.INPUT_FORMAT_TYPE_CONFIG, KeyValueTextInputFormat.class.getCanonicalName());
    paramMap.put("translator.type", DummyInputFormatTranslator.class.getCanonicalName());
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput formatInput = new FileSystemInput();
    assertNoValidationFailures(formatInput, config);
    formatInput.configure(config);
    formatInput.read().show();
  }

  @Test
  public void readInputFormat() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "input-format");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(CSV_DATA).getPath());
    paramMap.put(FileSystemInput.INPUT_FORMAT_TYPE_CONFIG, TextInputFormat.class.getCanonicalName());
    paramMap.put("translator" + "." + ComponentFactory.TYPE_CONFIG_NAME,
        DummyInputFormatTranslator.class.getCanonicalName());
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput formatInput = new FileSystemInput();
    assertNoValidationFailures(formatInput, config);
    formatInput.configure(config);

    Dataset<Row> results = formatInput.read();

    assertEquals("Invalid number of rows", 4, results.count());
    assertEquals("Invalid first row result", 0L, results.first().getLong(0));
    assertEquals("Invalid first row result", "One,Two,Three,Four", results.first().getString(1));
  }
  
  @Test
  public void readTextWithoutTranslator() throws Exception {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(FileSystemInput.FORMAT_CONFIG, FileSystemInput.TEXT_FORMAT);
    configMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(TEXT_DATA).getPath());
    config = ConfigFactory.parseMap(configMap);
    
    FileSystemInput formatInput = new FileSystemInput();
    assertNoValidationFailures(formatInput, config);
    formatInput.configure(config);
    
    List<Row> results = formatInput.read().collectAsList();
    
    assertEquals(2, results.size());
    assertTrue(results.contains(RowFactory.create("a=1,b=hello,c=true")));
    assertTrue(results.contains(RowFactory.create("a=2,b=world,c=false")));
  }
  
  @Test
  public void readTextWithTranslator() throws Exception {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(FileSystemInput.FORMAT_CONFIG, FileSystemInput.TEXT_FORMAT);
    configMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(TEXT_DATA).getPath());
    configMap.put("translator.type", KVPTranslator.class.getName());
    configMap.put("translator.delimiter.kvp", ",");
    configMap.put("translator.delimiter.field", "=");
    configMap.put("translator.schema.type", "flat");
    configMap.put("translator.schema.field.names", Lists.newArrayList("a", "b", "c"));
    configMap.put("translator.schema.field.types", Lists.newArrayList("integer", "string", "boolean"));
    config = ConfigFactory.parseMap(configMap);
    
    FileSystemInput formatInput = new FileSystemInput();
    assertNoValidationFailures(formatInput, config);
    formatInput.configure(config);
    
    List<Row> results = formatInput.read().collectAsList();
    
    assertEquals(2, results.size());
    assertTrue(results.contains(RowFactory.create(1, "hello", true)));
    assertTrue(results.contains(RowFactory.create(2, "world", false)));
  }

  @Test
  public void readTextWithTranslatorWithAppendRaw() throws Exception {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(FileSystemInput.FORMAT_CONFIG, FileSystemInput.TEXT_FORMAT);
    configMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(TEXT_DATA).getPath());
    configMap.put("translator.type", KVPTranslator.class.getName());
    configMap.put("translator.delimiter.kvp", ",");
    configMap.put("translator.delimiter.field", "=");
    configMap.put("translator.schema.type", "flat");
    configMap.put("translator.schema.field.names", Lists.newArrayList("a", "b", "c"));
    configMap.put("translator.schema.field.types", Lists.newArrayList("integer", "string", "boolean"));
    configMap.put("translator.append.raw.enabled", true);
    config = ConfigFactory.parseMap(configMap);

    FileSystemInput formatInput = new FileSystemInput();
    assertNoValidationFailures(formatInput, config);
    formatInput.configure(config);

    List<Row> results = formatInput.read().collectAsList();

    assertEquals(2, results.size());
    assertTrue(results.contains(RowFactory.create(1, "hello", true, "a=1,b=hello,c=true")));
    assertTrue(results.contains(RowFactory.create(2, "world", false, "a=2,b=world,c=false")));
  }

}
