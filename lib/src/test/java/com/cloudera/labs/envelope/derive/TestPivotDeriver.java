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

package com.cloudera.labs.envelope.derive;

import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.cloudera.labs.envelope.validate.ValidationAssert.assertNoValidationFailures;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestPivotDeriver {

  @Test
  public void testDefaultDynamicPivot() throws Exception {
    List<Row> sourceList = Lists.newArrayList(
        RowFactory.create("A", "hello", "1"),
        RowFactory.create("A", "world", "2"),
        RowFactory.create("B", "hello", "3"),
        RowFactory.create("C", "world", "4"));
    StructType schema = RowUtils.structTypeFor(
        Lists.newArrayList("entity_id", "key", "value"),
        Lists.newArrayList("string", "string", "string"));
    Dataset<Row> source = Contexts.getSparkSession().createDataFrame(sourceList, schema);

    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("source", source);
    
    Config config = ConfigFactory.empty()
        .withValue(PivotDeriver.STEP_NAME_CONFIG, ConfigValueFactory.fromAnyRef("source"))
        .withValue(PivotDeriver.ENTITY_KEY_FIELD_NAMES_CONFIG, ConfigValueFactory.fromAnyRef(Lists.newArrayList("entity_id")))
        .withValue(PivotDeriver.PIVOT_KEY_FIELD_NAME_CONFIG, ConfigValueFactory.fromAnyRef("key"))
        .withValue(PivotDeriver.PIVOT_VALUE_FIELD_NAME_CONFIG, ConfigValueFactory.fromAnyRef("value"));

    PivotDeriver d = new PivotDeriver();
    assertNoValidationFailures(d, config);
    d.configure(config);
    
    List<Row> results = d.derive(dependencies).collectAsList();
    
    assertEquals(results.size(), 3);
    assertTrue(results.contains(RowFactory.create("A", "1", "2")));
    assertTrue(results.contains(RowFactory.create("B", "3", null)));
    assertTrue(results.contains(RowFactory.create("C", null, "4")));
  }
  
  @Test
  public void testIntegerDataType() throws Exception {
    List<Row> sourceList = Lists.newArrayList(
        RowFactory.create("A", "hello", 1),
        RowFactory.create("A", "world", 2),
        RowFactory.create("B", "hello", 3),
        RowFactory.create("C", "world", 4));
    StructType schema = RowUtils.structTypeFor(
        Lists.newArrayList("entity_id", "key", "value"),
        Lists.newArrayList("string", "string", "int"));
    Dataset<Row> source = Contexts.getSparkSession().createDataFrame(sourceList, schema);

    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("source", source);
    
    Config config = ConfigFactory.empty()
        .withValue(PivotDeriver.STEP_NAME_CONFIG, ConfigValueFactory.fromAnyRef("source"))
        .withValue(PivotDeriver.ENTITY_KEY_FIELD_NAMES_CONFIG, ConfigValueFactory.fromAnyRef(Lists.newArrayList("entity_id")))
        .withValue(PivotDeriver.PIVOT_KEY_FIELD_NAME_CONFIG, ConfigValueFactory.fromAnyRef("key"))
        .withValue(PivotDeriver.PIVOT_VALUE_FIELD_NAME_CONFIG, ConfigValueFactory.fromAnyRef("value"));

    PivotDeriver d = new PivotDeriver();
    assertNoValidationFailures(d, config);
    d.configure(config);
    
    List<Row> results = d.derive(dependencies).collectAsList();
    
    assertEquals(results.size(), 3);
    assertTrue(results.contains(RowFactory.create("A", 1, 2)));
    assertTrue(results.contains(RowFactory.create("B", 3, null)));
    assertTrue(results.contains(RowFactory.create("C", null, 4)));
  }
  
  @Test
  public void testDoubleDataType() throws Exception {
    List<Row> sourceList = Lists.newArrayList(
        RowFactory.create("A", "hello", 1.0),
        RowFactory.create("A", "world", 2.0),
        RowFactory.create("B", "hello", 3.0),
        RowFactory.create("C", "world", 4.0));
    StructType schema = RowUtils.structTypeFor(
        Lists.newArrayList("entity_id", "key", "value"),
        Lists.newArrayList("string", "string", "double"));
    Dataset<Row> source = Contexts.getSparkSession().createDataFrame(sourceList, schema);

    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("source", source);
    
    Config config = ConfigFactory.empty()
        .withValue(PivotDeriver.STEP_NAME_CONFIG, ConfigValueFactory.fromAnyRef("source"))
        .withValue(PivotDeriver.ENTITY_KEY_FIELD_NAMES_CONFIG, ConfigValueFactory.fromAnyRef(Lists.newArrayList("entity_id")))
        .withValue(PivotDeriver.PIVOT_KEY_FIELD_NAME_CONFIG, ConfigValueFactory.fromAnyRef("key"))
        .withValue(PivotDeriver.PIVOT_VALUE_FIELD_NAME_CONFIG, ConfigValueFactory.fromAnyRef("value"));

    PivotDeriver d = new PivotDeriver();
    assertNoValidationFailures(d, config);
    d.configure(config);
    
    List<Row> results = d.derive(dependencies).collectAsList();
    
    assertEquals(results.size(), 3);
    assertTrue(results.contains(RowFactory.create("A", 1.0, 2.0)));
    assertTrue(results.contains(RowFactory.create("B", 3.0, null)));
    assertTrue(results.contains(RowFactory.create("C", null, 4.0)));
  }
  
  @Test
  public void testMultipleFieldEntityKeyPivot() throws Exception {
    List<Row> sourceList = Lists.newArrayList(
        RowFactory.create("A", "AA", "AAA", "hello", "1"),
        RowFactory.create("A", "AA", "AAA", "world", "2"),
        RowFactory.create("B", "BB", "BBB", "hello", "3"),
        RowFactory.create("C", "CC", "CCC", "world", "4"));
    StructType schema = RowUtils.structTypeFor(
        Lists.newArrayList("entity_id1", "entity_id2", "entity_id3", "key", "value"),
        Lists.newArrayList("string", "string", "string", "string", "string"));
    Dataset<Row> source = Contexts.getSparkSession().createDataFrame(sourceList, schema);

    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("source", source);
    
    Config config = ConfigFactory.empty()
        .withValue(PivotDeriver.STEP_NAME_CONFIG, ConfigValueFactory.fromAnyRef("source"))
        .withValue(PivotDeriver.ENTITY_KEY_FIELD_NAMES_CONFIG, ConfigValueFactory.fromAnyRef(
            Lists.newArrayList("entity_id1", "entity_id2", "entity_id3")))
        .withValue(PivotDeriver.PIVOT_KEY_FIELD_NAME_CONFIG, ConfigValueFactory.fromAnyRef("key"))
        .withValue(PivotDeriver.PIVOT_VALUE_FIELD_NAME_CONFIG, ConfigValueFactory.fromAnyRef("value"));

    PivotDeriver d = new PivotDeriver();
    assertNoValidationFailures(d, config);
    d.configure(config);
    
    List<Row> results = d.derive(dependencies).collectAsList();
    
    assertEquals(results.size(), 3);
    assertTrue(results.contains(RowFactory.create("A", "AA", "AAA", "1", "2")));
    assertTrue(results.contains(RowFactory.create("B", "BB", "BBB", "3", null)));
    assertTrue(results.contains(RowFactory.create("C", "CC", "CCC", null, "4")));
  }
  
  @Test
  public void testSpecifiedDynamicPivot() throws Exception {
    List<Row> sourceList = Lists.newArrayList(
        RowFactory.create("A", "hello", "1"),
        RowFactory.create("A", "world", "2"),
        RowFactory.create("B", "hello", "3"),
        RowFactory.create("C", "world", "4"));
    StructType schema = RowUtils.structTypeFor(
        Lists.newArrayList("entity_id", "key", "value"),
        Lists.newArrayList("string", "string", "string"));
    Dataset<Row> source = Contexts.getSparkSession().createDataFrame(sourceList, schema);

    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("source", source);
    
    Config config = ConfigFactory.empty()
        .withValue(PivotDeriver.STEP_NAME_CONFIG, ConfigValueFactory.fromAnyRef("source"))
        .withValue(PivotDeriver.ENTITY_KEY_FIELD_NAMES_CONFIG, ConfigValueFactory.fromAnyRef(Lists.newArrayList("entity_id")))
        .withValue(PivotDeriver.PIVOT_KEY_FIELD_NAME_CONFIG, ConfigValueFactory.fromAnyRef("key"))
        .withValue(PivotDeriver.PIVOT_VALUE_FIELD_NAME_CONFIG, ConfigValueFactory.fromAnyRef("value"))
        .withValue(PivotDeriver.PIVOT_KEYS_SOURCE_CONFIG, ConfigValueFactory.fromAnyRef(PivotDeriver.PIVOT_KEYS_SOURCE_DYNAMIC));

    PivotDeriver d = new PivotDeriver();
    assertNoValidationFailures(d, config);
    d.configure(config);
    
    List<Row> results = d.derive(dependencies).collectAsList();
    
    assertEquals(results.size(), 3);
    assertTrue(results.contains(RowFactory.create("A", "1", "2")));
    assertTrue(results.contains(RowFactory.create("B", "3", null)));
    assertTrue(results.contains(RowFactory.create("C", null, "4")));
  }
  
  @Test
  public void testStaticPivot() throws Exception {
    List<Row> sourceList = Lists.newArrayList(
        RowFactory.create("A", "hello", "1"),
        RowFactory.create("A", "world", "2"),
        RowFactory.create("B", "hello", "3"),
        RowFactory.create("C", "world", "4"),
        RowFactory.create("D", "dummy", "5"));
    StructType schema = RowUtils.structTypeFor(
        Lists.newArrayList("entity_id", "key", "value"),
        Lists.newArrayList("string", "string", "string"));
    Dataset<Row> source = Contexts.getSparkSession().createDataFrame(sourceList, schema);

    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("source", source);
    
    Config config = ConfigFactory.empty()
        .withValue(PivotDeriver.STEP_NAME_CONFIG, ConfigValueFactory.fromAnyRef("source"))
        .withValue(PivotDeriver.ENTITY_KEY_FIELD_NAMES_CONFIG, ConfigValueFactory.fromAnyRef(Lists.newArrayList("entity_id")))
        .withValue(PivotDeriver.PIVOT_KEY_FIELD_NAME_CONFIG, ConfigValueFactory.fromAnyRef("key"))
        .withValue(PivotDeriver.PIVOT_VALUE_FIELD_NAME_CONFIG, ConfigValueFactory.fromAnyRef("value"))
        .withValue(PivotDeriver.PIVOT_KEYS_SOURCE_CONFIG, ConfigValueFactory.fromAnyRef(PivotDeriver.PIVOT_KEYS_SOURCE_STATIC))
        .withValue(PivotDeriver.PIVOT_KEYS_LIST_CONFIG, ConfigValueFactory.fromAnyRef(Lists.newArrayList("hello", "world")));

    PivotDeriver d = new PivotDeriver();
    assertNoValidationFailures(d, config);
    d.configure(config);
    
    List<Row> results = d.derive(dependencies).collectAsList();
    
    assertEquals(results.size(), 4);
    assertTrue(results.contains(RowFactory.create("A", "1", "2")));
    assertTrue(results.contains(RowFactory.create("B", "3", null)));
    assertTrue(results.contains(RowFactory.create("C", null, "4")));
    assertTrue(results.contains(RowFactory.create("D", null, null)));
  }
  
}
