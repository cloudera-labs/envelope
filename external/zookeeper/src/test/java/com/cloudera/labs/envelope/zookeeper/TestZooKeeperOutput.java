/**
 * Copyright © 2016-2017 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.zookeeper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.curator.test.TestingServer;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.labs.envelope.output.RandomOutput;
import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.plan.PlannedRow;
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.cloudera.labs.envelope.zookeeper.ZooKeeperOutput;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestZooKeeperOutput implements Watcher {

  private static TestingServer zk;
  private static Config config;
  private static List<String> fieldNames = Lists.newArrayList("field1", "field2", "field3", "field4", "field5", "field6");
  private static List<String> fieldTypes = Lists.newArrayList("string", "int", "long", "boolean", "float", "double");
  private static List<String> keyFieldNames = Lists.newArrayList("field1", "field2", "field3");
  private static StructType schema = RowUtils.structTypeFor(fieldNames, fieldTypes);
  private static StructType keySchema = RowUtils.subsetSchema(schema, keyFieldNames);
  
  @BeforeClass
  public static void setup() throws Exception {
    zk = new TestingServer(2181, true);
    
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(ZooKeeperOutput.CONNECTION_CONFIG, "localhost:2181");
    configMap.put(ZooKeeperOutput.FIELD_NAMES_CONFIG, fieldNames);
    configMap.put(ZooKeeperOutput.FIELD_TYPES_CONFIG, fieldTypes);
    configMap.put(ZooKeeperOutput.KEY_FIELD_NAMES_CONFIG, keyFieldNames);
    config = ConfigFactory.parseMap(configMap);
  }
  
  @Test
  public void testUpserts() throws Exception {
    truncate();
    
    RandomOutput zkOutput = new ZooKeeperOutput();
    zkOutput.configure(config);
    
    Row row1 = new RowWithSchema(schema, "hello", 100, 1000L, true, 1.0f, -1.0);
    Row row2 = new RowWithSchema(schema, "world", -100, -1000L, false, -1.0f, 1.0);
    List<PlannedRow> plan = Lists.newArrayList(
        new PlannedRow(row1, MutationType.UPSERT), new PlannedRow(row2, MutationType.UPSERT));
    
    zkOutput.applyRandomMutations(plan);

    Row filter1 = new RowWithSchema(keySchema, "hello", 100, 1000L);
    Row filter2 = new RowWithSchema(keySchema, "world", -100, -1000L);
    List<Row> filters = Lists.newArrayList(filter1, filter2);
    List<Row> rows = Lists.newArrayList(zkOutput.getExistingForFilters(filters));
    
    assertEquals(rows.size(), 2);
    assertTrue(rows.contains(row1));
    assertTrue(rows.contains(row2));
  }
  
  @Test
  public void testDeletes() throws Exception {
    truncate();
    
    RandomOutput zkOutput = new ZooKeeperOutput();
    zkOutput.configure(config);
    
    Row row1 = new RowWithSchema(schema, "hello", 100, 1000L, true, 1.0f, -1.0);
    Row row2 = new RowWithSchema(schema, "world", -100, -1000L, false, -1.0f, 1.0);
    List<PlannedRow> upsertPlan = Lists.newArrayList(
        new PlannedRow(row1, MutationType.UPSERT), new PlannedRow(row2, MutationType.UPSERT));
    zkOutput.applyRandomMutations(upsertPlan);
    
    Row delete = new RowWithSchema(keySchema, "hello", 100, 1000L);
    List<PlannedRow> deletePlan = Lists.newArrayList(new PlannedRow(delete, MutationType.DELETE));
    zkOutput.applyRandomMutations(deletePlan);

    Row filter1 = new RowWithSchema(keySchema, "hello", 100, 1000L);
    Row filter2 = new RowWithSchema(keySchema, "world", -100, -1000L);
    List<Row> filters = Lists.newArrayList(filter1, filter2);
    List<Row> rows = Lists.newArrayList(zkOutput.getExistingForFilters(filters));
    
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0), row2);
  }
  
  @Test
  public void getByFullKey() throws Exception {
    truncate();
    
    RandomOutput zkOutput = new ZooKeeperOutput();
    zkOutput.configure(config);
    
    Row row1 = new RowWithSchema(schema, "hello", 100, 1000L, true, 1.0f, -1.0);
    Row row2 = new RowWithSchema(schema, "world", -100, -1000L, false, -1.0f, 1.0);
    List<PlannedRow> upsertPlan = Lists.newArrayList(
        new PlannedRow(row1, MutationType.UPSERT), new PlannedRow(row2, MutationType.UPSERT));
    zkOutput.applyRandomMutations(upsertPlan);
    
    Row filter = new RowWithSchema(keySchema, "hello", 100, 1000L);
    List<Row> filters = Lists.newArrayList(filter);
    List<Row> rows = Lists.newArrayList(zkOutput.getExistingForFilters(filters));
    
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0), row1);
  }
  
  @Test
  public void getByPartialKey() throws Exception {
    truncate();
    
    RandomOutput zkOutput = new ZooKeeperOutput();
    zkOutput.configure(config);
    
    Row row1 = new RowWithSchema(schema, "hello", 100, 1000L, true, 1.0f, -1.0);
    Row row2 = new RowWithSchema(schema, "hello", -100, -1000L, false, -1.0f, 1.0);
    List<PlannedRow> upsertPlan = Lists.newArrayList(
        new PlannedRow(row1, MutationType.UPSERT), new PlannedRow(row2, MutationType.UPSERT));
    zkOutput.applyRandomMutations(upsertPlan);
    
    StructType partialSchema = RowUtils.structTypeFor(Lists.newArrayList("field1"), Lists.newArrayList("string"));
    Row filter1 = new RowWithSchema(partialSchema, "hello");
    Row filter2 = new RowWithSchema(partialSchema, "world");
    List<Row> filters = Lists.newArrayList(filter1, filter2);
    List<Row> rows = Lists.newArrayList(zkOutput.getExistingForFilters(filters));
    
    assertEquals(rows.size(), 2);
    assertTrue(rows.contains(row1));
    assertTrue(rows.contains(row2));
  }
  
  @Test
  public void getByValues() throws Exception {
    truncate();
    
    RandomOutput zkOutput = new ZooKeeperOutput();
    zkOutput.configure(config);
    
    Row row1 = new RowWithSchema(schema, "hello", 100, 1000L, true, 1.0f, -1.0);
    Row row2 = new RowWithSchema(schema, "world", -100, -1000L, false, -1.0f, 1.0);
    List<PlannedRow> upsertPlan = Lists.newArrayList(
        new PlannedRow(row1, MutationType.UPSERT), new PlannedRow(row2, MutationType.UPSERT));
    zkOutput.applyRandomMutations(upsertPlan);
    
    StructType partialSchema = RowUtils.structTypeFor(Lists.newArrayList("field6"), Lists.newArrayList("double"));
    Row filter = new RowWithSchema(partialSchema, 1.0);
    List<Row> filters = Lists.newArrayList(filter);
    List<Row> rows = Lists.newArrayList(zkOutput.getExistingForFilters(filters));
    
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0), row2);
  }
  
  @Test
  public void getByFullKeyAndValues() throws Exception {
    truncate();
    
    RandomOutput zkOutput = new ZooKeeperOutput();
    zkOutput.configure(config);
    
    Row row1 = new RowWithSchema(schema, "hello", 100, 1000L, true, 1.0f, -1.0);
    Row row2 = new RowWithSchema(schema, "world", -100, -1000L, false, -1.0f, 1.0);
    List<PlannedRow> upsertPlan = Lists.newArrayList(
        new PlannedRow(row1, MutationType.UPSERT), new PlannedRow(row2, MutationType.UPSERT));
    zkOutput.applyRandomMutations(upsertPlan);
    
    StructType keySchemaWithValues = keySchema
        .add(DataTypes.createStructField("field4", DataTypes.BooleanType, false))
        .add(DataTypes.createStructField("field6", DataTypes.DoubleType, false));
    Row filter1 = new RowWithSchema(keySchemaWithValues, "hello", 100, 1000L, true, -1.0);
    Row filter2 = new RowWithSchema(keySchemaWithValues, "world", -100, -1000L, true, -1.0);
    List<Row> filters = Lists.newArrayList(filter1, filter2);
    List<Row> rows = Lists.newArrayList(zkOutput.getExistingForFilters(filters));
    
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0), row1);
  }
  
  @Test
  public void getByPartialKeyAndValues() throws Exception {
    truncate();
    
    RandomOutput zkOutput = new ZooKeeperOutput();
    zkOutput.configure(config);
    
    Row row1 = new RowWithSchema(schema, "hello", 100, 1000L, true, 1.0f, -1.0);
    Row row2 = new RowWithSchema(schema, "hello", -100, -1000L, false, -1.0f, 1.0);
    List<PlannedRow> upsertPlan = Lists.newArrayList(
        new PlannedRow(row1, MutationType.UPSERT), new PlannedRow(row2, MutationType.UPSERT));
    zkOutput.applyRandomMutations(upsertPlan);
    
    StructType partialSchemaWithValues = 
        RowUtils.structTypeFor(Lists.newArrayList("field1"), Lists.newArrayList("string"))
        .add(DataTypes.createStructField("field4", DataTypes.BooleanType, false))
        .add(DataTypes.createStructField("field6", DataTypes.DoubleType, false));
    Row filter1 = new RowWithSchema(partialSchemaWithValues, "hello", false, 1.0);
    Row filter2 = new RowWithSchema(partialSchemaWithValues, "world", false, 1.0);
    List<Row> filters = Lists.newArrayList(filter1, filter2);
    List<Row> rows = Lists.newArrayList(zkOutput.getExistingForFilters(filters));
    
    assertEquals(rows.size(), 1);
    assertTrue(rows.contains(row2));
  }
  
  private void truncate() throws Exception {
    RandomOutput zkOutput = new ZooKeeperOutput();
    zkOutput.configure(config);
    
    Row filter = new RowWithSchema(new StructType(new StructField[0]));
    List<Row> filters = Lists.newArrayList(filter);
    List<Row> existing = Lists.newArrayList(zkOutput.getExistingForFilters(filters));
    
    List<PlannedRow> deletes = Lists.newArrayList();
    for (Row exist : existing) {
      deletes.add(new PlannedRow(exist, MutationType.DELETE));
    }
    zkOutput.applyRandomMutations(deletes);
  }
  
  @AfterClass
  public static void teardown() throws IOException {
    zk.close();
  }

  @Override
  public void process(WatchedEvent event) {}
  
}
