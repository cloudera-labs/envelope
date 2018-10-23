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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Map;
import java.util.Set;

import static com.cloudera.labs.envelope.validate.ValidationAssert.assertNoValidationFailures;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestLatestDeriver {

  @Test
  public void testOneKeyField() {
    Dataset<Row> data = oneKeyFieldDataset();
    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("dep1", data);

    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(LatestDeriver.KEY_FIELD_NAMES_CONFIG, Lists.newArrayList("key"));
    configMap.put(LatestDeriver.TIMESTAMP_FIELD_NAME_CONFIG, "ts");
    Config config = ConfigFactory.parseMap(configMap);

    LatestDeriver d = new LatestDeriver();
    assertNoValidationFailures(d, config);
    d.configure(config);

    Dataset<Row> derived = d.derive(dependencies);

    assertEquals(3, derived.count());
    assertEquals(data.schema().size(), derived.schema().size());

    Set<Row> rows = Sets.newHashSet(derived.collectAsList());
    assertTrue(rows.contains(RowFactory.create("a", "hello", new Timestamp(10000))));
    assertTrue(rows.contains(RowFactory.create("b", "world!", new Timestamp(20000))));
    assertTrue(rows.contains(RowFactory.create("c", "hello?", new Timestamp(10000))));
  }

  @Test
  public void testMultipleHighestTimestampForSameKey() {
    Dataset<Row> data = Contexts.getSparkSession().createDataFrame(Lists.newArrayList(
        RowFactory.create("a", "hello", new Timestamp(10000)),
        RowFactory.create("a", "world", new Timestamp(5000)),
        RowFactory.create("a", "beep", new Timestamp(10000))
    ), DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("key", DataTypes.StringType, false),
        DataTypes.createStructField("value", DataTypes.StringType, false),
        DataTypes.createStructField("ts", DataTypes.TimestampType, false)
    )));

    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("dep1", data);

    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(LatestDeriver.KEY_FIELD_NAMES_CONFIG, Lists.newArrayList("key"));
    configMap.put(LatestDeriver.TIMESTAMP_FIELD_NAME_CONFIG, "ts");
    Config config = ConfigFactory.parseMap(configMap);

    LatestDeriver d = new LatestDeriver();
    assertNoValidationFailures(d, config);
    d.configure(config);

    Dataset<Row> derived = d.derive(dependencies);

    assertEquals(1, derived.count());
    assertEquals(data.schema().size(), derived.schema().size());

    // Don't assert which record it is because the behavior is not defined for selecting when there
    // are multiple record of the same key with the same highest timestamp.
  }

  @Test
  public void testMultipleKeyFields() {
    Dataset<Row> data = Contexts.getSparkSession().createDataFrame(Lists.newArrayList(
        RowFactory.create("a1", "a1", "hello", new Timestamp(10000)),
        RowFactory.create("a1", "a1", "world", new Timestamp(5000)),
        RowFactory.create("b1", "b1", "hello!", new Timestamp(10000)),
        RowFactory.create("b1", "b1", "world!", new Timestamp(20000)),
        RowFactory.create("c1", "c1", "hello?", new Timestamp(10000)),
        RowFactory.create("a1", "a2", "hello!!", new Timestamp(10000)),
        RowFactory.create("a1", "a2", "hello??", new Timestamp(30000))
    ), DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("key1", DataTypes.StringType, false),
        DataTypes.createStructField("key2", DataTypes.StringType, false),
        DataTypes.createStructField("value", DataTypes.StringType, false),
        DataTypes.createStructField("ts", DataTypes.TimestampType, false)
    )));
    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("dep1", data);

    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(LatestDeriver.KEY_FIELD_NAMES_CONFIG, Lists.newArrayList("key1", "key2"));
    configMap.put(LatestDeriver.TIMESTAMP_FIELD_NAME_CONFIG, "ts");
    Config config = ConfigFactory.parseMap(configMap);

    LatestDeriver d = new LatestDeriver();
    assertNoValidationFailures(d, config);
    d.configure(config);

    Dataset<Row> derived = d.derive(dependencies);

    assertEquals(4, derived.count());
    assertEquals(data.schema().size(), derived.schema().size());

    Set<Row> rows = Sets.newHashSet(derived.collectAsList());
    assertTrue(rows.contains(RowFactory.create("a1", "a1", "hello", new Timestamp(10000))));
    assertTrue(rows.contains(RowFactory.create("a1", "a2", "hello??", new Timestamp(30000))));
    assertTrue(rows.contains(RowFactory.create("b1", "b1", "world!", new Timestamp(20000))));
    assertTrue(rows.contains(RowFactory.create("c1", "c1", "hello?", new Timestamp(10000))));
  }

  @Test (expected = RuntimeException.class)
  public void testMissingDependency() {
    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("dep1", oneKeyFieldDataset());

    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(LatestDeriver.STEP_NAME_CONFIG, "dep2");
    configMap.put(LatestDeriver.KEY_FIELD_NAMES_CONFIG, Lists.newArrayList("key"));
    configMap.put(LatestDeriver.TIMESTAMP_FIELD_NAME_CONFIG, "ts");
    Config config = ConfigFactory.parseMap(configMap);

    LatestDeriver d = new LatestDeriver();
    assertNoValidationFailures(d, config);
    d.configure(config);

    d.derive(dependencies);
  }

  @Test
  public void testSpecifiedDependency() {
    Dataset<Row> data = oneKeyFieldDataset();
    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("dep1", data);
    dependencies.put("dep2", data);

    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(LatestDeriver.STEP_NAME_CONFIG, "dep1");
    configMap.put(LatestDeriver.KEY_FIELD_NAMES_CONFIG, Lists.newArrayList("key"));
    configMap.put(LatestDeriver.TIMESTAMP_FIELD_NAME_CONFIG, "ts");
    Config config = ConfigFactory.parseMap(configMap);

    LatestDeriver d = new LatestDeriver();
    assertNoValidationFailures(d, config);
    d.configure(config);

    Dataset<Row> derived = d.derive(dependencies);

    assertEquals(3, derived.count());
    assertEquals(data.schema().size(), derived.schema().size());

    Set<Row> rows = Sets.newHashSet(derived.collectAsList());
    assertTrue(rows.contains(RowFactory.create("a", "hello", new Timestamp(10000))));
    assertTrue(rows.contains(RowFactory.create("b", "world!", new Timestamp(20000))));
    assertTrue(rows.contains(RowFactory.create("c", "hello?", new Timestamp(10000))));
  }

  @Test (expected = RuntimeException.class)
  public void testCantUseDefaultDependency() {
    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("dep1", oneKeyFieldDataset());
    dependencies.put("dep2", oneKeyFieldDataset());

    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(LatestDeriver.KEY_FIELD_NAMES_CONFIG, Lists.newArrayList("key"));
    configMap.put(LatestDeriver.TIMESTAMP_FIELD_NAME_CONFIG, "ts");
    Config config = ConfigFactory.parseMap(configMap);

    LatestDeriver d = new LatestDeriver();
    assertNoValidationFailures(d, config);
    d.configure(config);

    d.derive(dependencies);
  }

  private Dataset<Row> oneKeyFieldDataset() {
    return Contexts.getSparkSession().createDataFrame(Lists.newArrayList(
        RowFactory.create("a", "hello", new Timestamp(10000)),
        RowFactory.create("a", "world", new Timestamp(5000)),
        RowFactory.create("b", "hello!", new Timestamp(10000)),
        RowFactory.create("b", "world!", new Timestamp(20000)),
        RowFactory.create("c", "hello?", new Timestamp(10000))
    ), DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("key", DataTypes.StringType, false),
        DataTypes.createStructField("value", DataTypes.StringType, false),
        DataTypes.createStructField("ts", DataTypes.TimestampType, false)
    )));
  }

}
