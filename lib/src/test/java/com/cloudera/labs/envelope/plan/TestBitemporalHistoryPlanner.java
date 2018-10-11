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

package com.cloudera.labs.envelope.plan;

import com.cloudera.labs.envelope.plan.time.TimeModelFactory;
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.PlannerUtils;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.cloudera.labs.envelope.plan.BitemporalHistoryPlanner.CURRENT_FLAG_DEFAULT_NO;
import static com.cloudera.labs.envelope.plan.BitemporalHistoryPlanner.CURRENT_FLAG_DEFAULT_YES;
import static com.cloudera.labs.envelope.plan.BitemporalHistoryPlanner.CURRENT_FLAG_FIELD_NAME_CONFIG_NAME;
import static com.cloudera.labs.envelope.plan.BitemporalHistoryPlanner.EVENT_TIME_EFFECTIVE_FROM_FIELD_NAMES_CONFIG_NAME;
import static com.cloudera.labs.envelope.plan.BitemporalHistoryPlanner.EVENT_TIME_EFFECTIVE_TO_FIELD_NAMES_CONFIG_NAME;
import static com.cloudera.labs.envelope.plan.BitemporalHistoryPlanner.KEY_FIELD_NAMES_CONFIG_NAME;
import static com.cloudera.labs.envelope.plan.BitemporalHistoryPlanner.SYSTEM_TIME_EFFECTIVE_FROM_FIELD_NAMES_CONFIG_NAME;
import static com.cloudera.labs.envelope.plan.BitemporalHistoryPlanner.SYSTEM_TIME_EFFECTIVE_TO_FIELD_NAMES_CONFIG_NAME;
import static com.cloudera.labs.envelope.plan.BitemporalHistoryPlanner.TIMESTAMP_FIELD_NAMES_CONFIG_NAME;
import static com.cloudera.labs.envelope.plan.BitemporalHistoryPlanner.VALUE_FIELD_NAMES_CONFIG_NAME;
import static com.cloudera.labs.envelope.validate.ValidationAssert.assertNoValidationFailures;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestBitemporalHistoryPlanner {

  private List<Row> arriving;
  private List<Row> existing;
  private StructType keySchema;
  private StructType arrivingSchema;
  private StructType existingSchema;
  private StructType existingSchemaWithoutCurrentFlag;
  private Config config;
  private Config configWithoutCurrentFlag;
  private BitemporalHistoryPlanner p;
  private long preplanSystemTime;

  @Before
  public void before() { 
    arriving = Lists.newArrayList();
    existing = Lists.newArrayList();

    keySchema = DataTypes.createStructType(Lists.newArrayList(
      DataTypes.createStructField("key", DataTypes.StringType, false)));
    arrivingSchema = DataTypes.createStructType(Lists.newArrayList(
      DataTypes.createStructField("key", DataTypes.StringType, false),
      DataTypes.createStructField("value", DataTypes.StringType, true),
      DataTypes.createStructField("timestamp", DataTypes.LongType, false)));
    existingSchema = DataTypes.createStructType(Lists.newArrayList(
      DataTypes.createStructField("key", DataTypes.StringType, false),
      DataTypes.createStructField("value", DataTypes.StringType, false),
      DataTypes.createStructField("timestamp", DataTypes.LongType, false),
      DataTypes.createStructField("eventstart", DataTypes.LongType, false),
      DataTypes.createStructField("eventend", DataTypes.LongType, false),
      DataTypes.createStructField("systemstart", DataTypes.LongType, false),
      DataTypes.createStructField("systemend", DataTypes.LongType, false),
      DataTypes.createStructField("currentflag", DataTypes.StringType, false)));
    existingSchemaWithoutCurrentFlag = DataTypes.createStructType(Lists.newArrayList(
      DataTypes.createStructField("key", DataTypes.StringType, false),
      DataTypes.createStructField("value", DataTypes.StringType, false),
      DataTypes.createStructField("timestamp", DataTypes.LongType, false),
      DataTypes.createStructField("eventstart", DataTypes.LongType, false),
      DataTypes.createStructField("eventend", DataTypes.LongType, false),
      DataTypes.createStructField("systemstart", DataTypes.LongType, false),
      DataTypes.createStructField("systemend", DataTypes.LongType, false)));

    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(KEY_FIELD_NAMES_CONFIG_NAME, Lists.newArrayList("key"));
    configMap.put(VALUE_FIELD_NAMES_CONFIG_NAME, Lists.newArrayList("value"));
    configMap.put(TIMESTAMP_FIELD_NAMES_CONFIG_NAME, Lists.newArrayList("timestamp"));
    configMap.put(EVENT_TIME_EFFECTIVE_FROM_FIELD_NAMES_CONFIG_NAME, Lists.newArrayList("eventstart"));
    configMap.put(EVENT_TIME_EFFECTIVE_TO_FIELD_NAMES_CONFIG_NAME, Lists.newArrayList("eventend"));
    configMap.put(SYSTEM_TIME_EFFECTIVE_FROM_FIELD_NAMES_CONFIG_NAME, Lists.newArrayList("systemstart"));
    configMap.put(SYSTEM_TIME_EFFECTIVE_TO_FIELD_NAMES_CONFIG_NAME, Lists.newArrayList("systemend"));
    configMap.put(CURRENT_FLAG_FIELD_NAME_CONFIG_NAME, "currentflag");
    config = ConfigFactory.parseMap(configMap);

    configMap.remove(CURRENT_FLAG_FIELD_NAME_CONFIG_NAME);
    configWithoutCurrentFlag = ConfigFactory.parseMap(configMap);

    preplanSystemTime = System.currentTimeMillis();
  }

  @Test
  public void testOneArrivingNoneExisting() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello", 100L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 1);

    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(0), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 253402214400000L);
    assertTrue((Long)RowUtils.get(planned.get(0), "systemstart") >= preplanSystemTime);
    assertTrue((Long)RowUtils.get(planned.get(0), "systemstart") < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(0), "currentflag"), CURRENT_FLAG_DEFAULT_YES);
  }

  @Test
  public void testOneArrivingNoneExistingNoCurrentFlag() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, configWithoutCurrentFlag);
    p.configure(configWithoutCurrentFlag);

    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello", 100L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 1);

    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(0), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 253402214400000L);
    assertTrue((Long)RowUtils.get(planned.get(0), "systemstart") >= preplanSystemTime);
    assertTrue((Long)RowUtils.get(planned.get(0), "systemstart") < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), 253402214400000L);
  }

  @Test
  public void testMultipleArrivingNoneExisting() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello", 100L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 200L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 2);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);

    Long systemStart0 = (Long)RowUtils.get(planned.get(0), "systemstart");
    Long systemStart1 = (Long)RowUtils.get(planned.get(1), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 199L);
    assertTrue(systemStart0 >= preplanSystemTime);
    assertTrue(systemStart0 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(0), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(1), "value"), "world");
    assertEquals(RowUtils.get(planned.get(1), "eventstart"), 200L);
    assertEquals(RowUtils.get(planned.get(1), "eventend"), 253402214400000L);
    assertTrue(systemStart1 >= preplanSystemTime);
    assertTrue(systemStart1 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(1), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(1), "currentflag"), CURRENT_FLAG_DEFAULT_YES);
  }

  @Test
  public void testMultipleArrivingNoneExistingNoCurrentFlag() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, configWithoutCurrentFlag);
    p.configure(configWithoutCurrentFlag);

    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello", 100L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 200L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 2);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);

    Long systemStart0 = (Long)RowUtils.get(planned.get(0), "systemstart");
    Long systemStart1 = (Long)RowUtils.get(planned.get(1), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 199L);
    assertTrue(systemStart0 >= preplanSystemTime);
    assertTrue(systemStart0 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), 253402214400000L);

    assertEquals(RowUtils.get(planned.get(1), "value"), "world");
    assertEquals(RowUtils.get(planned.get(1), "eventstart"), 200L);
    assertEquals(RowUtils.get(planned.get(1), "eventend"), 253402214400000L);
    assertTrue(systemStart1 >= preplanSystemTime);
    assertTrue(systemStart1 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(1), "systemend"), 253402214400000L);
  }

  @Test
  public void testOneArrivingOneExistingWhereArrivingLaterThanExisting() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, 1L, 253402214400000L, CURRENT_FLAG_DEFAULT_YES));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 200L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 3);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(2)), MutationType.INSERT);

    Long systemStart1 = (Long)RowUtils.get(planned.get(1), "systemstart");
    Long systemStart2 = (Long)RowUtils.get(planned.get(2), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(0), "systemstart"), 1L);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), systemStart1 - 1);
    assertEquals(RowUtils.get(planned.get(0), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(1), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(1), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(1), "eventend"), 199L);
    assertTrue(systemStart1 >= preplanSystemTime);
    assertTrue(systemStart1 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(1), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(1), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(2), "value"), "world");
    assertEquals(RowUtils.get(planned.get(2), "eventstart"), 200L);
    assertEquals(RowUtils.get(planned.get(2), "eventend"), 253402214400000L);
    assertTrue(systemStart2 >= preplanSystemTime);
    assertTrue(systemStart2 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(2), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(2), "currentflag"), CURRENT_FLAG_DEFAULT_YES);
  }
  
  @Test
  public void testOneArrivingOneExistingWhereArrivingLaterThanExistingButSameValues() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, 1L, 253402214400000L, CURRENT_FLAG_DEFAULT_YES));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello", 200L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 0);
  }

  @Test
  public void testOneArrivingOneExistingWhereArrivingLaterThanExistingNoCurrentFlag() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, configWithoutCurrentFlag);
    p.configure(configWithoutCurrentFlag);

    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello", 100L, 100L, 253402214400000L, 1L, 253402214400000L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 200L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 3);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(2)), MutationType.INSERT);

    Long systemStart1 = (Long)RowUtils.get(planned.get(1), "systemstart");
    Long systemStart2 = (Long)RowUtils.get(planned.get(2), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(0), "systemstart"), 1L);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), systemStart1 - 1);

    assertEquals(RowUtils.get(planned.get(1), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(1), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(1), "eventend"), 199L);
    assertTrue(systemStart1 >= preplanSystemTime);
    assertTrue(systemStart1 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(1), "systemend"), 253402214400000L);

    assertEquals(RowUtils.get(planned.get(2), "value"), "world");
    assertEquals(RowUtils.get(planned.get(2), "eventstart"), 200L);
    assertEquals(RowUtils.get(planned.get(2), "eventend"), 253402214400000L);
    assertTrue(systemStart2 >= preplanSystemTime);
    assertTrue(systemStart2 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(2), "systemend"), 253402214400000L);
  }

  @Test
  public void testOneArrivingOneExistingWhereArrivingSameTimeAsExistingWithSameValues() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, 1L, 253402214400000L, CURRENT_FLAG_DEFAULT_YES));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello", 100L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 0);
  }

  @Test
  public void testOneArrivingOneExistingWhereArrivingSameTimeAsExistingWithSameValuesNoCurrentFlag() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello", 100L, 100L, 253402214400000L, 1L, 253402214400000L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello", 100L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 0);
  }

  @Test
  public void testOneArrivingOneExistingWhereArrivingSameTimeAsExistingWithDifferentValues() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, 1L, 253402214400000L, CURRENT_FLAG_DEFAULT_YES));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 100L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 2);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);

    Long systemStart1 = (Long)RowUtils.get(planned.get(1), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(0), "systemstart"), 1L);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), systemStart1 - 1);
    assertEquals(RowUtils.get(planned.get(0), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(1), "value"), "world");
    assertEquals(RowUtils.get(planned.get(1), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(1), "eventend"), 253402214400000L);
    assertTrue(systemStart1 >= preplanSystemTime);
    assertTrue(systemStart1 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(1), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(1), "currentflag"), CURRENT_FLAG_DEFAULT_YES);
  }

  @Test
  public void testOneArrivingOneExistingWhereArrivingSameTimeAsExistingWithDifferentValuesNoCurrentFlag() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, configWithoutCurrentFlag);
    p.configure(configWithoutCurrentFlag);

    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello", 100L, 100L, 253402214400000L, 1L, 253402214400000L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 100L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 2);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);

    Long systemStart1 = (Long)RowUtils.get(planned.get(1), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(0), "systemstart"), 1L);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), systemStart1 - 1);

    assertEquals(RowUtils.get(planned.get(1), "value"), "world");
    assertEquals(RowUtils.get(planned.get(1), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(1), "eventend"), 253402214400000L);
    assertTrue(systemStart1 >= preplanSystemTime);
    assertTrue(systemStart1 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(1), "systemend"), 253402214400000L);
  }

  @Test
  public void testOneArrivingOneExistingWhereArrivingEarlierThanExisting() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, 1L, 253402214400000L, CURRENT_FLAG_DEFAULT_YES));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 50L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 1);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.INSERT);

    Long systemStart0 = (Long)RowUtils.get(planned.get(0), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value"), "world");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 50L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 99L);
    assertTrue(systemStart0 >= preplanSystemTime);
    assertTrue(systemStart0 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(0), "currentflag"), CURRENT_FLAG_DEFAULT_NO);
  }
  
  @Test
  public void testTwoArrivingOneExistingWhereArrivingEarlierThanExisting() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, 1L, 253402214400000L, CURRENT_FLAG_DEFAULT_YES));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 50L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world!", 75L));

    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 2);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.INSERT);

    Long systemStart0 = (Long)RowUtils.get(planned.get(0), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value"), "world");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 50L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 74L);
    assertTrue(systemStart0 >= preplanSystemTime);
    assertTrue(systemStart0 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(0), "currentflag"), CURRENT_FLAG_DEFAULT_NO);
    
    assertEquals(RowUtils.get(planned.get(1), "value"), "world!");
    assertEquals(RowUtils.get(planned.get(1), "eventstart"), 75L);
    assertEquals(RowUtils.get(planned.get(1), "eventend"), 99L);
    assertTrue(systemStart0 >= preplanSystemTime);
    assertTrue(systemStart0 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(1), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(1), "currentflag"), CURRENT_FLAG_DEFAULT_NO);
  }

  @Test
  public void testOneArrivingOneExistingWhereArrivingEarlierThanExistingNoCurrentFlag() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, configWithoutCurrentFlag);
    p.configure(configWithoutCurrentFlag);

    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello", 100L, 100L, 253402214400000L, 1L, 253402214400000L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 50L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 1);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.INSERT);

    Long systemStart0 = (Long)RowUtils.get(planned.get(0), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value"), "world");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 50L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 99L);
    assertTrue(systemStart0 >= preplanSystemTime);
    assertTrue(systemStart0 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), 253402214400000L);
  }

  @Test
  public void testOneArrivingMultipleExistingWhereArrivingLaterThanAllExisting() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, 1L, 2L, CURRENT_FLAG_DEFAULT_NO));
    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 199L, 3L, 253402214400000L, CURRENT_FLAG_DEFAULT_NO));
    existing.add(new RowWithSchema(existingSchema, "a", "hello!", 200L, 200L, 253402214400000L, 3L, 4L, CURRENT_FLAG_DEFAULT_NO));
    existing.add(new RowWithSchema(existingSchema, "a", "hello!", 200L, 200L, 299L, 5L, 253402214400000L, CURRENT_FLAG_DEFAULT_NO));
    existing.add(new RowWithSchema(existingSchema, "a", "hello?", 300L, 300L, 253402214400000L, 5L, 253402214400000L, CURRENT_FLAG_DEFAULT_YES));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 400L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 3);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(2)), MutationType.INSERT);

    Long systemStart1 = (Long)RowUtils.get(planned.get(1), "systemstart");
    Long systemStart2 = (Long)RowUtils.get(planned.get(2), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value"), "hello?");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 300L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(0), "systemstart"), 5L);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), systemStart1 - 1);
    assertEquals(RowUtils.get(planned.get(0), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(1), "value"), "hello?");
    assertEquals(RowUtils.get(planned.get(1), "eventstart"), 300L);
    assertEquals(RowUtils.get(planned.get(1), "eventend"), 399L);
    assertTrue(systemStart1 >= preplanSystemTime);
    assertTrue(systemStart1 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(1), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(1), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(2), "value"), "world");
    assertEquals(RowUtils.get(planned.get(2), "eventstart"), 400L);
    assertEquals(RowUtils.get(planned.get(2), "eventend"), 253402214400000L);
    assertTrue(systemStart2 >= preplanSystemTime);
    assertTrue(systemStart2 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(2), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(2), "currentflag"), CURRENT_FLAG_DEFAULT_YES);
  }

  @Test
  public void testOneArrivingMultipleExistingWhereArrivingLaterThanAllExistingNoCurrentFlag() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, configWithoutCurrentFlag);
    p.configure(configWithoutCurrentFlag);

    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello", 100L, 100L, 253402214400000L, 1L, 2L));
    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello", 100L, 100L, 199L, 3L, 253402214400000L));
    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello!", 200L, 200L, 253402214400000L, 3L, 4L));
    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello!", 200L, 200L, 299L, 5L, 253402214400000L));
    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello?", 300L, 300L, 253402214400000L, 5L, 253402214400000L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 400L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 3);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(2)), MutationType.INSERT);

    Long systemStart1 = (Long)RowUtils.get(planned.get(1), "systemstart");
    Long systemStart2 = (Long)RowUtils.get(planned.get(2), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value"), "hello?");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 300L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(0), "systemstart"), 5L);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), systemStart1 - 1);

    assertEquals(RowUtils.get(planned.get(1), "value"), "hello?");
    assertEquals(RowUtils.get(planned.get(1), "eventstart"), 300L);
    assertEquals(RowUtils.get(planned.get(1), "eventend"), 399L);
    assertTrue(systemStart1 >= preplanSystemTime);
    assertTrue(systemStart1 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(1), "systemend"), 253402214400000L);

    assertEquals(RowUtils.get(planned.get(2), "value"), "world");
    assertEquals(RowUtils.get(planned.get(2), "eventstart"), 400L);
    assertEquals(RowUtils.get(planned.get(2), "eventend"), 253402214400000L);
    assertTrue(systemStart2 >= preplanSystemTime);
    assertTrue(systemStart2 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(2), "systemend"), 253402214400000L);
  }

  @Test
  public void testOneArrivingMultipleExistingWhereArrivingSameTimeAsLatestExistingWithSameValues() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, 1L, 2L, CURRENT_FLAG_DEFAULT_NO));
    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 199L, 3L, 253402214400000L, CURRENT_FLAG_DEFAULT_NO));
    existing.add(new RowWithSchema(existingSchema, "a", "hello!", 200L, 200L, 253402214400000L, 3L, 4L, CURRENT_FLAG_DEFAULT_NO));
    existing.add(new RowWithSchema(existingSchema, "a", "hello!", 200L, 200L, 299L, 5L, 253402214400000L, CURRENT_FLAG_DEFAULT_NO));
    existing.add(new RowWithSchema(existingSchema, "a", "hello?", 300L, 300L, 253402214400000L, 5L, 253402214400000L, CURRENT_FLAG_DEFAULT_YES));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello?", 300L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 0);
  }

  @Test
  public void testOneArrivingMultipleExistingWhereArrivingSameTimeAsLatestExistingWithSameValuesNoCurrentFlag() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, configWithoutCurrentFlag);
    p.configure(configWithoutCurrentFlag);

    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello", 100L, 100L, 253402214400000L, 1L, 2L));
    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello", 100L, 100L, 199L, 3L, 253402214400000L));
    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello!", 200L, 200L, 253402214400000L, 3L, 4L));
    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello!", 200L, 200L, 299L, 5L, 253402214400000L));
    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello?", 300L, 300L, 253402214400000L, 5L, 253402214400000L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello?", 300L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 0);
  }

  @Test
  public void testOneArrivingMultipleExistingWhereArrivingSameTimeAsLatestExistingWithDifferentValues() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, 1L, 2L, CURRENT_FLAG_DEFAULT_NO));
    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 199L, 3L, 253402214400000L, CURRENT_FLAG_DEFAULT_NO));
    existing.add(new RowWithSchema(existingSchema, "a", "hello!", 200L, 200L, 253402214400000L, 3L, 4L, CURRENT_FLAG_DEFAULT_NO));
    existing.add(new RowWithSchema(existingSchema, "a", "hello!", 200L, 200L, 299L, 5L, 253402214400000L, CURRENT_FLAG_DEFAULT_NO));
    existing.add(new RowWithSchema(existingSchema, "a", "hello?", 300L, 300L, 253402214400000L, 5L, 253402214400000L, CURRENT_FLAG_DEFAULT_YES));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 300L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 2);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);

    Long systemStart1 = (Long)RowUtils.get(planned.get(1), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value"), "hello?");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 300L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(0), "systemstart"), 5L);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), systemStart1 - 1);
    assertEquals(RowUtils.get(planned.get(0), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(1), "value"), "world");
    assertEquals(RowUtils.get(planned.get(1), "eventstart"), 300L);
    assertEquals(RowUtils.get(planned.get(1), "eventend"), 253402214400000L);
    assertTrue(systemStart1 >= preplanSystemTime);
    assertTrue(systemStart1 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(1), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(1), "currentflag"), CURRENT_FLAG_DEFAULT_YES);
  }

  @Test
  public void testOneArrivingMultipleExistingWhereArrivingSameTimeAsLatestExistingWithDifferentValuesNoCurrentFlag() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, configWithoutCurrentFlag);
    p.configure(configWithoutCurrentFlag);

    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello", 100L, 100L, 253402214400000L, 1L, 2L));
    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello", 100L, 100L, 199L, 3L, 253402214400000L));
    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello!", 200L, 200L, 253402214400000L, 3L, 4L));
    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello!", 200L, 200L, 299L, 5L, 253402214400000L));
    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello?", 300L, 300L, 253402214400000L, 5L, 253402214400000L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 300L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 2);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);

    Long systemStart1 = (Long)RowUtils.get(planned.get(1), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value"), "hello?");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 300L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(0), "systemstart"), 5L);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), systemStart1 - 1);

    assertEquals(RowUtils.get(planned.get(1), "value"), "world");
    assertEquals(RowUtils.get(planned.get(1), "eventstart"), 300L);
    assertEquals(RowUtils.get(planned.get(1), "eventend"), 253402214400000L);
    assertTrue(systemStart1 >= preplanSystemTime);
    assertTrue(systemStart1 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(1), "systemend"), 253402214400000L);
  }

  @Test
  public void testOneArrivingMultipleExistingWhereArrivingBetweenTwoExisting() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, 1L, 2L, CURRENT_FLAG_DEFAULT_NO));
    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 199L, 3L, 253402214400000L, CURRENT_FLAG_DEFAULT_NO));
    existing.add(new RowWithSchema(existingSchema, "a", "hello!", 200L, 200L, 253402214400000L, 3L, 4L, CURRENT_FLAG_DEFAULT_NO));
    existing.add(new RowWithSchema(existingSchema, "a", "hello!", 200L, 200L, 299L, 5L, 253402214400000L, CURRENT_FLAG_DEFAULT_NO));
    existing.add(new RowWithSchema(existingSchema, "a", "hello?", 300L, 300L, 253402214400000L, 5L, 253402214400000L, CURRENT_FLAG_DEFAULT_YES));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 150L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 3);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(2)), MutationType.INSERT);

    Long systemStart1 = (Long)RowUtils.get(planned.get(1), "systemstart");
    Long systemStart2 = (Long)RowUtils.get(planned.get(2), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 199L);
    assertEquals(RowUtils.get(planned.get(0), "systemstart"), 3L);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), systemStart1 - 1);
    assertEquals(RowUtils.get(planned.get(0), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(1), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(1), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(1), "eventend"), 149L);
    assertTrue(systemStart1 >= preplanSystemTime);
    assertTrue(systemStart1 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(1), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(1), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(2), "value"), "world");
    assertEquals(RowUtils.get(planned.get(2), "eventstart"), 150L);
    assertEquals(RowUtils.get(planned.get(2), "eventend"), 199L);
    assertTrue(systemStart2 >= preplanSystemTime);
    assertTrue(systemStart2 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(2), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(2), "currentflag"), CURRENT_FLAG_DEFAULT_NO);
  }

  @Test
  public void testOneArrivingMultipleExistingWhereArrivingBetweenTwoExistingNoCurrentFlag() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, configWithoutCurrentFlag);
    p.configure(configWithoutCurrentFlag);

    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello", 100L, 100L, 253402214400000L, 1L, 2L));
    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello", 100L, 100L, 199L, 3L, 253402214400000L));
    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello!", 200L, 200L, 253402214400000L, 3L, 4L));
    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello!", 200L, 200L, 299L, 5L, 253402214400000L));
    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello?", 300L, 300L, 253402214400000L, 5L, 253402214400000L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 150L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 3);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(2)), MutationType.INSERT);

    Long systemStart1 = (Long)RowUtils.get(planned.get(1), "systemstart");
    Long systemStart2 = (Long)RowUtils.get(planned.get(2), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 199L);
    assertEquals(RowUtils.get(planned.get(0), "systemstart"), 3L);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), systemStart1 - 1);

    assertEquals(RowUtils.get(planned.get(1), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(1), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(1), "eventend"), 149L);
    assertTrue(systemStart1 >= preplanSystemTime);
    assertTrue(systemStart1 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(1), "systemend"), 253402214400000L);

    assertEquals(RowUtils.get(planned.get(2), "value"), "world");
    assertEquals(RowUtils.get(planned.get(2), "eventstart"), 150L);
    assertEquals(RowUtils.get(planned.get(2), "eventend"), 199L);
    assertTrue(systemStart2 >= preplanSystemTime);
    assertTrue(systemStart2 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(2), "systemend"), 253402214400000L);
  }

  @Test
  public void testOneArrivingMultipleExistingWhereArrivingEarlierThanAllExisting() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, 1L, 2L, CURRENT_FLAG_DEFAULT_NO));
    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 199L, 3L, 253402214400000L, CURRENT_FLAG_DEFAULT_NO));
    existing.add(new RowWithSchema(existingSchema, "a", "hello!", 200L, 200L, 253402214400000L, 3L, 4L, CURRENT_FLAG_DEFAULT_NO));
    existing.add(new RowWithSchema(existingSchema, "a", "hello!", 200L, 200L, 299L, 5L, 253402214400000L, CURRENT_FLAG_DEFAULT_NO));
    existing.add(new RowWithSchema(existingSchema, "a", "hello?", 300L, 300L, 253402214400000L, 5L, 253402214400000L, CURRENT_FLAG_DEFAULT_YES));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 50L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 1);

    Long systemStart0 = (Long)RowUtils.get(planned.get(0), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value"), "world");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 50L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 99L);
    assertTrue(systemStart0 >= preplanSystemTime);
    assertTrue(systemStart0 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(0), "currentflag"), CURRENT_FLAG_DEFAULT_NO);
  }

  @Test
  public void testOneArrivingMultipleExistingWhereArrivingEarlierThanAllExistingNoCurrentFlag() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, configWithoutCurrentFlag);
    p.configure(configWithoutCurrentFlag);

    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello", 100L, 100L, 253402214400000L, 1L, 2L));
    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello", 100L, 100L, 199L, 3L, 253402214400000L));
    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello!", 200L, 200L, 253402214400000L, 3L, 4L));
    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello!", 200L, 200L, 299L, 5L, 253402214400000L));
    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello?", 300L, 300L, 253402214400000L, 5L, 253402214400000L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 50L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 1);

    Long systemStart0 = (Long)RowUtils.get(planned.get(0), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value"), "world");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 50L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 99L);
    assertTrue(systemStart0 >= preplanSystemTime);
    assertTrue(systemStart0 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), 253402214400000L);
  }

  @Test
  public void testMultipleArrivingOneExistingWhereAllArrivingLaterThanExisting() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, 1L, 253402214400000L, CURRENT_FLAG_DEFAULT_YES));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 200L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world!", 300L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world?", 400L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 5);

    Long systemStart1 = (Long)RowUtils.get(planned.get(1), "systemstart");
    Long systemStart2 = (Long)RowUtils.get(planned.get(2), "systemstart");
    Long systemStart3 = (Long)RowUtils.get(planned.get(3), "systemstart");
    Long systemStart4 = (Long)RowUtils.get(planned.get(4), "systemstart");

    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(RowUtils.get(planned.get(0), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(0), "systemstart"), 1L);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), systemStart1 - 1);
    assertEquals(RowUtils.get(planned.get(0), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(1), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(1), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(1), "eventend"), 199L);
    assertTrue(systemStart1 >= preplanSystemTime);
    assertTrue(systemStart1 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(1), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(1), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(PlannerUtils.getMutationType(planned.get(2)), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(2), "value"), "world");
    assertEquals(RowUtils.get(planned.get(2), "eventstart"), 200L);
    assertEquals(RowUtils.get(planned.get(2), "eventend"), 299L);
    assertTrue(systemStart2 >= preplanSystemTime);
    assertTrue(systemStart2 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(2), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(2), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(PlannerUtils.getMutationType(planned.get(3)), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(3), "value"), "world!");
    assertEquals(RowUtils.get(planned.get(3), "eventstart"), 300L);
    assertEquals(RowUtils.get(planned.get(3), "eventend"), 399L);
    assertTrue(systemStart3 >= preplanSystemTime);
    assertTrue(systemStart3 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(3), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(3), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(PlannerUtils.getMutationType(planned.get(4)), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(4), "value"), "world?");
    assertEquals(RowUtils.get(planned.get(4), "eventstart"), 400L);
    assertEquals(RowUtils.get(planned.get(4), "eventend"), 253402214400000L);
    assertTrue(systemStart4 >= preplanSystemTime);
    assertTrue(systemStart4 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(4), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(4), "currentflag"), CURRENT_FLAG_DEFAULT_YES);
  }

  @Test
  public void testMultipleArrivingOneExistingWhereAllArrivingLaterThanExistingNoCurrentFlag() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, configWithoutCurrentFlag);
    p.configure(configWithoutCurrentFlag);

    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello", 100L, 100L, 253402214400000L, 1L, 253402214400000L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 200L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world!", 300L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world?", 400L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 5);

    Long systemStart1 = (Long)RowUtils.get(planned.get(1), "systemstart");
    Long systemStart2 = (Long)RowUtils.get(planned.get(2), "systemstart");
    Long systemStart3 = (Long)RowUtils.get(planned.get(3), "systemstart");
    Long systemStart4 = (Long)RowUtils.get(planned.get(4), "systemstart");

    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(RowUtils.get(planned.get(0), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(0), "systemstart"), 1L);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), systemStart1 - 1);

    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(1), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(1), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(1), "eventend"), 199L);
    assertTrue(systemStart1 >= preplanSystemTime);
    assertTrue(systemStart1 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(1), "systemend"), 253402214400000L);

    assertEquals(PlannerUtils.getMutationType(planned.get(2)), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(2), "value"), "world");
    assertEquals(RowUtils.get(planned.get(2), "eventstart"), 200L);
    assertEquals(RowUtils.get(planned.get(2), "eventend"), 299L);
    assertTrue(systemStart2 >= preplanSystemTime);
    assertTrue(systemStart2 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(2), "systemend"), 253402214400000L);

    assertEquals(PlannerUtils.getMutationType(planned.get(3)), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(3), "value"), "world!");
    assertEquals(RowUtils.get(planned.get(3), "eventstart"), 300L);
    assertEquals(RowUtils.get(planned.get(3), "eventend"), 399L);
    assertTrue(systemStart3 >= preplanSystemTime);
    assertTrue(systemStart3 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(3), "systemend"), 253402214400000L);

    assertEquals(PlannerUtils.getMutationType(planned.get(4)), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(4), "value"), "world?");
    assertEquals(RowUtils.get(planned.get(4), "eventstart"), 400L);
    assertEquals(RowUtils.get(planned.get(4), "eventend"), 253402214400000L);
    assertTrue(systemStart4 >= preplanSystemTime);
    assertTrue(systemStart4 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(4), "systemend"), 253402214400000L);
  }

  @Test
  public void testMultipleArrivingOneExistingWhereOneArrivingSameTimeAsExistingWithSameValuesAndRestArrivingLaterThanExisting() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, 1L, 253402214400000L, CURRENT_FLAG_DEFAULT_YES));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello", 100L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 200L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world!", 300L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 4);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(2)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(3)), MutationType.INSERT);

    Long systemStart1 = (Long)RowUtils.get(planned.get(1), "systemstart");
    Long systemStart2 = (Long)RowUtils.get(planned.get(2), "systemstart");
    Long systemStart3 = (Long)RowUtils.get(planned.get(3), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(0), "systemstart"), 1L);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), systemStart1 - 1);
    assertEquals(RowUtils.get(planned.get(0), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(1), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(1), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(1), "eventend"), 199L);
    assertTrue(systemStart1 >= preplanSystemTime);
    assertTrue(systemStart1 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(1), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(1), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(2), "value"), "world");
    assertEquals(RowUtils.get(planned.get(2), "eventstart"), 200L);
    assertEquals(RowUtils.get(planned.get(2), "eventend"), 299L);
    assertTrue(systemStart2 >= preplanSystemTime);
    assertTrue(systemStart2 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(2), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(2), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(3), "value"), "world!");
    assertEquals(RowUtils.get(planned.get(3), "eventstart"), 300L);
    assertEquals(RowUtils.get(planned.get(3), "eventend"), 253402214400000L);
    assertTrue(systemStart3 >= preplanSystemTime);
    assertTrue(systemStart3 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(3), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(3), "currentflag"), CURRENT_FLAG_DEFAULT_YES);
  }

  @Test
  public void testMultipleArrivingOneExistingWhereOneArrivingSameTimeAsExistingWithSameValuesAndRestArrivingLaterThanExistingNoCurrentFlag() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, configWithoutCurrentFlag);
    p.configure(configWithoutCurrentFlag);

    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello", 100L, 100L, 253402214400000L, 1L, 253402214400000L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello", 100L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 200L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world!", 300L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 4);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(2)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(3)), MutationType.INSERT);

    Long systemStart1 = (Long)RowUtils.get(planned.get(1), "systemstart");
    Long systemStart2 = (Long)RowUtils.get(planned.get(2), "systemstart");
    Long systemStart3 = (Long)RowUtils.get(planned.get(3), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(0), "systemstart"), 1L);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), systemStart1 - 1);

    assertEquals(RowUtils.get(planned.get(1), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(1), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(1), "eventend"), 199L);
    assertTrue(systemStart1 >= preplanSystemTime);
    assertTrue(systemStart1 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(1), "systemend"), 253402214400000L);

    assertEquals(RowUtils.get(planned.get(2), "value"), "world");
    assertEquals(RowUtils.get(planned.get(2), "eventstart"), 200L);
    assertEquals(RowUtils.get(planned.get(2), "eventend"), 299L);
    assertTrue(systemStart2 >= preplanSystemTime);
    assertTrue(systemStart2 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(2), "systemend"), 253402214400000L);

    assertEquals(RowUtils.get(planned.get(3), "value"), "world!");
    assertEquals(RowUtils.get(planned.get(3), "eventstart"), 300L);
    assertEquals(RowUtils.get(planned.get(3), "eventend"), 253402214400000L);
    assertTrue(systemStart3 >= preplanSystemTime);
    assertTrue(systemStart3 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(3), "systemend"), 253402214400000L);
  }

  @Test
  public void testMultipleArrivingOneExistingWhereOneArrivingSameTimeAsExistingWithDifferentValuesAndRestArrivingLaterThanExisting() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, 1L, 253402214400000L, CURRENT_FLAG_DEFAULT_YES));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 100L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world!", 200L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world?", 300L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 4);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(2)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(3)), MutationType.INSERT);

    Long systemStart1 = (Long)RowUtils.get(planned.get(1), "systemstart");
    Long systemStart2 = (Long)RowUtils.get(planned.get(2), "systemstart");
    Long systemStart3 = (Long)RowUtils.get(planned.get(3), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(0), "systemstart"), 1L);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), systemStart1 - 1);
    assertEquals(RowUtils.get(planned.get(0), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(1), "value"), "world");
    assertEquals(RowUtils.get(planned.get(1), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(1), "eventend"), 199L);
    assertTrue(systemStart1 >= preplanSystemTime);
    assertTrue(systemStart1 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(1), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(1), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(2), "value"), "world!");
    assertEquals(RowUtils.get(planned.get(2), "eventstart"), 200L);
    assertEquals(RowUtils.get(planned.get(2), "eventend"), 299L);
    assertTrue(systemStart2 >= preplanSystemTime);
    assertTrue(systemStart2 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(2), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(2), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(3), "value"), "world?");
    assertEquals(RowUtils.get(planned.get(3), "eventstart"), 300L);
    assertEquals(RowUtils.get(planned.get(3), "eventend"), 253402214400000L);
    assertTrue(systemStart3 >= preplanSystemTime);
    assertTrue(systemStart3 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(3), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(3), "currentflag"), CURRENT_FLAG_DEFAULT_YES);
  }

  @Test
  public void testMultipleArrivingOneExistingWhereOneArrivingSameTimeAsExistingWithDifferentValuesAndRestArrivingLaterThanExistingNoCurrentFlag() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, configWithoutCurrentFlag);
    p.configure(configWithoutCurrentFlag);

    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello", 100L, 100L, 253402214400000L, 1L, 253402214400000L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 100L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world!", 200L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world?", 300L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 4);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(2)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(3)), MutationType.INSERT);

    Long systemStart1 = (Long)RowUtils.get(planned.get(1), "systemstart");
    Long systemStart2 = (Long)RowUtils.get(planned.get(2), "systemstart");
    Long systemStart3 = (Long)RowUtils.get(planned.get(3), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(0), "systemstart"), 1L);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), systemStart1 - 1);

    assertEquals(RowUtils.get(planned.get(1), "value"), "world");
    assertEquals(RowUtils.get(planned.get(1), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(1), "eventend"), 199L);
    assertTrue(systemStart1 >= preplanSystemTime);
    assertTrue(systemStart1 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(1), "systemend"), 253402214400000L);

    assertEquals(RowUtils.get(planned.get(2), "value"), "world!");
    assertEquals(RowUtils.get(planned.get(2), "eventstart"), 200L);
    assertEquals(RowUtils.get(planned.get(2), "eventend"), 299L);
    assertTrue(systemStart2 >= preplanSystemTime);
    assertTrue(systemStart2 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(2), "systemend"), 253402214400000L);

    assertEquals(RowUtils.get(planned.get(3), "value"), "world?");
    assertEquals(RowUtils.get(planned.get(3), "eventstart"), 300L);
    assertEquals(RowUtils.get(planned.get(3), "eventend"), 253402214400000L);
    assertTrue(systemStart3 >= preplanSystemTime);
    assertTrue(systemStart3 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(3), "systemend"), 253402214400000L);
  }

  @Test
  public void testMultipleArrivingMultipleExistingWhereAllArrivingSameTimeAsExistingWithSameValues() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, 1L, 2L, CURRENT_FLAG_DEFAULT_NO));
    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 199L, 3L, 253402214400000L, CURRENT_FLAG_DEFAULT_NO));
    existing.add(new RowWithSchema(existingSchema, "a", "hello!", 200L, 200L, 253402214400000L, 3L, 4L, CURRENT_FLAG_DEFAULT_NO));
    existing.add(new RowWithSchema(existingSchema, "a", "hello!", 200L, 200L, 299L, 5L, 253402214400000L, CURRENT_FLAG_DEFAULT_NO));
    existing.add(new RowWithSchema(existingSchema, "a", "hello?", 300L, 300L, 253402214400000L, 5L, 253402214400000L, CURRENT_FLAG_DEFAULT_YES));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello", 100L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello!", 200L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello?", 300L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 0);
  }

  @Test
  public void testMultipleArrivingMultipleExistingWhereAllArrivingSameTimeAsExistingWithSameValuesNoCurrentFlag() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, configWithoutCurrentFlag);
    p.configure(configWithoutCurrentFlag);

    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello", 100L, 100L, 253402214400000L, 1L, 2L));
    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello", 100L, 100L, 199L, 3L, 253402214400000L));
    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello!", 200L, 200L, 253402214400000L, 3L, 4L));
    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello!", 200L, 200L, 299L, 5L, 253402214400000L));
    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello?", 300L, 300L, 253402214400000L, 5L, 253402214400000L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello", 100L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello!", 200L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello?", 300L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 0);
  }

  @Test
  public void testMultipleArrivingMultipleExistingWhereAllArrivingSameTimeAsExistingWithDifferentValues() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, 1L, 2L, CURRENT_FLAG_DEFAULT_NO));
    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 199L, 3L, 253402214400000L, CURRENT_FLAG_DEFAULT_NO));
    existing.add(new RowWithSchema(existingSchema, "a", "hello!", 200L, 200L, 253402214400000L, 3L, 4L, CURRENT_FLAG_DEFAULT_NO));
    existing.add(new RowWithSchema(existingSchema, "a", "hello!", 200L, 200L, 299L, 5L, 253402214400000L, CURRENT_FLAG_DEFAULT_NO));
    existing.add(new RowWithSchema(existingSchema, "a", "hello?", 300L, 300L, 253402214400000L, 5L, 253402214400000L, CURRENT_FLAG_DEFAULT_YES));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 100L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world!", 200L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world?", 300L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 6);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(2)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(3)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(4)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(5)), MutationType.INSERT);

    Long systemStart1 = (Long)RowUtils.get(planned.get(1), "systemstart");
    Long systemStart3 = (Long)RowUtils.get(planned.get(3), "systemstart");
    Long systemStart5 = (Long)RowUtils.get(planned.get(5), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 199L);
    assertEquals(RowUtils.get(planned.get(0), "systemstart"), 3L);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), systemStart1 - 1);
    assertEquals(RowUtils.get(planned.get(0), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(1), "value"), "world");
    assertEquals(RowUtils.get(planned.get(1), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(1), "eventend"), 199L);
    assertTrue(systemStart1 >= preplanSystemTime);
    assertTrue(systemStart1 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(1), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(1), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(2), "value"), "hello!");
    assertEquals(RowUtils.get(planned.get(2), "eventstart"), 200L);
    assertEquals(RowUtils.get(planned.get(2), "eventend"), 299L);
    assertEquals(RowUtils.get(planned.get(2), "systemstart"), 5L);
    assertEquals(RowUtils.get(planned.get(2), "systemend"), systemStart3 - 1);
    assertEquals(RowUtils.get(planned.get(2), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(3), "value"), "world!");
    assertEquals(RowUtils.get(planned.get(3), "eventstart"), 200L);
    assertEquals(RowUtils.get(planned.get(3), "eventend"), 299L);
    assertTrue(systemStart3 >= preplanSystemTime);
    assertTrue(systemStart3 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(3), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(3), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(4), "value"), "hello?");
    assertEquals(RowUtils.get(planned.get(4), "eventstart"), 300L);
    assertEquals(RowUtils.get(planned.get(4), "eventend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(4), "systemstart"), 5L);
    assertEquals(RowUtils.get(planned.get(4), "systemend"), systemStart5 - 1);
    assertEquals(RowUtils.get(planned.get(4), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(5), "value"), "world?");
    assertEquals(RowUtils.get(planned.get(5), "eventstart"), 300L);
    assertEquals(RowUtils.get(planned.get(5), "eventend"), 253402214400000L);
    assertTrue(systemStart5 >= preplanSystemTime);
    assertTrue(systemStart5 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(5), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(5), "currentflag"), CURRENT_FLAG_DEFAULT_YES);

  }

  @Test
  public void testMultipleArrivingMultipleExistingWhereAllArrivingSameTimeAsExistingWithDifferentValuesNoCurrentFlag() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, configWithoutCurrentFlag);
    p.configure(configWithoutCurrentFlag);

    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello", 100L, 100L, 253402214400000L, 1L, 2L));
    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello", 100L, 100L, 199L, 3L, 253402214400000L));
    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello!", 200L, 200L, 253402214400000L, 3L, 4L));
    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello!", 200L, 200L, 299L, 5L, 253402214400000L));
    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello?", 300L, 300L, 253402214400000L, 5L, 253402214400000L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 100L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world!", 200L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world?", 300L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 6);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(2)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(3)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(4)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(5)), MutationType.INSERT);

    Long systemStart1 = (Long)RowUtils.get(planned.get(1), "systemstart");
    Long systemStart3 = (Long)RowUtils.get(planned.get(3), "systemstart");
    Long systemStart5 = (Long)RowUtils.get(planned.get(5), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 199L);
    assertEquals(RowUtils.get(planned.get(0), "systemstart"), 3L);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), systemStart1 - 1);

    assertEquals(RowUtils.get(planned.get(1), "value"), "world");
    assertEquals(RowUtils.get(planned.get(1), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(1), "eventend"), 199L);
    assertTrue(systemStart1 >= preplanSystemTime);
    assertTrue(systemStart1 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(1), "systemend"), 253402214400000L);

    assertEquals(RowUtils.get(planned.get(2), "value"), "hello!");
    assertEquals(RowUtils.get(planned.get(2), "eventstart"), 200L);
    assertEquals(RowUtils.get(planned.get(2), "eventend"), 299L);
    assertEquals(RowUtils.get(planned.get(2), "systemstart"), 5L);
    assertEquals(RowUtils.get(planned.get(2), "systemend"), systemStart3 - 1);

    assertEquals(RowUtils.get(planned.get(3), "value"), "world!");
    assertEquals(RowUtils.get(planned.get(3), "eventstart"), 200L);
    assertEquals(RowUtils.get(planned.get(3), "eventend"), 299L);
    assertTrue(systemStart3 >= preplanSystemTime);
    assertTrue(systemStart3 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(3), "systemend"), 253402214400000L);

    assertEquals(RowUtils.get(planned.get(4), "value"), "hello?");
    assertEquals(RowUtils.get(planned.get(4), "eventstart"), 300L);
    assertEquals(RowUtils.get(planned.get(4), "eventend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(4), "systemstart"), 5L);
    assertEquals(RowUtils.get(planned.get(4), "systemend"), systemStart5 - 1);

    assertEquals(RowUtils.get(planned.get(5), "value"), "world?");
    assertEquals(RowUtils.get(planned.get(5), "eventstart"), 300L);
    assertEquals(RowUtils.get(planned.get(5), "eventend"), 253402214400000L);
    assertTrue(systemStart5 >= preplanSystemTime);
    assertTrue(systemStart5 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(5), "systemend"), 253402214400000L);

  }

  @Test
  public void testNoneArrivingNoneExisting() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 0);
  }

  @Test
  public void testNoneArrivingOneExisting() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, 1L, 253402214400000L, CURRENT_FLAG_DEFAULT_YES));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 0);
  }

  @Test
  public void testNoneArrivingOneExistingNoCurrentFlag() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, configWithoutCurrentFlag);
    p.configure(configWithoutCurrentFlag);

    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello", 100L, 100L, 253402214400000L, 1L, 253402214400000L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 0);
  }

  @Test
  public void testCarryForwardWhenNull() {
    p = new BitemporalHistoryPlanner();
    config = config.withValue(BitemporalHistoryPlanner.CARRY_FORWARD_CONFIG_NAME, ConfigValueFactory.fromAnyRef(true));
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, 1L, 253402214400000L, CURRENT_FLAG_DEFAULT_YES));
    arriving.add(new RowWithSchema(arrivingSchema, "a", null, 200L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 3);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(2)), MutationType.INSERT);

    Long systemStart1 = (Long)RowUtils.get(planned.get(1), "systemstart");
    Long systemStart2 = (Long)RowUtils.get(planned.get(2), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(0), "systemstart"), 1L);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), systemStart1 - 1);
    assertEquals(RowUtils.get(planned.get(0), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(1), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(1), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(1), "eventend"), 199L);
    assertTrue(systemStart1 >= preplanSystemTime);
    assertTrue(systemStart1 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(1), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(1), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(2), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(2), "eventstart"), 200L);
    assertEquals(RowUtils.get(planned.get(2), "eventend"), 253402214400000L);
    assertTrue(systemStart2 >= preplanSystemTime);
    assertTrue(systemStart2 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(2), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(2), "currentflag"), CURRENT_FLAG_DEFAULT_YES);
  }

  @Test
  public void testCarryForwardWhenNullNoCurrentFlag() {
    p = new BitemporalHistoryPlanner();
    configWithoutCurrentFlag = configWithoutCurrentFlag.withValue(BitemporalHistoryPlanner.CARRY_FORWARD_CONFIG_NAME, ConfigValueFactory.fromAnyRef(true));
    assertNoValidationFailures(p, configWithoutCurrentFlag);
    p.configure(configWithoutCurrentFlag);

    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello", 100L, 100L, 253402214400000L, 1L, 253402214400000L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", null, 200L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 3);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(2)), MutationType.INSERT);

    Long systemStart1 = (Long)RowUtils.get(planned.get(1), "systemstart");
    Long systemStart2 = (Long)RowUtils.get(planned.get(2), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(0), "systemstart"), 1L);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), systemStart1 - 1);

    assertEquals(RowUtils.get(planned.get(1), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(1), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(1), "eventend"), 199L);
    assertTrue(systemStart1 >= preplanSystemTime);
    assertTrue(systemStart1 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(1), "systemend"), 253402214400000L);

    assertEquals(RowUtils.get(planned.get(2), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(2), "eventstart"), 200L);
    assertEquals(RowUtils.get(planned.get(2), "eventend"), 253402214400000L);
    assertTrue(systemStart2 >= preplanSystemTime);
    assertTrue(systemStart2 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(2), "systemend"), 253402214400000L);
  }

  @Test
  public void testNoCarryForwardWhenNull() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, 1L, 253402214400000L, CURRENT_FLAG_DEFAULT_YES));
    arriving.add(new RowWithSchema(arrivingSchema, "a", null, 200L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 3);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(2)), MutationType.INSERT);

    Long systemStart1 = (Long)RowUtils.get(planned.get(1), "systemstart");
    Long systemStart2 = (Long)RowUtils.get(planned.get(2), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(0), "systemstart"), 1L);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), systemStart1 - 1);
    assertEquals(RowUtils.get(planned.get(0), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(1), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(1), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(1), "eventend"), 199L);
    assertTrue(systemStart1 >= preplanSystemTime);
    assertTrue(systemStart1 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(1), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(1), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(2), "value"), null);
    assertEquals(RowUtils.get(planned.get(2), "eventstart"), 200L);
    assertEquals(RowUtils.get(planned.get(2), "eventend"), 253402214400000L);
    assertTrue(systemStart2 >= preplanSystemTime);
    assertTrue(systemStart2 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(2), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(2), "currentflag"), CURRENT_FLAG_DEFAULT_YES);
  }

  @Test
  public void testNoCarryForwardWhenNullNoCurrentFlag() {
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, configWithoutCurrentFlag);
    p.configure(configWithoutCurrentFlag);

    existing.add(new RowWithSchema(existingSchemaWithoutCurrentFlag, "a", "hello", 100L, 100L, 253402214400000L, 1L, 253402214400000L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", null, 200L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 3);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(2)), MutationType.INSERT);

    Long systemStart1 = (Long)RowUtils.get(planned.get(1), "systemstart");
    Long systemStart2 = (Long)RowUtils.get(planned.get(2), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(0), "systemstart"), 1L);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), systemStart1 - 1);

    assertEquals(RowUtils.get(planned.get(1), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(1), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(1), "eventend"), 199L);
    assertTrue(systemStart1 >= preplanSystemTime);
    assertTrue(systemStart1 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(1), "systemend"), 253402214400000L);

    assertEquals(RowUtils.get(planned.get(2), "value"), null);
    assertEquals(RowUtils.get(planned.get(2), "eventstart"), 200L);
    assertEquals(RowUtils.get(planned.get(2), "eventend"), 253402214400000L);
    assertTrue(systemStart2 >= preplanSystemTime);
    assertTrue(systemStart2 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(2), "systemend"), 253402214400000L);
  }

  @Test
  public void testCarryForwardWhenNullMultipleOutOfOrderArriving() {
    p = new BitemporalHistoryPlanner();
    config = config.
        withValue(BitemporalHistoryPlanner.CARRY_FORWARD_CONFIG_NAME, ConfigValueFactory.fromAnyRef(true)).
        withValue(BitemporalHistoryPlanner.VALUE_FIELD_NAMES_CONFIG_NAME, ConfigValueFactory.fromAnyRef(Lists.newArrayList("value1","value2")));
    assertNoValidationFailures(p, config);
    p.configure(config);

    arrivingSchema = DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("key", DataTypes.StringType, false),
        DataTypes.createStructField("value1", DataTypes.StringType, true),
        DataTypes.createStructField("value2", DataTypes.StringType, true),
        DataTypes.createStructField("timestamp", DataTypes.LongType, false)));
    existingSchema = DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("key", DataTypes.StringType, false),
        DataTypes.createStructField("value1", DataTypes.StringType, false),
        DataTypes.createStructField("value2", DataTypes.StringType, false),
        DataTypes.createStructField("timestamp", DataTypes.LongType, false),
        DataTypes.createStructField("eventstart", DataTypes.LongType, false),
        DataTypes.createStructField("eventend", DataTypes.LongType, false),
        DataTypes.createStructField("systemstart", DataTypes.LongType, false),
        DataTypes.createStructField("systemend", DataTypes.LongType, false),
        DataTypes.createStructField("currentflag", DataTypes.StringType, false)));

    existing.add(new RowWithSchema(existingSchema, "a", "hello1:100", "hello2:100", 100L, 100L, 253402214400000L, 1L, 253402214400000L, CURRENT_FLAG_DEFAULT_YES));
    arriving.add(new RowWithSchema(arrivingSchema, "a", null, "hello2:200", 200L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello1:150", null, 150L));

    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 4);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(2)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(3)), MutationType.INSERT);

    Long systemStart1 = (Long)RowUtils.get(planned.get(1), "systemstart");
    Long systemStart2 = (Long)RowUtils.get(planned.get(2), "systemstart");
    Long systemStart3 = (Long)RowUtils.get(planned.get(3), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value1"), "hello1:100");
    assertEquals(RowUtils.get(planned.get(0), "value2"), "hello2:100");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(0), "systemstart"), 1L);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), systemStart1 - 1);
    assertEquals(RowUtils.get(planned.get(0), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(1), "value1"), "hello1:100");
    assertEquals(RowUtils.get(planned.get(1), "value2"), "hello2:100");
    assertEquals(RowUtils.get(planned.get(1), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(1), "eventend"), 149L);
    assertTrue(systemStart1 >= preplanSystemTime);
    assertTrue(systemStart1 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(1), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(1), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(2), "value1"), "hello1:150");
    assertEquals(RowUtils.get(planned.get(2), "value2"), "hello2:100");
    assertEquals(RowUtils.get(planned.get(2), "eventstart"), 150L);
    assertEquals(RowUtils.get(planned.get(2), "eventend"), 199L);
    assertTrue(systemStart2 >= preplanSystemTime);
    assertTrue(systemStart2 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(2), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(2), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(3), "value1"), "hello1:150");
    assertEquals(RowUtils.get(planned.get(3), "value2"), "hello2:200");
    assertEquals(RowUtils.get(planned.get(3), "eventstart"), 200L);
    assertEquals(RowUtils.get(planned.get(3), "eventend"), 253402214400000L);
    assertTrue(systemStart3 >= preplanSystemTime);
    assertTrue(systemStart3 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(3), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(3), "currentflag"), CURRENT_FLAG_DEFAULT_YES);
  }

  @Test
  public void testCarryForwardWhenNullMultipleOutOfOrderArrivingWithPreceding() {
    p = new BitemporalHistoryPlanner();
    config = config.
        withValue(BitemporalHistoryPlanner.CARRY_FORWARD_CONFIG_NAME, ConfigValueFactory.fromAnyRef(true)).
        withValue(BitemporalHistoryPlanner.VALUE_FIELD_NAMES_CONFIG_NAME, ConfigValueFactory.fromAnyRef(Lists.newArrayList("value1","value2")));
    assertNoValidationFailures(p, config);
    p.configure(config);

    arrivingSchema = DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("key", DataTypes.StringType, false),
        DataTypes.createStructField("value1", DataTypes.StringType, true),
        DataTypes.createStructField("value2", DataTypes.StringType, true),
        DataTypes.createStructField("timestamp", DataTypes.LongType, false)));
    existingSchema = DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("key", DataTypes.StringType, false),
        DataTypes.createStructField("value1", DataTypes.StringType, false),
        DataTypes.createStructField("value2", DataTypes.StringType, false),
        DataTypes.createStructField("timestamp", DataTypes.LongType, false),
        DataTypes.createStructField("eventstart", DataTypes.LongType, false),
        DataTypes.createStructField("eventend", DataTypes.LongType, false),
        DataTypes.createStructField("systemstart", DataTypes.LongType, false),
        DataTypes.createStructField("systemend", DataTypes.LongType, false),
        DataTypes.createStructField("currentflag", DataTypes.StringType, false)));

    existing.add(new RowWithSchema(existingSchema, "a", null, "hello2:100", 100L, 100L, 253402214400000L, 1L, 253402214400000L, CURRENT_FLAG_DEFAULT_YES));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello1:50", null, 50L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", null, "hello2:150", 150L));

    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 4);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(2)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(3)), MutationType.INSERT);

    Long systemStart0 = (Long)RowUtils.get(planned.get(0), "systemstart");
    Long systemStart2 = (Long)RowUtils.get(planned.get(2), "systemstart");
    Long systemStart3 = (Long)RowUtils.get(planned.get(3), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value1"), "hello1:50");
    assertEquals(RowUtils.get(planned.get(0), "value2"), null);
    assertEquals(RowUtils.get(planned.get(0), "timestamp"), 50L);
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 50L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 99L);
    assertTrue(systemStart0 >= preplanSystemTime);
    assertTrue(systemStart0 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(0), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(1), "value1"), null);
    assertEquals(RowUtils.get(planned.get(1), "value2"), "hello2:100");
    assertEquals(RowUtils.get(planned.get(1), "timestamp"), 100L);
    assertEquals(RowUtils.get(planned.get(1), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(1), "eventend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(1), "systemstart"), 1L);
    assertEquals(RowUtils.get(planned.get(1), "systemend"), systemStart2 - 1);
    assertEquals(RowUtils.get(planned.get(1), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(2), "value1"), "hello1:50");
    assertEquals(RowUtils.get(planned.get(2), "value2"), "hello2:100");
    assertEquals(RowUtils.get(planned.get(2), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(2), "eventend"), 149L);
    assertTrue(systemStart2 >= preplanSystemTime);
    assertTrue(systemStart2 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(2), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(2), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(3), "value1"), "hello1:50");
    assertEquals(RowUtils.get(planned.get(3), "value2"), "hello2:150");
    assertEquals(RowUtils.get(planned.get(3), "eventstart"), 150L);
    assertEquals(RowUtils.get(planned.get(3), "eventend"), 253402214400000L);
    assertTrue(systemStart3 >= preplanSystemTime);
    assertTrue(systemStart3 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(3), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(3), "currentflag"), CURRENT_FLAG_DEFAULT_YES);
  }

  @Test
  public void testNoCarryForwardWhenNullMultipleOutOfOrderArriving() {
    p = new BitemporalHistoryPlanner();
    config = config.
        withValue(BitemporalHistoryPlanner.VALUE_FIELD_NAMES_CONFIG_NAME, ConfigValueFactory.fromAnyRef(Lists.newArrayList("value1","value2")));
    assertNoValidationFailures(p, config);
    p.configure(config);

    arrivingSchema = DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("key", DataTypes.StringType, false),
        DataTypes.createStructField("value1", DataTypes.StringType, true),
        DataTypes.createStructField("value2", DataTypes.StringType, true),
        DataTypes.createStructField("timestamp", DataTypes.LongType, false)));
    existingSchema = DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("key", DataTypes.StringType, false),
        DataTypes.createStructField("value1", DataTypes.StringType, false),
        DataTypes.createStructField("value2", DataTypes.StringType, false),
        DataTypes.createStructField("timestamp", DataTypes.LongType, false),
        DataTypes.createStructField("eventstart", DataTypes.LongType, false),
        DataTypes.createStructField("eventend", DataTypes.LongType, false),
        DataTypes.createStructField("systemstart", DataTypes.LongType, false),
        DataTypes.createStructField("systemend", DataTypes.LongType, false),
        DataTypes.createStructField("currentflag", DataTypes.StringType, false)));

    existing.add(new RowWithSchema(existingSchema, "a", "hello1:100", "hello2:100", 100L, 100L, 253402214400000L, 1L, 253402214400000L, CURRENT_FLAG_DEFAULT_YES));
    arriving.add(new RowWithSchema(arrivingSchema, "a", null, "hello2:200", 200L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello1:150", null, 150L));

    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 4);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(2)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(3)), MutationType.INSERT);

    Long systemStart1 = (Long)RowUtils.get(planned.get(1), "systemstart");
    Long systemStart2 = (Long)RowUtils.get(planned.get(2), "systemstart");
    Long systemStart3 = (Long)RowUtils.get(planned.get(3), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value1"), "hello1:100");
    assertEquals(RowUtils.get(planned.get(0), "value2"), "hello2:100");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(0), "systemstart"), 1L);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), systemStart1 - 1);
    assertEquals(RowUtils.get(planned.get(0), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(1), "value1"), "hello1:100");
    assertEquals(RowUtils.get(planned.get(1), "value2"), "hello2:100");
    assertEquals(RowUtils.get(planned.get(1), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(1), "eventend"), 149L);
    assertTrue(systemStart1 >= preplanSystemTime);
    assertTrue(systemStart1 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(1), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(1), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(2), "value1"), "hello1:150");
    assertEquals(RowUtils.get(planned.get(2), "value2"), null);
    assertEquals(RowUtils.get(planned.get(2), "eventstart"), 150L);
    assertEquals(RowUtils.get(planned.get(2), "eventend"), 199L);
    assertTrue(systemStart2 >= preplanSystemTime);
    assertTrue(systemStart2 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(2), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(2), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(3), "value1"), null);
    assertEquals(RowUtils.get(planned.get(3), "value2"), "hello2:200");
    assertEquals(RowUtils.get(planned.get(3), "eventstart"), 200L);
    assertEquals(RowUtils.get(planned.get(3), "eventend"), 253402214400000L);
    assertTrue(systemStart3 >= preplanSystemTime);
    assertTrue(systemStart3 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(3), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(3), "currentflag"), CURRENT_FLAG_DEFAULT_YES);
  }

  @Test
  public void testNoCarryForwardWhenNullMultipleOutOfOrderArrivingWithPreceding() {
    p = new BitemporalHistoryPlanner();
    config = config.
        withValue(BitemporalHistoryPlanner.VALUE_FIELD_NAMES_CONFIG_NAME, ConfigValueFactory.fromAnyRef(Lists.newArrayList("value1","value2")));
    assertNoValidationFailures(p, config);
    p.configure(config);

    arrivingSchema = DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("key", DataTypes.StringType, false),
        DataTypes.createStructField("value1", DataTypes.StringType, true),
        DataTypes.createStructField("value2", DataTypes.StringType, true),
        DataTypes.createStructField("timestamp", DataTypes.LongType, false)));
    existingSchema = DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("key", DataTypes.StringType, false),
        DataTypes.createStructField("value1", DataTypes.StringType, false),
        DataTypes.createStructField("value2", DataTypes.StringType, false),
        DataTypes.createStructField("timestamp", DataTypes.LongType, false),
        DataTypes.createStructField("eventstart", DataTypes.LongType, false),
        DataTypes.createStructField("eventend", DataTypes.LongType, false),
        DataTypes.createStructField("systemstart", DataTypes.LongType, false),
        DataTypes.createStructField("systemend", DataTypes.LongType, false),
        DataTypes.createStructField("currentflag", DataTypes.StringType, false)));

    existing.add(new RowWithSchema(existingSchema, "a", null, "hello2:100", 100L, 100L, 253402214400000L, 1L, 253402214400000L, CURRENT_FLAG_DEFAULT_YES));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello1:50", null, 50L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", null, "hello2:150", 150L));

    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 4);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(2)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(3)), MutationType.INSERT);

    Long systemStart0 = (Long)RowUtils.get(planned.get(0), "systemstart");
    Long systemStart2 = (Long)RowUtils.get(planned.get(2), "systemstart");
    Long systemStart3 = (Long)RowUtils.get(planned.get(3), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value1"), "hello1:50");
    assertEquals(RowUtils.get(planned.get(0), "value2"), null);
    assertEquals(RowUtils.get(planned.get(0), "timestamp"), 50L);
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 50L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 99L);
    assertTrue(systemStart0 >= preplanSystemTime);
    assertTrue(systemStart0 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(0), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(1), "value1"), null);
    assertEquals(RowUtils.get(planned.get(1), "value2"), "hello2:100");
    assertEquals(RowUtils.get(planned.get(1), "timestamp"), 100L);
    assertEquals(RowUtils.get(planned.get(1), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(1), "eventend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(1), "systemstart"), 1L);
    assertEquals(RowUtils.get(planned.get(1), "systemend"), systemStart2 - 1);
    assertEquals(RowUtils.get(planned.get(1), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(2), "value1"), null);
    assertEquals(RowUtils.get(planned.get(2), "value2"), "hello2:100");
    assertEquals(RowUtils.get(planned.get(2), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(2), "eventend"), 149L);
    assertTrue(systemStart2 >= preplanSystemTime);
    assertTrue(systemStart2 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(2), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(2), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(3), "value1"), null);
    assertEquals(RowUtils.get(planned.get(3), "value2"), "hello2:150");
    assertEquals(RowUtils.get(planned.get(3), "eventstart"), 150L);
    assertEquals(RowUtils.get(planned.get(3), "eventend"), 253402214400000L);
    assertTrue(systemStart3 >= preplanSystemTime);
    assertTrue(systemStart3 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(3), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(3), "currentflag"), CURRENT_FLAG_DEFAULT_YES);
  }

  @Test
  public void testNonDefaultCurrentFlag() {
    String currFlagYes = "YES";
    String currFlagNo = "NO";

    config = config.
        withValue(BitemporalHistoryPlanner.CURRENT_FLAG_YES_CONFIG_NAME, ConfigValueFactory.fromAnyRef(currFlagYes)).
        withValue(BitemporalHistoryPlanner.CURRENT_FLAG_NO_CONFIG_NAME, ConfigValueFactory.fromAnyRef(currFlagNo));

    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello", 100L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 200L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 2);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);

    Long systemStart0 = (Long)RowUtils.get(planned.get(0), "systemstart");
    Long systemStart1 = (Long)RowUtils.get(planned.get(1), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 199L);
    assertTrue(systemStart0 >= preplanSystemTime);
    assertTrue(systemStart0 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(0), "currentflag"), currFlagNo);

    assertEquals(RowUtils.get(planned.get(1), "value"), "world");
    assertEquals(RowUtils.get(planned.get(1), "eventstart"), 200L);
    assertEquals(RowUtils.get(planned.get(1), "eventend"), 253402214400000L);
    assertTrue(systemStart1 >= preplanSystemTime);
    assertTrue(systemStart1 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(1), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(1), "currentflag"), currFlagYes);
  }
  
  @Test
  public void testNonDefaultTimeModel() {
    config = config
        .withValue(BitemporalHistoryPlanner.EVENT_TIME_MODEL_CONFIG_NAME + "." + TimeModelFactory.TYPE_CONFIG_NAME, 
            ConfigValueFactory.fromAnyRef("longmillis"))
        .withValue(BitemporalHistoryPlanner.SYSTEM_TIME_MODEL_CONFIG_NAME + "." + TimeModelFactory.TYPE_CONFIG_NAME, 
            ConfigValueFactory.fromAnyRef("longmillis"));
    
    p = new BitemporalHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, 1L, 253402214400000L, CURRENT_FLAG_DEFAULT_YES));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 200L));
    Row key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 3);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(2)), MutationType.INSERT);

    Long systemStart1 = (Long)RowUtils.get(planned.get(1), "systemstart");
    Long systemStart2 = (Long)RowUtils.get(planned.get(2), "systemstart");

    assertEquals(RowUtils.get(planned.get(0), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(0), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(0), "eventend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(0), "systemstart"), 1L);
    assertEquals(RowUtils.get(planned.get(0), "systemend"), systemStart1 - 1);
    assertEquals(RowUtils.get(planned.get(0), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(1), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(1), "eventstart"), 100L);
    assertEquals(RowUtils.get(planned.get(1), "eventend"), 199L);
    assertTrue(systemStart1 >= preplanSystemTime);
    assertTrue(systemStart1 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(1), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(1), "currentflag"), CURRENT_FLAG_DEFAULT_NO);

    assertEquals(RowUtils.get(planned.get(2), "value"), "world");
    assertEquals(RowUtils.get(planned.get(2), "eventstart"), 200L);
    assertEquals(RowUtils.get(planned.get(2), "eventend"), 253402214400000L);
    assertTrue(systemStart2 >= preplanSystemTime);
    assertTrue(systemStart2 < preplanSystemTime + 5000);
    assertEquals(RowUtils.get(planned.get(2), "systemend"), 253402214400000L);
    assertEquals(RowUtils.get(planned.get(2), "currentflag"), CURRENT_FLAG_DEFAULT_YES);
  }
  
}

