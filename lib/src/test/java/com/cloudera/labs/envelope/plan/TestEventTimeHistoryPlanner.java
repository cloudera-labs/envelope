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

package com.cloudera.labs.envelope.plan;

import com.cloudera.labs.envelope.component.ComponentFactory;
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

import static com.cloudera.labs.envelope.validate.ValidationAssert.assertNoValidationFailures;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

public class TestEventTimeHistoryPlanner {

  private Row key;
  private List<Row> arriving;
  private List<Row> existing;
  private StructType keySchema;
  private StructType arrivingSchema;
  private StructType existingSchema;
  private Map<String, Object> configMap;
  private Config config;
  private EventTimeHistoryPlanner p;

  @Before
  public void before() {
    key = null;
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
      DataTypes.createStructField("startdate", DataTypes.LongType, false),
      DataTypes.createStructField("enddate", DataTypes.LongType, false),
      DataTypes.createStructField("currentflag", DataTypes.StringType, false),
      DataTypes.createStructField("lastupdated", DataTypes.StringType, false)));

    configMap = Maps.newHashMap();
    configMap.put(EventTimeHistoryPlanner.KEY_FIELD_NAMES_CONFIG_NAME, Lists.newArrayList("key"));
    configMap.put(EventTimeHistoryPlanner.VALUE_FIELD_NAMES_CONFIG_NAME, Lists.newArrayList("value"));
    configMap.put(EventTimeHistoryPlanner.TIMESTAMP_FIELD_NAMES_CONFIG_NAME, Lists.newArrayList("timestamp"));
    configMap.put(EventTimeHistoryPlanner.EFFECTIVE_FROM_FIELD_NAMES_CONFIG_NAME, Lists.newArrayList("startdate"));
    configMap.put(EventTimeHistoryPlanner.EFFECTIVE_TO_FIELD_NAMES_CONFIG_NAME, Lists.newArrayList("enddate"));
    configMap.put(EventTimeHistoryPlanner.CURRENT_FLAG_FIELD_NAME_CONFIG_NAME, "currentflag");
    configMap.put(EventTimeHistoryPlanner.LAST_UPDATED_FIELD_NAME_CONFIG_NAME, "lastupdated");

    config = ConfigFactory.parseMap(configMap);
  }

  @Test
  public void testOneArrivingNoneExisting() {
    p = new EventTimeHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello", 100L));
    key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 1);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.INSERT);
    assertEquals(planned.get(0).getAs("startdate"), 100L);
    assertEquals(planned.get(0).getAs("enddate"), 253402214400000L);
    assertEquals(planned.get(0).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES);
  }

  @Test
  public void testMultipleArrivingNoneExisting() {
    p = new EventTimeHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello", 100L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 200L));
    key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 2);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(planned.get(0).getAs("startdate"), 100L);
    assertEquals(planned.get(0).getAs("enddate"), 199L);
    assertEquals(planned.get(0).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO);
    assertEquals(planned.get(1).getAs("startdate"), 200L);
    assertEquals(planned.get(1).getAs("enddate"), 253402214400000L);
    assertEquals(planned.get(1).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES);
  }

  @Test
  public void testOneArrivingOneExistingWhereArrivingLaterThanExisting() {
    p = new EventTimeHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 200L));
    key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 2);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(planned.get(0).getAs("startdate"), 100L);
    assertEquals(planned.get(0).getAs("enddate"), 199L);
    assertEquals(planned.get(0).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO);
    assertEquals(planned.get(1).getAs("startdate"), 200L);
    assertEquals(planned.get(1).getAs("enddate"), 253402214400000L);
    assertEquals(planned.get(1).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES);
  }
  
  @Test
  public void testOneArrivingOneExistingWhereArrivingLaterThanExistingButSameValues() {
    p = new EventTimeHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello", 200L));
    key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 0);
  }

  @Test
  public void testOneArrivingOneExistingWhereArrivingSameTimeAsExistingWithSameValues() {
    p = new EventTimeHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello", 100L));
    key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 0);
  }

  @Test
  public void testOneArrivingOneExistingWhereArrivingSameTimeAsExistingWithDifferentValues() {
    p = new EventTimeHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 100L));
    key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 1);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(planned.get(0).getAs("value"), "world");
    assertEquals(planned.get(0).getAs("startdate"), 100L);
    assertEquals(planned.get(0).getAs("enddate"), 253402214400000L);
    assertEquals(planned.get(0).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES);
  }

  @Test
  public void testOneArrivingOneExistingWhereArrivingEarlierThanExisting() {
    p = new EventTimeHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 50L));
    key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 1);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.INSERT);
    assertEquals(planned.get(0).getAs("value"), "world");
    assertEquals(planned.get(0).getAs("startdate"), 50L);
    assertEquals(planned.get(0).getAs("enddate"), 99L);
    assertEquals(planned.get(0).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO);
  }

  @Test
  public void testOneArrivingMultipleExistingWhereArrivingLaterThanAllExisting() {
    p = new EventTimeHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 199L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO, ""));
    existing.add(new RowWithSchema(existingSchema, "a", "hello!", 200L, 200L, 299L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO, ""));
    existing.add(new RowWithSchema(existingSchema, "a", "hello?", 300L, 300L, 253402214400000L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 400L));
    key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 2);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(planned.get(0).getAs("startdate"), 300L);
    assertEquals(planned.get(0).getAs("enddate"), 399L);
    assertEquals(planned.get(0).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO);
    assertEquals(planned.get(1).getAs("startdate"), 400L);
    assertEquals(planned.get(1).getAs("enddate"), 253402214400000L);
    assertEquals(planned.get(1).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES);
  }

  @Test
  public void testOneArrivingMultipleExistingWhereArrivingSameTimeAsLatestExistingWithSameValues() {
    p = new EventTimeHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 199L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO, ""));
    existing.add(new RowWithSchema(existingSchema, "a", "hello!", 200L, 200L, 299L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO, ""));
    existing.add(new RowWithSchema(existingSchema, "a", "hello?", 300L, 300L, 253402214400000L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello?", 300L));
    key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 0);
  }

  @Test
  public void testOneArrivingMultipleExistingWhereArrivingSameTimeAsLatestExistingWithDifferentValues() {
    p = new EventTimeHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 199L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO, ""));
    existing.add(new RowWithSchema(existingSchema, "a", "hello!", 200L, 200L, 299L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO, ""));
    existing.add(new RowWithSchema(existingSchema, "a", "hello?", 300L, 300L, 253402214400000L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 300L));
    key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 1);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(planned.get(0).getAs("value"), "world");
    assertEquals(planned.get(0).getAs("startdate"), 300L);
    assertEquals(planned.get(0).getAs("enddate"), 253402214400000L);
    assertEquals(planned.get(0).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES);
  }

  @Test
  public void testOneArrivingMultipleExistingWhereArrivingBetweenTwoExisting() {
    p = new EventTimeHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 199L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO, ""));
    existing.add(new RowWithSchema(existingSchema, "a", "hello!", 200L, 200L, 299L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO, ""));
    existing.add(new RowWithSchema(existingSchema, "a", "hello?", 300L, 300L, 253402214400000L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 150L));
    key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 2);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(planned.get(0).getAs("startdate"), 100L);
    assertEquals(planned.get(0).getAs("enddate"), 149L);
    assertEquals(planned.get(0).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO);
    assertEquals(planned.get(1).getAs("startdate"), 150L);
    assertEquals(planned.get(1).getAs("enddate"), 199L);
    assertEquals(planned.get(0).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO);
  }

  @Test
  public void testOneArrivingMultipleExistingWhereArrivingEarlierThanAllExisting() {
    p = new EventTimeHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 199L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO, ""));
    existing.add(new RowWithSchema(existingSchema, "a", "hello!", 200L, 200L, 299L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO, ""));
    existing.add(new RowWithSchema(existingSchema, "a", "hello?", 300L, 300L, 253402214400000L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 50L));
    key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 1);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.INSERT);
    assertEquals(planned.get(0).getAs("startdate"), 50L);
    assertEquals(planned.get(0).getAs("enddate"), 99L);
    assertEquals(planned.get(0).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO);
  }

  @Test
  public void testMultipleArrivingOneExistingWhereAllArrivingLaterThanExisting() {
    p = new EventTimeHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 200L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world!", 300L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world?", 400L));
    key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 4);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(planned.get(0).getAs("startdate"), 100L);
    assertEquals(planned.get(0).getAs("enddate"), 199L);
    assertEquals(planned.get(0).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(planned.get(1).getAs("startdate"), 200L);
    assertEquals(planned.get(1).getAs("enddate"), 299L);
    assertEquals(planned.get(1).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO);
    assertEquals(PlannerUtils.getMutationType(planned.get(2)), MutationType.INSERT);
    assertEquals(planned.get(2).getAs("startdate"), 300L);
    assertEquals(planned.get(2).getAs("enddate"), 399L);
    assertEquals(planned.get(2).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO);
    assertEquals(PlannerUtils.getMutationType(planned.get(3)), MutationType.INSERT);
    assertEquals(planned.get(3).getAs("startdate"), 400L);
    assertEquals(planned.get(3).getAs("enddate"), 253402214400000L);
    assertEquals(planned.get(3).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES);
  }

  @Test
  public void testMultipleArrivingOneExistingWhereOneArrivingSameTimeAsExistingWithSameValuesAndRestArrivingLaterThanExisting() {
    p = new EventTimeHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello", 100L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 200L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world!", 300L));
    key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 3);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(planned.get(0).getAs("startdate"), 100L);
    assertEquals(planned.get(0).getAs("enddate"), 199L);
    assertEquals(planned.get(0).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(planned.get(1).getAs("startdate"), 200L);
    assertEquals(planned.get(1).getAs("enddate"), 299L);
    assertEquals(planned.get(1).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO);
    assertEquals(PlannerUtils.getMutationType(planned.get(2)), MutationType.INSERT);
    assertEquals(planned.get(2).getAs("startdate"), 300L);
    assertEquals(planned.get(2).getAs("enddate"), 253402214400000L);
    assertEquals(planned.get(2).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES);
  }

  @Test
  public void testMultipleArrivingOneExistingWhereOneArrivingSameTimeAsExistingWithDifferentValuesAndRestArrivingLaterThanExisting() {
    p = new EventTimeHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 100L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world!", 200L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world?", 300L));
    key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 3);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(planned.get(0).getAs("value"), "world");
    assertEquals(planned.get(0).getAs("startdate"), 100L);
    assertEquals(planned.get(0).getAs("enddate"), 199L);
    assertEquals(planned.get(0).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(planned.get(1).getAs("startdate"), 200L);
    assertEquals(planned.get(1).getAs("enddate"), 299L);
    assertEquals(planned.get(1).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO);
    assertEquals(PlannerUtils.getMutationType(planned.get(2)), MutationType.INSERT);
    assertEquals(planned.get(2).getAs("startdate"), 300L);
    assertEquals(planned.get(2).getAs("enddate"), 253402214400000L);
    assertEquals(planned.get(2).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES);
  }

  @Test
  public void testMultipleArrivingMultipleExistingWhereAllArrivingSameTimeAsExistingWithSameValues() {
    p = new EventTimeHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 199L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO, ""));
    existing.add(new RowWithSchema(existingSchema, "a", "hello!", 200L, 200L, 299L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO, ""));
    existing.add(new RowWithSchema(existingSchema, "a", "hello?", 300L, 300L, 253402214400000L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello", 100L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello!", 200L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello?", 300L));
    key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 0);
  }

  @Test
  public void testMultipleArrivingMultipleExistingWhereAllArrivingSameTimeAsExistingWithDifferentValues() {
    p = new EventTimeHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 199L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO, ""));
    existing.add(new RowWithSchema(existingSchema, "a", "hello!", 200L, 200L, 299L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO, ""));
    existing.add(new RowWithSchema(existingSchema, "a", "hello?", 300L, 300L, 253402214400000L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 100L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world!", 200L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world?", 300L));
    key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 3);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(planned.get(0).getAs("value"), "world");
    assertEquals(planned.get(0).getAs("startdate"), 100L);
    assertEquals(planned.get(0).getAs("enddate"), 199L);
    assertEquals(planned.get(0).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.UPDATE);
    assertEquals(planned.get(1).getAs("value"), "world!");
    assertEquals(planned.get(1).getAs("startdate"), 200L);
    assertEquals(planned.get(1).getAs("enddate"), 299L);
    assertEquals(planned.get(1).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO);
    assertEquals(PlannerUtils.getMutationType(planned.get(2)), MutationType.UPDATE);
    assertEquals(planned.get(2).getAs("value"), "world?");
    assertEquals(planned.get(2).getAs("startdate"), 300L);
    assertEquals(planned.get(2).getAs("enddate"), 253402214400000L);
    assertEquals(planned.get(2).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES);
  }

  @Test
  public void testNoneArrivingNoneExisting() {
    p = new EventTimeHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 0);
  }

  @Test
  public void testNoneArrivingOneExisting() {
    p = new EventTimeHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES, ""));
    key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 0);
  }

  @Test
  public void testCarryForwardWhenNull() {
    p = new EventTimeHistoryPlanner();
    config = config.withValue(EventTimeHistoryPlanner.CARRY_FORWARD_CONFIG_NAME, ConfigValueFactory.fromAnyRef(true));
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", null, 200L));
    key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 2);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(planned.get(0).getAs("value"), "hello");
    assertEquals(planned.get(0).getAs("startdate"), 100L);
    assertEquals(planned.get(0).getAs("enddate"), 199L);
    assertEquals(planned.get(0).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(planned.get(1).getAs("value"), "hello");
    assertEquals(planned.get(1).getAs("startdate"), 200L);
    assertEquals(planned.get(1).getAs("enddate"), 253402214400000L);
    assertEquals(planned.get(1).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES);
  }

  @Test
  public void testNoCarryForwardWhenNull() {
    p = new EventTimeHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", null, 200L));
    key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 2);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(planned.get(0).getAs("value"), "hello");
    assertEquals(planned.get(0).getAs("startdate"), 100L);
    assertEquals(planned.get(0).getAs("enddate"), 199L);
    assertEquals(planned.get(0).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(planned.get(1).getAs("value"), null);
    assertEquals(planned.get(1).getAs("startdate"), 200L);
    assertEquals(planned.get(1).getAs("enddate"), 253402214400000L);
    assertEquals(planned.get(1).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES);
  }

  @Test
  public void testCarryForwardMultipleWhenNull() {
    p = new EventTimeHistoryPlanner();
    config = config.withValue(EventTimeHistoryPlanner.CARRY_FORWARD_CONFIG_NAME, ConfigValueFactory.fromAnyRef(true));
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", null, 200L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", null, 150L));
    key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 3);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(planned.get(0).getAs("value"), "hello");
    assertEquals(planned.get(0).getAs("startdate"), 100L);
    assertEquals(planned.get(0).getAs("enddate"), 149L);
    assertEquals(planned.get(0).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(planned.get(1).getAs("value"), "hello");
    assertEquals(planned.get(1).getAs("startdate"), 150L);
    assertEquals(planned.get(1).getAs("enddate"), 199L);
    assertEquals(planned.get(1).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO);
    assertEquals(PlannerUtils.getMutationType(planned.get(2)), MutationType.INSERT);
    assertEquals(planned.get(2).getAs("value"), "hello");
    assertEquals(planned.get(2).getAs("startdate"), 200L);
    assertEquals(planned.get(2).getAs("enddate"), 253402214400000L);
    assertEquals(planned.get(2).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES);
  }

  @Test
  public void testCarryForwardMultipleWhenNullOutOfOrderMultipleValued() {
    p = new EventTimeHistoryPlanner();
    config = config.
        withValue(EventTimeHistoryPlanner.CARRY_FORWARD_CONFIG_NAME, ConfigValueFactory.fromAnyRef(true)).
        withValue(EventTimeHistoryPlanner.VALUE_FIELD_NAMES_CONFIG_NAME, ConfigValueFactory.fromAnyRef(Lists.newArrayList("value1","value2")));
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
        DataTypes.createStructField("startdate", DataTypes.LongType, false),
        DataTypes.createStructField("enddate", DataTypes.LongType, false),
        DataTypes.createStructField("currentflag", DataTypes.StringType, false),
        DataTypes.createStructField("lastupdated", DataTypes.StringType, false)));

    existing.add(new RowWithSchema(existingSchema, "a", "hello1:100", "hello2:100", 100L, 100L, 253402214400000L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", null, "hello2:200", 200L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello1:150", null, 150L));
    key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 3);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(planned.get(0).getAs("value1"), "hello1:100");
    assertEquals(planned.get(0).getAs("value2"), "hello2:100");
    assertEquals(planned.get(0).getAs("startdate"), 100L);
    assertEquals(planned.get(0).getAs("enddate"), 149L);
    assertEquals(planned.get(0).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(planned.get(1).getAs("value1"), "hello1:150");
    assertEquals(planned.get(1).getAs("value2"), "hello2:100");
    assertEquals(planned.get(1).getAs("startdate"), 150L);
    assertEquals(planned.get(1).getAs("enddate"), 199L);
    assertEquals(planned.get(1).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO);
    assertEquals(PlannerUtils.getMutationType(planned.get(2)), MutationType.INSERT);
    assertEquals(planned.get(2).getAs("value1"), "hello1:150");
    assertEquals(planned.get(2).getAs("value2"), "hello2:200");
    assertEquals(planned.get(2).getAs("startdate"), 200L);
    assertEquals(planned.get(2).getAs("enddate"), 253402214400000L);
    assertEquals(planned.get(2).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES);
  }

  @Test
  public void testCarryForwardMultipleWhenNullOutOfOrderMultipleValuedWithPreceding() {
    p = new EventTimeHistoryPlanner();
    config = config.
        withValue(EventTimeHistoryPlanner.CARRY_FORWARD_CONFIG_NAME, ConfigValueFactory.fromAnyRef(true)).
        withValue(EventTimeHistoryPlanner.VALUE_FIELD_NAMES_CONFIG_NAME, ConfigValueFactory.fromAnyRef(Lists.newArrayList("value1","value2")));
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
        DataTypes.createStructField("startdate", DataTypes.LongType, false),
        DataTypes.createStructField("enddate", DataTypes.LongType, false),
        DataTypes.createStructField("currentflag", DataTypes.StringType, false),
        DataTypes.createStructField("lastupdated", DataTypes.StringType, false)));

    existing.add(new RowWithSchema(existingSchema, "a", "hello1:100", null, 100L, 100L, 253402214400000L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", null, "hello2:50", 50L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", null, "hello2:200", 200L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello1:150", null, 150L));
    key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 4);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.INSERT);
    assertEquals(planned.get(0).getAs("value1"), null);
    assertEquals(planned.get(0).getAs("value2"), "hello2:50");
    assertEquals(planned.get(0).getAs("startdate"), 50L);
    assertEquals(planned.get(0).getAs("enddate"), 99L);
    assertEquals(planned.get(0).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.UPDATE);
    assertEquals(planned.get(1).getAs("value1"), "hello1:100");
    assertEquals(planned.get(1).getAs("value2"), "hello2:50");
    assertEquals(planned.get(1).getAs("startdate"), 100L);
    assertEquals(planned.get(1).getAs("enddate"), 149L);
    assertEquals(planned.get(1).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO);
    assertEquals(PlannerUtils.getMutationType(planned.get(2)), MutationType.INSERT);
    assertEquals(planned.get(2).getAs("value1"), "hello1:150");
    assertEquals(planned.get(2).getAs("value2"), "hello2:50");
    assertEquals(planned.get(2).getAs("startdate"), 150L);
    assertEquals(planned.get(2).getAs("enddate"), 199L);
    assertEquals(planned.get(2).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO);
    assertEquals(PlannerUtils.getMutationType(planned.get(3)), MutationType.INSERT);
    assertEquals(planned.get(3).getAs("value1"), "hello1:150");
    assertEquals(planned.get(3).getAs("value2"), "hello2:200");
    assertEquals(planned.get(3).getAs("startdate"), 200L);
    assertEquals(planned.get(3).getAs("enddate"), 253402214400000L);
    assertEquals(planned.get(3).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES);
  }

  @Test
  public void testNoCarryForwardMultipleWhenNullOutOfOrderMultipleValuedWithPreceding() {
    p = new EventTimeHistoryPlanner();
    config = config.
        withValue(EventTimeHistoryPlanner.VALUE_FIELD_NAMES_CONFIG_NAME, ConfigValueFactory.fromAnyRef(Lists.newArrayList("value1","value2")));
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
        DataTypes.createStructField("startdate", DataTypes.LongType, false),
        DataTypes.createStructField("enddate", DataTypes.LongType, false),
        DataTypes.createStructField("currentflag", DataTypes.StringType, false),
        DataTypes.createStructField("lastupdated", DataTypes.StringType, false)));

    existing.add(new RowWithSchema(existingSchema, "a", "hello1:100", null, 100L, 100L, 253402214400000L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", null, "hello2:50", 50L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", null, "hello2:200", 200L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello1:150", null, 150L));
    key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 4);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.INSERT);
    assertEquals(planned.get(0).getAs("value1"), null);
    assertEquals(planned.get(0).getAs("value2"), "hello2:50");
    assertEquals(planned.get(0).getAs("startdate"), 50L);
    assertEquals(planned.get(0).getAs("enddate"), 99L);
    assertEquals(planned.get(0).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.UPDATE);
    assertEquals(planned.get(1).getAs("value1"), "hello1:100");
    assertEquals(planned.get(1).getAs("value2"), null);
    assertEquals(planned.get(1).getAs("startdate"), 100L);
    assertEquals(planned.get(1).getAs("enddate"), 149L);
    assertEquals(planned.get(1).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO);
    assertEquals(PlannerUtils.getMutationType(planned.get(2)), MutationType.INSERT);
    assertEquals(planned.get(2).getAs("value1"), "hello1:150");
    assertEquals(planned.get(2).getAs("value2"), null);
    assertEquals(planned.get(2).getAs("startdate"), 150L);
    assertEquals(planned.get(2).getAs("enddate"), 199L);
    assertEquals(planned.get(2).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO);
    assertEquals(PlannerUtils.getMutationType(planned.get(3)), MutationType.INSERT);
    assertEquals(planned.get(3).getAs("value1"), null);
    assertEquals(planned.get(3).getAs("value2"), "hello2:200");
    assertEquals(planned.get(3).getAs("startdate"), 200L);
    assertEquals(planned.get(3).getAs("enddate"), 253402214400000L);
    assertEquals(planned.get(3).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES);
  }

  @Test
  public void testNonDefaultCurrentFlag() {
    String currFlagYes = "YES";
    String currFlagNo = "NO";

    config = config.
        withValue(EventTimeHistoryPlanner.CURRENT_FLAG_YES_CONFIG_NAME, ConfigValueFactory.fromAnyRef(currFlagYes)).
        withValue(EventTimeHistoryPlanner.CURRENT_FLAG_NO_CONFIG_NAME, ConfigValueFactory.fromAnyRef(currFlagNo));

    p = new EventTimeHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello", 100L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 200L));
    key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 2);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.INSERT);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(planned.get(0).getAs("startdate"), 100L);
    assertEquals(planned.get(0).getAs("enddate"), 199L);
    assertEquals(planned.get(0).getAs("currentflag"), currFlagNo);
    assertEquals(planned.get(1).getAs("startdate"), 200L);
    assertEquals(planned.get(1).getAs("enddate"), 253402214400000L);
    assertEquals(planned.get(1).getAs("currentflag"), currFlagYes);
  }
  
  @Test
  public void testNonDefaultTimeModel() {
    config = config
        .withValue(EventTimeHistoryPlanner.EVENT_TIME_MODEL_CONFIG_NAME + "." + ComponentFactory.TYPE_CONFIG_NAME,
            ConfigValueFactory.fromAnyRef("longmillis"))
        .withValue(EventTimeHistoryPlanner.LAST_UPDATED_TIME_MODEL_CONFIG_NAME + "." + ComponentFactory.TYPE_CONFIG_NAME,
            ConfigValueFactory.fromAnyRef("longmillis"));
    
    p = new EventTimeHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 200L));
    key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 2);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(planned.get(0).getAs("startdate"), 100L);
    assertEquals(planned.get(0).getAs("enddate"), 199L);
    assertEquals(planned.get(0).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO);
    assertEquals(planned.get(1).getAs("startdate"), 200L);
    assertEquals(planned.get(1).getAs("enddate"), 253402214400000L);
    assertEquals(planned.get(1).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES);
  }

  @Test
  public void testSurrogateKey() {
    config = config
        .withValue(EventTimeHistoryPlanner.SURROGATE_KEY_FIELD_NAME_CONFIG_NAME,
            ConfigValueFactory.fromAnyRef("surrogate"));
    p = new EventTimeHistoryPlanner();
    assertNoValidationFailures(p, config);
    p.configure(config);

    Row existingRow = new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 253402214400000L, EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES, "");
    existingRow = PlannerUtils.appendSurrogateKey(existingRow, "surrogate");

    existing.add(existingRow);
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 200L));
    key = new RowWithSchema(keySchema, "a");

    List<Row> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 2);
    assertEquals(PlannerUtils.getMutationType(planned.get(0)), MutationType.UPDATE);
    assertEquals(PlannerUtils.getMutationType(planned.get(1)), MutationType.INSERT);
    assertEquals(planned.get(0).getAs("startdate"), 100L);
    assertEquals(planned.get(0).getAs("enddate"), 199L);
    assertEquals(planned.get(0).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_NO);
    assertEquals(planned.get(0).getAs("surrogate"), existingRow.getAs("surrogate"));
    assertEquals(planned.get(1).getAs("startdate"), 200L);
    assertEquals(planned.get(1).getAs("enddate"), 253402214400000L);
    assertEquals(planned.get(1).getAs("currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_DEFAULT_YES);
    assertNotNull(planned.get(1).getAs("surrogate"));
    assertNotEquals(planned.get(0).getAs("surrogate"), planned.get(1).getAs("surrogate"));
  }
  
}


