/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.plan;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class TestEventTimeHistoryPlanner {

  Row key;
  List<Row> arriving;
  List<Row> existing;
  StructType keySchema;
  StructType arrivingSchema;
  StructType existingSchema;
  Map<String, Object> configMap;
  Config config;
  RandomPlanner p;

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
      DataTypes.createStructField("startdate", DataTypes.LongType, false),
      DataTypes.createStructField("enddate", DataTypes.LongType, false),
      DataTypes.createStructField("currentflag", DataTypes.StringType, false),
      DataTypes.createStructField("lastupdated", DataTypes.StringType, false)));

    configMap = Maps.newHashMap();
    configMap.put(EventTimeHistoryPlanner.KEY_FIELD_NAMES_CONFIG_NAME, Lists.newArrayList("key"));
    configMap.put(EventTimeHistoryPlanner.VALUE_FIELD_NAMES_CONFIG_NAME, Lists.newArrayList("value"));
    configMap.put(EventTimeHistoryPlanner.TIMESTAMP_FIELD_NAME_CONFIG_NAME, "timestamp");
    configMap.put(EventTimeHistoryPlanner.EFFECTIVE_FROM_FIELD_NAME_CONFIG_NAME, "startdate");
    configMap.put(EventTimeHistoryPlanner.EFFECTIVE_TO_FIELD_NAME_CONFIG_NAME, "enddate");
    configMap.put(EventTimeHistoryPlanner.CURRENT_FLAG_FIELD_NAME_CONFIG_NAME, "currentflag");
    configMap.put(EventTimeHistoryPlanner.LAST_UPDATED_FIELD_NAME_CONFIG_NAME, "lastupdated");
    config = ConfigFactory.parseMap(configMap);
  }

  @Test
  public void testOneArrivingNoneExisting() {
    p = new EventTimeHistoryPlanner();
    p.configure(config);

    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello", 100L));
    Row key = new RowWithSchema(keySchema, "a");

    List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 1);
    assertEquals(planned.get(0).getMutationType(), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "startdate"), 100L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "enddate"), EventTimeHistoryPlanner.FAR_FUTURE_MILLIS);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_YES);
  }

  @Test
  public void testMultipleArrivingNoneExisting() {
    p = new EventTimeHistoryPlanner();
    p.configure(config);

    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello", 100L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 200L));
    Row key = new RowWithSchema(keySchema, "a");

    List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 2);
    assertEquals(planned.get(0).getMutationType(), MutationType.INSERT);
    assertEquals(planned.get(1).getMutationType(), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "startdate"), 100L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "enddate"), 199L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_NO);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "startdate"), 200L);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "enddate"), EventTimeHistoryPlanner.FAR_FUTURE_MILLIS);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_YES);
  }

  @Test
  public void testOneArrivingOneExistingWhereArrivingLaterThanExisting() {
    p = new EventTimeHistoryPlanner();
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, EventTimeHistoryPlanner.FAR_FUTURE_MILLIS, EventTimeHistoryPlanner.CURRENT_FLAG_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 200L));
    Row key = new RowWithSchema(keySchema, "a");

    List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 2);
    assertEquals(planned.get(0).getMutationType(), MutationType.UPDATE);
    assertEquals(planned.get(1).getMutationType(), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "startdate"), 100L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "enddate"), 199L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_NO);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "startdate"), 200L);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "enddate"), EventTimeHistoryPlanner.FAR_FUTURE_MILLIS);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_YES);
  }

  @Test
  public void testOneArrivingOneExistingWhereArrivingSameTimeAsExistingWithSameValues() {
    p = new EventTimeHistoryPlanner();
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, EventTimeHistoryPlanner.FAR_FUTURE_MILLIS, EventTimeHistoryPlanner.CURRENT_FLAG_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello", 100L));
    Row key = new RowWithSchema(keySchema, "a");

    List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 0);
  }

  @Test
  public void testOneArrivingOneExistingWhereArrivingSameTimeAsExistingWithDifferentValues() {
    p = new EventTimeHistoryPlanner();
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, EventTimeHistoryPlanner.FAR_FUTURE_MILLIS, EventTimeHistoryPlanner.CURRENT_FLAG_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 100L));
    Row key = new RowWithSchema(keySchema, "a");

    List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 1);
    assertEquals(planned.get(0).getMutationType(), MutationType.UPDATE);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "value"), "world");
    assertEquals(RowUtils.get(planned.get(0).getRow(), "startdate"), 100L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "enddate"), EventTimeHistoryPlanner.FAR_FUTURE_MILLIS);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_YES);
  }

  @Test
  public void testOneArrivingOneExistingWhereArrivingEarlierThanExisting() {
    p = new EventTimeHistoryPlanner();
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, EventTimeHistoryPlanner.FAR_FUTURE_MILLIS, EventTimeHistoryPlanner.CURRENT_FLAG_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 50L));
    Row key = new RowWithSchema(keySchema, "a");

    List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 1);
    assertEquals(planned.get(0).getMutationType(), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "value"), "world");
    assertEquals(RowUtils.get(planned.get(0).getRow(), "startdate"), 50L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "enddate"), 99L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_NO);
  }

  @Test
  public void testOneArrivingMultipleExistingWhereArrivingLaterThanAllExisting() {
    p = new EventTimeHistoryPlanner();
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 199L, EventTimeHistoryPlanner.CURRENT_FLAG_NO, ""));
    existing.add(new RowWithSchema(existingSchema, "a", "hello!", 200L, 200L, 299L, EventTimeHistoryPlanner.CURRENT_FLAG_NO, ""));
    existing.add(new RowWithSchema(existingSchema, "a", "hello?", 300L, 300L, EventTimeHistoryPlanner.FAR_FUTURE_MILLIS, EventTimeHistoryPlanner.CURRENT_FLAG_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 400L));
    Row key = new RowWithSchema(keySchema, "a");

    List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 2);
    assertEquals(planned.get(0).getMutationType(), MutationType.UPDATE);
    assertEquals(planned.get(1).getMutationType(), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "startdate"), 300L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "enddate"), 399L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_NO);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "startdate"), 400L);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "enddate"), EventTimeHistoryPlanner.FAR_FUTURE_MILLIS);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_YES);
  }

  @Test
  public void testOneArrivingMultipleExistingWhereArrivingSameTimeAsLatestExistingWithSameValues() {
    p = new EventTimeHistoryPlanner();
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 199L, EventTimeHistoryPlanner.CURRENT_FLAG_NO, ""));
    existing.add(new RowWithSchema(existingSchema, "a", "hello!", 200L, 200L, 299L, EventTimeHistoryPlanner.CURRENT_FLAG_NO, ""));
    existing.add(new RowWithSchema(existingSchema, "a", "hello?", 300L, 300L, EventTimeHistoryPlanner.FAR_FUTURE_MILLIS, EventTimeHistoryPlanner.CURRENT_FLAG_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello?", 300L));
    Row key = new RowWithSchema(keySchema, "a");

    List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 0);
  }

  @Test
  public void testOneArrivingMultipleExistingWhereArrivingSameTimeAsLatestExistingWithDifferentValues() {
    p = new EventTimeHistoryPlanner();
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 199L, EventTimeHistoryPlanner.CURRENT_FLAG_NO, ""));
    existing.add(new RowWithSchema(existingSchema, "a", "hello!", 200L, 200L, 299L, EventTimeHistoryPlanner.CURRENT_FLAG_NO, ""));
    existing.add(new RowWithSchema(existingSchema, "a", "hello?", 300L, 300L, EventTimeHistoryPlanner.FAR_FUTURE_MILLIS, EventTimeHistoryPlanner.CURRENT_FLAG_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 300L));
    Row key = new RowWithSchema(keySchema, "a");

    List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 1);
    assertEquals(planned.get(0).getMutationType(), MutationType.UPDATE);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "value"), "world");
    assertEquals(RowUtils.get(planned.get(0).getRow(), "startdate"), 300L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "enddate"), EventTimeHistoryPlanner.FAR_FUTURE_MILLIS);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_YES);
  }

  @Test
  public void testOneArrivingMultipleExistingWhereArrivingBetweenTwoExisting() {
    p = new EventTimeHistoryPlanner();
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 199L, EventTimeHistoryPlanner.CURRENT_FLAG_NO, ""));
    existing.add(new RowWithSchema(existingSchema, "a", "hello!", 200L, 200L, 299L, EventTimeHistoryPlanner.CURRENT_FLAG_NO, ""));
    existing.add(new RowWithSchema(existingSchema, "a", "hello?", 300L, 300L, EventTimeHistoryPlanner.FAR_FUTURE_MILLIS, EventTimeHistoryPlanner.CURRENT_FLAG_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 150L));
    Row key = new RowWithSchema(keySchema, "a");

    List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 2);
    assertEquals(planned.get(0).getMutationType(), MutationType.UPDATE);
    assertEquals(planned.get(1).getMutationType(), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "startdate"), 100L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "enddate"), 149L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_NO);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "startdate"), 150L);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "enddate"), 199L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_NO);
  }

  @Test
  public void testOneArrivingMultipleExistingWhereArrivingEarlierThanAllExisting() {
    p = new EventTimeHistoryPlanner();
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 199L, EventTimeHistoryPlanner.CURRENT_FLAG_NO, ""));
    existing.add(new RowWithSchema(existingSchema, "a", "hello!", 200L, 200L, 299L, EventTimeHistoryPlanner.CURRENT_FLAG_NO, ""));
    existing.add(new RowWithSchema(existingSchema, "a", "hello?", 300L, 300L, EventTimeHistoryPlanner.FAR_FUTURE_MILLIS, EventTimeHistoryPlanner.CURRENT_FLAG_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 50L));
    Row key = new RowWithSchema(keySchema, "a");

    List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 1);
    assertEquals(planned.get(0).getMutationType(), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "startdate"), 50L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "enddate"), 99L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_NO);
  }

  @Test
  public void testMultipleArrivingOneExistingWhereAllArrivingLaterThanExisting() {
    p = new EventTimeHistoryPlanner();
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, EventTimeHistoryPlanner.FAR_FUTURE_MILLIS, EventTimeHistoryPlanner.CURRENT_FLAG_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 200L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world!", 300L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world?", 400L));
    Row key = new RowWithSchema(keySchema, "a");

    List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 4);
    assertEquals(planned.get(0).getMutationType(), MutationType.UPDATE);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "startdate"), 100L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "enddate"), 199L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_NO);
    assertEquals(planned.get(1).getMutationType(), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "startdate"), 200L);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "enddate"), 299L);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_NO);
    assertEquals(planned.get(2).getMutationType(), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(2).getRow(), "startdate"), 300L);
    assertEquals(RowUtils.get(planned.get(2).getRow(), "enddate"), 399L);
    assertEquals(RowUtils.get(planned.get(2).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_NO);
    assertEquals(planned.get(3).getMutationType(), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(3).getRow(), "startdate"), 400L);
    assertEquals(RowUtils.get(planned.get(3).getRow(), "enddate"), EventTimeHistoryPlanner.FAR_FUTURE_MILLIS);
    assertEquals(RowUtils.get(planned.get(3).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_YES);
  }

  @Test
  public void testMultipleArrivingOneExistingWhereOneArrivingSameTimeAsExistingWithSameValuesAndRestArrivingLaterThanExisting() {
    p = new EventTimeHistoryPlanner();
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, EventTimeHistoryPlanner.FAR_FUTURE_MILLIS, EventTimeHistoryPlanner.CURRENT_FLAG_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello", 100L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 200L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world!", 300L));
    Row key = new RowWithSchema(keySchema, "a");

    List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 3);
    assertEquals(planned.get(0).getMutationType(), MutationType.UPDATE);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "startdate"), 100L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "enddate"), 199L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_NO);
    assertEquals(planned.get(1).getMutationType(), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "startdate"), 200L);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "enddate"), 299L);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_NO);
    assertEquals(planned.get(2).getMutationType(), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(2).getRow(), "startdate"), 300L);
    assertEquals(RowUtils.get(planned.get(2).getRow(), "enddate"), EventTimeHistoryPlanner.FAR_FUTURE_MILLIS);
    assertEquals(RowUtils.get(planned.get(2).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_YES);
  }

  @Test
  public void testMultipleArrivingOneExistingWhereOneArrivingSameTimeAsExistingWithDifferentValuesAndRestArrivingLaterThanExisting() {
    p = new EventTimeHistoryPlanner();
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, EventTimeHistoryPlanner.FAR_FUTURE_MILLIS, EventTimeHistoryPlanner.CURRENT_FLAG_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 100L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world!", 200L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world?", 300L));
    Row key = new RowWithSchema(keySchema, "a");

    List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 3);
    assertEquals(planned.get(0).getMutationType(), MutationType.UPDATE);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "value"), "world");
    assertEquals(RowUtils.get(planned.get(0).getRow(), "startdate"), 100L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "enddate"), 199L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_NO);
    assertEquals(planned.get(1).getMutationType(), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "startdate"), 200L);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "enddate"), 299L);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_NO);
    assertEquals(planned.get(2).getMutationType(), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(2).getRow(), "startdate"), 300L);
    assertEquals(RowUtils.get(planned.get(2).getRow(), "enddate"), EventTimeHistoryPlanner.FAR_FUTURE_MILLIS);
    assertEquals(RowUtils.get(planned.get(2).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_YES);
  }

  @Test
  public void testMultipleArrivingMultipleExistingWhereAllArrivingSameTimeAsExistingWithSameValues() {
    p = new EventTimeHistoryPlanner();
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 199L, EventTimeHistoryPlanner.CURRENT_FLAG_NO, ""));
    existing.add(new RowWithSchema(existingSchema, "a", "hello!", 200L, 200L, 299L, EventTimeHistoryPlanner.CURRENT_FLAG_NO, ""));
    existing.add(new RowWithSchema(existingSchema, "a", "hello?", 300L, 300L, EventTimeHistoryPlanner.FAR_FUTURE_MILLIS, EventTimeHistoryPlanner.CURRENT_FLAG_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello", 100L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello!", 200L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello?", 300L));
    Row key = new RowWithSchema(keySchema, "a");

    List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 0);
  }

  @Test
  public void testMultipleArrivingMultipleExistingWhereAllArrivingSameTimeAsExistingWithDifferentValues() {
    p = new EventTimeHistoryPlanner();
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, 199L, EventTimeHistoryPlanner.CURRENT_FLAG_NO, ""));
    existing.add(new RowWithSchema(existingSchema, "a", "hello!", 200L, 200L, 299L, EventTimeHistoryPlanner.CURRENT_FLAG_NO, ""));
    existing.add(new RowWithSchema(existingSchema, "a", "hello?", 300L, 300L, EventTimeHistoryPlanner.FAR_FUTURE_MILLIS, EventTimeHistoryPlanner.CURRENT_FLAG_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world", 100L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world!", 200L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "world?", 300L));
    Row key = new RowWithSchema(keySchema, "a");

    List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 3);
    assertEquals(planned.get(0).getMutationType(), MutationType.UPDATE);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "value"), "world");
    assertEquals(RowUtils.get(planned.get(0).getRow(), "startdate"), 100L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "enddate"), 199L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_NO);
    assertEquals(planned.get(1).getMutationType(), MutationType.UPDATE);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "value"), "world!");
    assertEquals(RowUtils.get(planned.get(1).getRow(), "startdate"), 200L);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "enddate"), 299L);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_NO);
    assertEquals(planned.get(2).getMutationType(), MutationType.UPDATE);
    assertEquals(RowUtils.get(planned.get(2).getRow(), "value"), "world?");
    assertEquals(RowUtils.get(planned.get(2).getRow(), "startdate"), 300L);
    assertEquals(RowUtils.get(planned.get(2).getRow(), "enddate"), EventTimeHistoryPlanner.FAR_FUTURE_MILLIS);
    assertEquals(RowUtils.get(planned.get(2).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_YES);
  }

  @Test
  public void testNoneArrivingNoneExisting() {
    p = new EventTimeHistoryPlanner();
    p.configure(config);

    Row key = new RowWithSchema(keySchema, "a");

    List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 0);
  }

  @Test
  public void testNoneArrivingOneExisting() {
    p = new EventTimeHistoryPlanner();
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, EventTimeHistoryPlanner.FAR_FUTURE_MILLIS, EventTimeHistoryPlanner.CURRENT_FLAG_YES, ""));
    Row key = new RowWithSchema(keySchema, "a");

    List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 0);
  }
  
  @Test
  public void testCarryForwardWhenNull() {
    p = new EventTimeHistoryPlanner();
    config = config.withValue(EventTimeHistoryPlanner.CARRY_FORWARD_CONFIG_NAME, ConfigValueFactory.fromAnyRef(true));
    p.configure(config);
    
    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, EventTimeHistoryPlanner.FAR_FUTURE_MILLIS, EventTimeHistoryPlanner.CURRENT_FLAG_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", null, 200L));
    Row key = new RowWithSchema(keySchema, "a");

    List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 2);
    assertEquals(planned.get(0).getMutationType(), MutationType.UPDATE);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(0).getRow(), "startdate"), 100L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "enddate"), 199L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_NO);
    assertEquals(planned.get(1).getMutationType(), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(1).getRow(), "startdate"), 200L);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "enddate"), EventTimeHistoryPlanner.FAR_FUTURE_MILLIS);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_YES);
  }
  
  @Test
  public void testNoCarryForwardWhenNull() {
    p = new EventTimeHistoryPlanner();
    p.configure(config);
    
    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, EventTimeHistoryPlanner.FAR_FUTURE_MILLIS, EventTimeHistoryPlanner.CURRENT_FLAG_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", null, 200L));
    Row key = new RowWithSchema(keySchema, "a");

    List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 2);
    assertEquals(planned.get(0).getMutationType(), MutationType.UPDATE);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(0).getRow(), "startdate"), 100L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "enddate"), 199L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_NO);
    assertEquals(planned.get(1).getMutationType(), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "value"), null);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "startdate"), 200L);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "enddate"), EventTimeHistoryPlanner.FAR_FUTURE_MILLIS);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_YES);
  }

  @Test
  public void testCarryForwardMultipleWhenNull() {
    p = new EventTimeHistoryPlanner();
    config = config.withValue(EventTimeHistoryPlanner.CARRY_FORWARD_CONFIG_NAME, ConfigValueFactory.fromAnyRef(true));
    p.configure(config);

    existing.add(new RowWithSchema(existingSchema, "a", "hello", 100L, 100L, EventTimeHistoryPlanner.FAR_FUTURE_MILLIS, EventTimeHistoryPlanner.CURRENT_FLAG_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", null, 200L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", null, 150L));
    Row key = new RowWithSchema(keySchema, "a");

    List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 3);
    assertEquals(planned.get(0).getMutationType(), MutationType.UPDATE);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(0).getRow(), "startdate"), 100L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "enddate"), 149L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_NO);
    assertEquals(planned.get(1).getMutationType(), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(1).getRow(), "startdate"), 150L);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "enddate"), 199L);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_NO);
    assertEquals(planned.get(2).getMutationType(), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(2).getRow(), "value"), "hello");
    assertEquals(RowUtils.get(planned.get(2).getRow(), "startdate"), 200L);
    assertEquals(RowUtils.get(planned.get(2).getRow(), "enddate"), EventTimeHistoryPlanner.FAR_FUTURE_MILLIS);
    assertEquals(RowUtils.get(planned.get(2).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_YES);
  }

  @Test
  public void testCarryForwardMultipleWhenNullOutOfOrderMultipleValued() {
    p = new EventTimeHistoryPlanner();
    config = config.
        withValue(EventTimeHistoryPlanner.CARRY_FORWARD_CONFIG_NAME, ConfigValueFactory.fromAnyRef(true)).
        withValue(EventTimeHistoryPlanner.VALUE_FIELD_NAMES_CONFIG_NAME, ConfigValueFactory.fromAnyRef(Lists.newArrayList("value1","value2")));
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

    existing.add(new RowWithSchema(existingSchema, "a", "hello1:100", "hello2:100", 100L, 100L, EventTimeHistoryPlanner.FAR_FUTURE_MILLIS, EventTimeHistoryPlanner.CURRENT_FLAG_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", null, "hello2:200", 200L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello1:150", null, 150L));
    Row key = new RowWithSchema(keySchema, "a");

    List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 3);
    assertEquals(planned.get(0).getMutationType(), MutationType.UPDATE);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "value1"), "hello1:100");
    assertEquals(RowUtils.get(planned.get(0).getRow(), "value2"), "hello2:100");
    assertEquals(RowUtils.get(planned.get(0).getRow(), "startdate"), 100L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "enddate"), 149L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_NO);
    assertEquals(planned.get(1).getMutationType(), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "value1"), "hello1:150");
    assertEquals(RowUtils.get(planned.get(1).getRow(), "value2"), "hello2:100");
    assertEquals(RowUtils.get(planned.get(1).getRow(), "startdate"), 150L);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "enddate"), 199L);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_NO);
    assertEquals(planned.get(2).getMutationType(), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(2).getRow(), "value1"), "hello1:150");
    assertEquals(RowUtils.get(planned.get(2).getRow(), "value2"), "hello2:200");
    assertEquals(RowUtils.get(planned.get(2).getRow(), "startdate"), 200L);
    assertEquals(RowUtils.get(planned.get(2).getRow(), "enddate"), EventTimeHistoryPlanner.FAR_FUTURE_MILLIS);
    assertEquals(RowUtils.get(planned.get(2).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_YES);
  }

  @Test
  public void testCarryForwardMultipleWhenNullOutOfOrderMultipleValuedWithPreceding() {
    p = new EventTimeHistoryPlanner();
    config = config.
        withValue(EventTimeHistoryPlanner.CARRY_FORWARD_CONFIG_NAME, ConfigValueFactory.fromAnyRef(true)).
        withValue(EventTimeHistoryPlanner.VALUE_FIELD_NAMES_CONFIG_NAME, ConfigValueFactory.fromAnyRef(Lists.newArrayList("value1","value2")));
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

    existing.add(new RowWithSchema(existingSchema, "a", "hello1:100", null, 100L, 100L, EventTimeHistoryPlanner.FAR_FUTURE_MILLIS, EventTimeHistoryPlanner.CURRENT_FLAG_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", null, "hello2:50", 50L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", null, "hello2:200", 200L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello1:150", null, 150L));
    Row key = new RowWithSchema(keySchema, "a");

    List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 4);
    assertEquals(planned.get(0).getMutationType(), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "value1"), null);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "value2"), "hello2:50");
    assertEquals(RowUtils.get(planned.get(0).getRow(), "startdate"), 50L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "enddate"), 99L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_NO);
    assertEquals(planned.get(1).getMutationType(), MutationType.UPDATE);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "value1"), "hello1:100");
    assertEquals(RowUtils.get(planned.get(1).getRow(), "value2"), "hello2:50");
    assertEquals(RowUtils.get(planned.get(1).getRow(), "startdate"), 100L);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "enddate"), 149L);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_NO);
    assertEquals(planned.get(2).getMutationType(), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(2).getRow(), "value1"), "hello1:150");
    assertEquals(RowUtils.get(planned.get(2).getRow(), "value2"), "hello2:50");
    assertEquals(RowUtils.get(planned.get(2).getRow(), "startdate"), 150L);
    assertEquals(RowUtils.get(planned.get(2).getRow(), "enddate"), 199L);
    assertEquals(RowUtils.get(planned.get(2).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_NO);
    assertEquals(planned.get(3).getMutationType(), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(3).getRow(), "value1"), "hello1:150");
    assertEquals(RowUtils.get(planned.get(3).getRow(), "value2"), "hello2:200");
    assertEquals(RowUtils.get(planned.get(3).getRow(), "startdate"), 200L);
    assertEquals(RowUtils.get(planned.get(3).getRow(), "enddate"), EventTimeHistoryPlanner.FAR_FUTURE_MILLIS);
    assertEquals(RowUtils.get(planned.get(3).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_YES);
  }

  @Test
  public void testNoCarryForwardMultipleWhenNullOutOfOrderMultipleValuedWithPreceding() {
    p = new EventTimeHistoryPlanner();
    config = config.
        withValue(EventTimeHistoryPlanner.VALUE_FIELD_NAMES_CONFIG_NAME, ConfigValueFactory.fromAnyRef(Lists.newArrayList("value1","value2")));
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

    existing.add(new RowWithSchema(existingSchema, "a", "hello1:100", null, 100L, 100L, EventTimeHistoryPlanner.FAR_FUTURE_MILLIS, EventTimeHistoryPlanner.CURRENT_FLAG_YES, ""));
    arriving.add(new RowWithSchema(arrivingSchema, "a", null, "hello2:50", 50L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", null, "hello2:200", 200L));
    arriving.add(new RowWithSchema(arrivingSchema, "a", "hello1:150", null, 150L));
    Row key = new RowWithSchema(keySchema, "a");

    List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);

    assertEquals(planned.size(), 4);
    assertEquals(planned.get(0).getMutationType(), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "value1"), null);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "value2"), "hello2:50");
    assertEquals(RowUtils.get(planned.get(0).getRow(), "startdate"), 50L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "enddate"), 99L);
    assertEquals(RowUtils.get(planned.get(0).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_NO);
    assertEquals(planned.get(1).getMutationType(), MutationType.UPDATE);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "value1"), "hello1:100");
    assertEquals(RowUtils.get(planned.get(1).getRow(), "value2"), null);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "startdate"), 100L);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "enddate"), 149L);
    assertEquals(RowUtils.get(planned.get(1).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_NO);
    assertEquals(planned.get(2).getMutationType(), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(2).getRow(), "value1"), "hello1:150");
    assertEquals(RowUtils.get(planned.get(2).getRow(), "value2"), null);
    assertEquals(RowUtils.get(planned.get(2).getRow(), "startdate"), 150L);
    assertEquals(RowUtils.get(planned.get(2).getRow(), "enddate"), 199L);
    assertEquals(RowUtils.get(planned.get(2).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_NO);
    assertEquals(planned.get(3).getMutationType(), MutationType.INSERT);
    assertEquals(RowUtils.get(planned.get(3).getRow(), "value1"), null);
    assertEquals(RowUtils.get(planned.get(3).getRow(), "value2"), "hello2:200");
    assertEquals(RowUtils.get(planned.get(3).getRow(), "startdate"), 200L);
    assertEquals(RowUtils.get(planned.get(3).getRow(), "enddate"), EventTimeHistoryPlanner.FAR_FUTURE_MILLIS);
    assertEquals(RowUtils.get(planned.get(3).getRow(), "currentflag"), EventTimeHistoryPlanner.CURRENT_FLAG_YES);
  }
}
