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

package com.cloudera.labs.envelope.plan.time;

import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.PlannerUtils;
import com.google.common.collect.Lists;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestLongMillisTimeModel {
  
  private TimeModel tm;
  private StructField field;
  private StructType schema;
  private Row first, second;
  
  @Before
  public void before() {
    field = DataTypes.createStructField("time", DataTypes.LongType, true);
    schema = DataTypes.createStructType(Lists.newArrayList(field));
    
    tm = new LongMillisTimeModel();
    tm.configure(ConfigFactory.empty(), Lists.newArrayList(field.name()));
    
    first = new RowWithSchema(schema, 1000L);
    second = new RowWithSchema(schema, 2000L);
  }
  
  @Test
  public void testSchema() {
    assertEquals(schema, tm.getSchema());
  }

  @Test
  public void testBefore() {
    assertTrue(PlannerUtils.before(tm, first, second));
    assertFalse(PlannerUtils.before(tm, second, first));
  }

  @Test
  public void testSimultaneous() {
    assertTrue(PlannerUtils.simultaneous(tm, first, first));
    assertFalse(PlannerUtils.simultaneous(tm, second, first));
  }

  @Test
  public void testAfter() {
    assertFalse(PlannerUtils.after(tm, first, second));
    assertTrue(PlannerUtils.after(tm, second, first));
  }

  @Test
  public void testFarFuture() {
    Row ff = tm.setFarFutureTime(first);
    
    // > 2100-01-01
    assertTrue(ff.<Long>getAs(field.name()) > 4102444800000L);
  }

  @Test
  public void testCurrentSystemTime() {
    Long currentTimeMillis = System.currentTimeMillis();
    tm.configureCurrentSystemTime(currentTimeMillis);
    Row current = tm.setCurrentSystemTime(first);
    
    assertEquals(current.<Long>getAs(field.name()), currentTimeMillis);
  }

  @Test
  public void testPrecedingSystemTime() {
    Long currentTimeMillis = System.currentTimeMillis();
    tm.configureCurrentSystemTime(currentTimeMillis);
    Row current = tm.setPrecedingSystemTime(first);
    
    assertEquals(current.<Long>getAs(field.name()), (Long)(currentTimeMillis - 1L));
  }

  @Test
  public void testAppendFields() {
    StructType withoutSchema = DataTypes.createStructType(
        Lists.newArrayList(
            DataTypes.createStructField("other", DataTypes.StringType, true)));
    
    Row without = new RowWithSchema(withoutSchema, "hello");
    Row with = tm.appendFields(without);
    
    assertEquals(with.schema(), withoutSchema.add(field));
  }
  
}
