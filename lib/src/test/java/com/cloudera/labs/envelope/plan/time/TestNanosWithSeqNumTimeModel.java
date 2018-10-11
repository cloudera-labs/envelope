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

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestNanosWithSeqNumTimeModel {

  private TimeModel tm;
  private StructField nanoField;
  private StructField seqNumField;
  private StructType schema;
  private Row first, second, third;
  
  @Before
  public void before() {
    nanoField = DataTypes.createStructField("nanos", DataTypes.createDecimalType(38, 0), true);
    seqNumField = DataTypes.createStructField("seqnum", DataTypes.IntegerType, true);
    schema = DataTypes.createStructType(Lists.newArrayList(nanoField, seqNumField));
    
    tm = new NanosWithSeqNumTimeModel();
    tm.configure(ConfigFactory.empty(), Lists.newArrayList(nanoField.name(), seqNumField.name()));
    
    first = new RowWithSchema(schema, new BigDecimal(1000), 10);
    second = new RowWithSchema(schema, new BigDecimal(1000), 11);
    third = new RowWithSchema(schema, new BigDecimal(1001), 1);
  }
  
  @Test
  public void testSchema() {
    assertEquals(schema, tm.getSchema());
  }

  @Test
  public void testBefore() {
    assertTrue(PlannerUtils.before(tm, first, second));
    assertTrue(PlannerUtils.before(tm, first, third));
    assertTrue(PlannerUtils.before(tm, second, third));
    assertFalse(PlannerUtils.before(tm, second, first));
    assertFalse(PlannerUtils.before(tm, third, second));
    assertFalse(PlannerUtils.before(tm, third, first));
  }

  @Test
  public void testSimultaneous() {
    assertTrue(PlannerUtils.simultaneous(tm, first, first));
    assertFalse(PlannerUtils.simultaneous(tm, first, second));
    assertFalse(PlannerUtils.simultaneous(tm, first, third));
    assertFalse(PlannerUtils.simultaneous(tm, second, third));
  }

  @Test
  public void testAfter() {
    assertFalse(PlannerUtils.after(tm, first, second));
    assertFalse(PlannerUtils.after(tm, first, third));
    assertFalse(PlannerUtils.after(tm, second, third));
    assertTrue(PlannerUtils.after(tm, second, first));
    assertTrue(PlannerUtils.after(tm, third, second));
    assertTrue(PlannerUtils.after(tm, third, first));
  }

  @Test
  public void testFarFuture() {
    Row ff = tm.setFarFutureTime(first);
    
    // > 2100-01-01
    assertTrue(ff.<BigDecimal>getAs(nanoField.name()).compareTo(new BigDecimal("4102444800000000000")) > 0);
  }

  @Test
  public void testCurrentSystemTime() {
    Long currentTimeMillis = System.currentTimeMillis();
    tm.configureCurrentSystemTime(currentTimeMillis);
    Row current = tm.setCurrentSystemTime(first);
    
    assertEquals(current.<BigDecimal>getAs(nanoField.name()), 
        new BigDecimal(currentTimeMillis).multiply(new BigDecimal(1000 * 1000)));
    assertEquals(current.<Integer>getAs(seqNumField.name()), (Integer)1); 
  }

  @Test
  public void testPrecedingSystemTime() {
    Long currentTimeMillis = System.currentTimeMillis();
    tm.configureCurrentSystemTime(currentTimeMillis);
    Row preceding = tm.setPrecedingSystemTime(first);
    
    assertEquals(preceding.<BigDecimal>getAs(nanoField.name()), 
        new BigDecimal(currentTimeMillis).multiply(new BigDecimal(1000 * 1000)).subtract(BigDecimal.ONE));
    assertEquals(preceding.<Integer>getAs(seqNumField.name()), (Integer)Integer.MAX_VALUE); 
  }

  @Test
  public void testAppendFields() {
    StructType withoutSchema = DataTypes.createStructType(
        Lists.newArrayList(
            DataTypes.createStructField("other", DataTypes.StringType, true)));
    
    Row without = new RowWithSchema(withoutSchema, "hello");
    Row with = tm.appendFields(without);
    
    assertEquals(with.schema(), withoutSchema.add(nanoField).add(seqNumField));
  }
  
}
