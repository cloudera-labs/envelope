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
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import static com.cloudera.labs.envelope.validate.ValidationAssert.assertNoValidationFailures;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestStringDateTimeModel {

  private StringDateTimeModel tm;
  private StructField field;
  private StructType schema;
  private Row first, second;
  private DateFormat format;
  private Config config;
  
  @Before
  public void before() {
    field = DataTypes.createStructField("time", DataTypes.StringType, true);
    schema = DataTypes.createStructType(Lists.newArrayList(field));
    
    tm = new StringDateTimeModel();
    config = ConfigFactory.empty();
    assertNoValidationFailures(tm, config);
    tm.configure(config, Lists.newArrayList(field.name()));
    
    first = new RowWithSchema(schema, "2017-12-31");
    second = new RowWithSchema(schema, "2018-01-01");
    
    format = new SimpleDateFormat("yyyy-MM-dd");
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
  public void testFarFuture() throws ParseException {
    Row ff = tm.setFarFutureTime(first);
    
    assertTrue(format.parse(ff.<String>getAs(field.name())).after(new Date(4102444800000L)));
  }

  @Test
  public void testCurrentSystemTime() throws ParseException {
    Long currentTimeMillis = System.currentTimeMillis();
    tm.configureCurrentSystemTime(currentTimeMillis);
    Row current = tm.setCurrentSystemTime(first);
    
    assertEquals(current.<String>getAs(field.name()), format.format(new Date(currentTimeMillis)));
  }

  @Test
  public void testPrecedingSystemTime() {
    Long currentTimeMillis = System.currentTimeMillis();
    tm.configureCurrentSystemTime(currentTimeMillis);
    Row current = tm.setPrecedingSystemTime(second);
    Date currentDate = new Date(currentTimeMillis);
    Calendar cal = Calendar.getInstance();
    cal.setTime(currentDate);
    cal.add(Calendar.DATE, -1);
    
    assertEquals(current.<String>getAs(field.name()), format.format(cal.getTime()));
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
  
  @Test
  public void testCustomFormat() throws ParseException {
    format = new SimpleDateFormat("dd-MMM-yyyy");
    config = config.withValue(StringDateTimeModel.DATETIME_FORMAT_CONFIG,
        ConfigValueFactory.fromAnyRef("dd-MMM-yyyy"));
    assertNoValidationFailures(tm, config);
    tm.configure(config, Lists.newArrayList(field.name()));
    
    first = new RowWithSchema(schema, "31-DEC-2017");
    second = new RowWithSchema(schema, "01-JAN-2018");
    
    testSchema();
    testBefore();
    testSimultaneous();
    testAfter();
    testFarFuture();
    testCurrentSystemTime();
    testPrecedingSystemTime();
    testAppendFields();
  }
  
}
