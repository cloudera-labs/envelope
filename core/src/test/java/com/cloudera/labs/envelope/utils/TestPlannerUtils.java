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

package com.cloudera.labs.envelope.utils;

import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.plan.time.TimeModel;
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestPlannerUtils {
  
  StructType schemaWithMT, schemaWithoutMT, schemaWithTMs;
  TimeModel firstTM, secondTM;
  
  @Before
  public void before() {
    schemaWithoutMT = DataTypes.createStructType(Lists.newArrayList(DataTypes.createStructField("other", DataTypes.StringType, true)));
    schemaWithMT = schemaWithoutMT.add(DataTypes.createStructField(MutationType.MUTATION_TYPE_FIELD_NAME, DataTypes.StringType, true));
    schemaWithTMs = DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("first", DataTypes.StringType, true),
        DataTypes.createStructField("second", DataTypes.StringType, true)));
    
    firstTM = new DummyTimeModel();
    secondTM = new DummyTimeModel();
    firstTM.configure(ConfigFactory.empty(), Lists.newArrayList("first"));
    secondTM.configure(ConfigFactory.empty(), Lists.newArrayList("second"));
  }

  @Test
  public void testSetMutationType() {
    Row rowWith = new RowWithSchema(schemaWithMT, "hello", null);
    rowWith = PlannerUtils.setMutationType(rowWith, MutationType.DELETE);
    
    assertEquals(rowWith.<String>getAs(MutationType.MUTATION_TYPE_FIELD_NAME), MutationType.DELETE.toString());
    
    Row rowWithout = new RowWithSchema(schemaWithoutMT, "hello");
    rowWithout = PlannerUtils.setMutationType(rowWithout, MutationType.DELETE);
    
    assertEquals(rowWithout.<String>getAs(MutationType.MUTATION_TYPE_FIELD_NAME), MutationType.DELETE.toString());
  }

  @Test
  public void testGetMutationType() {
    Row row = new RowWithSchema(schemaWithMT, "hello", MutationType.DELETE.toString());
    
    assertEquals(MutationType.DELETE, PlannerUtils.getMutationType(row));
  }

  @Test
  public void testAppendMutationTypeField() {
    Row rowWithout = new RowWithSchema(schemaWithoutMT, "hello");
    Row rowWith = PlannerUtils.appendMutationTypeField(rowWithout);
    
    assertEquals(rowWith.schema(), schemaWithMT);
  }

  @Test
  public void testRemoveMutationTypeField() {
    Row rowWith = new RowWithSchema(schemaWithMT, "hello", MutationType.DELETE.toString());
    Row rowWithout = PlannerUtils.removeMutationTypeField(rowWith);
    
    assertEquals(rowWithout.schema(), schemaWithoutMT);
  }

  @Test
  public void testHasMutationTypeField() {
    Row rowWith = new RowWithSchema(schemaWithMT, "hello", MutationType.DELETE.toString());
    Row rowWithout = PlannerUtils.removeMutationTypeField(rowWith);
    
    assertTrue(PlannerUtils.hasMutationTypeField(rowWith));
    assertFalse(PlannerUtils.hasMutationTypeField(rowWithout));
  }

  @Test
  public void testCopyTime() {
    Row row = new RowWithSchema(schemaWithTMs, 1000L, null);
    Row copied = PlannerUtils.copyTime(row, firstTM, row, secondTM);
    
    assertEquals(copied.<Long>getAs("second"), (Long)1000L);
  }

  @Test
  public void testCopyPrecedingTime() {
    Row row = new RowWithSchema(schemaWithTMs, 1000L, null);
    Row copied = PlannerUtils.copyPrecedingTime(row, firstTM, row, secondTM);
    
    assertEquals(copied.<Long>getAs("second"), (Long)999L);
  }
  
  private static class DummyTimeModel implements TimeModel {
    private StructField field;

    @Override
    public void configure(Config config, List<String> fieldNames) {
      this.field = DataTypes.createStructField(fieldNames.get(0), DataTypes.LongType, true);
    }

    @Override
    public void configureCurrentSystemTime(long currentSystemTimeMillis) {
    }

    @Override
    public int compare(Row first, Row second) {
      return 0;
    }

    @Override
    public Row setCurrentSystemTime(Row row) {
      return row;
    }

    @Override
    public Row setPrecedingSystemTime(Row row) {
      return row;
    }

    @Override
    public StructType getSchema() {
      return DataTypes.createStructType(Lists.newArrayList(field));
    }

    @Override
    public Row setFarFutureTime(Row row) {
      return row;
    }

    @Override
    public Row appendFields(Row row) {
      return row;
    }

    @Override
    public Row getTime(Row row) {
      return new RowWithSchema(getSchema(), RowUtils.get(row, field.name()));
    }

    @Override
    public Row getPrecedingTime(Row row) {
      return new RowWithSchema(getSchema(), row.<Long>getAs(field.name()) - 1);
    }
  }
  
}
