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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestPassthroughDeriver {

  @Test
  public void testPassthrough() throws Exception {
    StructType schema = DataTypes.createStructType(Lists.<StructField>newArrayList(
        DataTypes.createStructField("col1", DataTypes.StringType, false)));
    Dataset<Row> dep1 = Contexts.getSparkSession().createDataFrame(
        Lists.newArrayList(RowFactory.create("a")), schema);
    Dataset<Row> dep2= Contexts.getSparkSession().createDataFrame(
        Lists.newArrayList(RowFactory.create("b")), schema);
    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("dep1", dep1);
    dependencies.put("dep2", dep2);

    Deriver deriver = new PassthroughDeriver();

    List<Row> result = deriver.derive(dependencies).collectAsList();

    assertTrue(result.contains(RowFactory.create("a")));
    assertTrue(result.contains(RowFactory.create("b")));
    assertEquals(2, result.size());
  }

  @Test (expected = RuntimeException.class)
  public void testDifferentSchemas() throws Exception {
    StructType schema1 = DataTypes.createStructType(Lists.<StructField>newArrayList(
        DataTypes.createStructField("col1", DataTypes.StringType, false)));
    StructType schema2 = DataTypes.createStructType(Lists.<StructField>newArrayList(
        DataTypes.createStructField("col2", DataTypes.StringType, false)));
    Dataset<Row> dep1 = Contexts.getSparkSession().createDataFrame(
        Lists.newArrayList(RowFactory.create("a")), schema1);
    Dataset<Row> dep2= Contexts.getSparkSession().createDataFrame(
        Lists.newArrayList(RowFactory.create("b")), schema2);
    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("dep1", dep1);
    dependencies.put("dep2", dep2);

    Deriver deriver = new PassthroughDeriver();

    deriver.derive(dependencies).collectAsList();
  }

}
