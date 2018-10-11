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
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.cloudera.labs.envelope.validate.ValidationAssert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Distinct Deriver Unit Test
 */
public class TestDistinctDeriver {

  @Test
  public void derive() throws Exception {
    Dataset<Row> source = createTestDataframe();
    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("df1", source);

    DistinctDeriver deriver = new DistinctDeriver();

    Config config = ConfigFactory.empty();
    assertNoValidationFailures(deriver, config);
    deriver.configure(config);
    List<Row> results = deriver.derive(dependencies).collectAsList();
    assertEquals(results.size(), 11);
    assertTrue(results.containsAll(createTestData()));

    config = ConfigFactory.empty().withValue(DistinctDeriver.DISTINCT_STEP_CONFIG,
        ConfigValueFactory.fromAnyRef("df1"));
    deriver.configure(config);
    results = deriver.derive(dependencies).collectAsList();
    assertEquals(results.size(), 11);
    assertTrue(results.containsAll(createTestData()));

    dependencies.put("df2", null);
    dependencies.put("df3", null);
    results = deriver.derive(dependencies).collectAsList();
    assertEquals(results.size(), 11);
    assertTrue(results.containsAll(createTestData()));
  }

  @Test(expected = RuntimeException.class)
  public void missingDependencies() throws Exception {
    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    Config config = ConfigFactory.empty();
    DistinctDeriver deriver = new DistinctDeriver();
    assertNoValidationFailures(deriver, config);
    deriver.configure(config);
    deriver.derive(dependencies);
  }

  @Test(expected = RuntimeException.class)
  public void missingConfig() throws Exception {
    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("df1", null);
    dependencies.put("df2", null);
    Config config = ConfigFactory.empty();
    DistinctDeriver deriver = new DistinctDeriver();
    assertNoValidationFailures(deriver, config);
    deriver.configure(config);
    deriver.derive(dependencies);
  }

  @Test(expected = RuntimeException.class)
  public void wrongConfig() throws Exception {
    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("df1", null);
    dependencies.put("df2", null);
    Config config = ConfigFactory.empty().withValue(DistinctDeriver.DISTINCT_STEP_CONFIG,
        ConfigValueFactory.fromAnyRef("df3"));
    DistinctDeriver deriver = new DistinctDeriver();
    assertNoValidationFailures(deriver, config);
    deriver.configure(config);
    deriver.derive(dependencies);
  }

  private static Dataset<Row> createTestDataframe() {
    List<Row> rows = createTestData();
    StructType schema = DataTypes
        .createStructType(Lists.newArrayList(DataTypes.createStructField("id", DataTypes.StringType, true),
            DataTypes.createStructField("descr", DataTypes.StringType, true),
            DataTypes.createStructField("value", DataTypes.IntegerType, true)));
    return Contexts.getSparkSession().createDataFrame(rows, schema);
  }

  private static List<Row> createTestData() {
    // 16 rows , 11 - unique
    return Lists.newArrayList(RowFactory.create("A", "Alfa", 1), RowFactory.create("A", "Alfa", 1),
        RowFactory.create("A", "Alfa", 1), RowFactory.create("B", "Bravo", 2), RowFactory.create("B", "Bravo", 2),
        RowFactory.create("B", "Bravo", 3), RowFactory.create("B", "Bravo", 3), RowFactory.create("B", "Bravo", 4),
        RowFactory.create("B", "Bravo", 4), RowFactory.create("C", "Charlie", 5), RowFactory.create("D", "Delta", 5),
        RowFactory.create("E", "Echo", 6), RowFactory.create("Z", "Zulu", 26), RowFactory.create("Z", "Zulu", null),
        RowFactory.create("Z", null, null), RowFactory.create(null, null, null));
  }

}