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
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.cloudera.labs.envelope.validate.ValidationAssert.assertNoValidationFailures;
import static com.cloudera.labs.envelope.validate.ValidationAssert.assertValidationFailures;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 *
 */
public class TestExcludeDeriver {
  private Config config;

  @Test
  public void missingDatasets() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    config = ConfigFactory.parseMap(paramMap);

    ExcludeDeriver excludeDeriver = new ExcludeDeriver();
    assertValidationFailures(excludeDeriver, config);
  }

  @Test
  public void missingCompare() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(ExcludeDeriver.EXCLUSION_WITH_CONFIG, "New");
    config = ConfigFactory.parseMap(paramMap);

    ExcludeDeriver excludeDeriver = new ExcludeDeriver();
    assertValidationFailures(excludeDeriver, config);
  }

  @Test
  public void missingReference() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(ExcludeDeriver.EXCLUSION_COMPARE_CONFIG, "New");
    config = ConfigFactory.parseMap(paramMap);

    ExcludeDeriver excludeDeriver = new ExcludeDeriver();
    assertValidationFailures(excludeDeriver, config);
  }

  @Test
  public void missingFields() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(ExcludeDeriver.EXCLUSION_COMPARE_CONFIG, "Compare");
    paramMap.put(ExcludeDeriver.EXCLUSION_WITH_CONFIG, "With");
    config = ConfigFactory.parseMap(paramMap);

    ExcludeDeriver excludeDeriver = new ExcludeDeriver();
    assertValidationFailures(excludeDeriver, config);
  }

  @Test (expected = RuntimeException.class)
  public void mismatchedDependencyComparison() throws Exception {
    Map<String, Dataset<Row>> dependencies = new HashMap<>();
    dependencies.put("One", Contexts.getSparkSession().emptyDataFrame());
    dependencies.put("With", Contexts.getSparkSession().emptyDataFrame());

    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(ExcludeDeriver.EXCLUSION_COMPARE_CONFIG, "Compare");
    paramMap.put(ExcludeDeriver.EXCLUSION_WITH_CONFIG, "With");
    paramMap.put(ExcludeDeriver.EXCLUSION_FIELDS_CONFIG, Lists.newArrayList("field1", "field2"));
    config = ConfigFactory.parseMap(paramMap);

    ExcludeDeriver excludeDeriver = new ExcludeDeriver();
    assertNoValidationFailures(excludeDeriver, config);
    excludeDeriver.configure(config);

    excludeDeriver.derive(dependencies);
  }

  @Test (expected = RuntimeException.class)
  public void mismatchedDependencyReference() throws Exception {
    Map<String, Dataset<Row>> dependencies = new HashMap<>();
    dependencies.put("Compare", Contexts.getSparkSession().emptyDataFrame());
    dependencies.put("Two", Contexts.getSparkSession().emptyDataFrame());

    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(ExcludeDeriver.EXCLUSION_COMPARE_CONFIG, "Compare");
    paramMap.put(ExcludeDeriver.EXCLUSION_WITH_CONFIG, "With");
    paramMap.put(ExcludeDeriver.EXCLUSION_FIELDS_CONFIG, Lists.newArrayList("field1", "field2"));
    config = ConfigFactory.parseMap(paramMap);

    ExcludeDeriver excludeDeriver = new ExcludeDeriver();
    assertNoValidationFailures(excludeDeriver, config);
    excludeDeriver.configure(config);

    excludeDeriver.derive(dependencies);
  }

  @Test
  public void derive() throws Exception {
    StructType existingSchema = DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("field1", DataTypes.IntegerType, true),
        DataTypes.createStructField("field2", DataTypes.StringType, true))
    );

    List<Row> existingRows = Lists.newArrayList();
    existingRows.add(RowFactory.create(1000, "Envelopes"));
    existingRows.add(RowFactory.create(1001, "Stamps"));

    Dataset<Row> existingDF = Contexts.getSparkSession().createDataFrame(existingRows, existingSchema);

    StructType newSchema = DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("field1", DataTypes.IntegerType, true),
        DataTypes.createStructField("field2", DataTypes.StringType, true),
        DataTypes.createStructField("field3", DataTypes.StringType, true)
        )
    );

    List<Row> newRows= Lists.newArrayList();
    newRows.add(RowFactory.create(1000, "Envelopes", "Nope"));
    newRows.add(RowFactory.create(1001, "Stamps", "Nope"));
    newRows.add(RowFactory.create(1000, "Stamps", "Yep"));
    newRows.add(RowFactory.create(1000, "Staplers", "Yep"));
    newRows.add(RowFactory.create(1001, "Envelopes", "Yep"));

    Dataset<Row> newDF = Contexts.getSparkSession().createDataFrame(newRows, newSchema);

    Map<String, Dataset<Row>> dependencies = new HashMap<>();
    dependencies.put("New", newDF);
    dependencies.put("Existing", existingDF);

    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(ExcludeDeriver.EXCLUSION_COMPARE_CONFIG, "New");
    paramMap.put(ExcludeDeriver.EXCLUSION_WITH_CONFIG, "Existing");
    paramMap.put(ExcludeDeriver.EXCLUSION_FIELDS_CONFIG, Lists.newArrayList("field1", "field2"));
    config = ConfigFactory.parseMap(paramMap);

    ExcludeDeriver excludeDeriver = new ExcludeDeriver();
    assertNoValidationFailures(excludeDeriver, config);
    excludeDeriver.configure(config);

    Dataset<Row> results = excludeDeriver.derive(dependencies);

    assertNotNull("Results is null", results);
    assertNotNull("Invalid schema", results.schema());

    assertEquals("Invalid schema field count", 3, results.schema().fieldNames().length);
    assertEquals("Invalid schema field name", "field3", results.schema().fieldNames()[2]);
    assertEquals("Invalid schema field name", DataTypes.StringType, results.schema().fields()[2].dataType());

    assertEquals("Invalid row count", 3, results.count());
  }
}