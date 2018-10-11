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

package com.cloudera.labs.envelope.derive.dq;

import com.cloudera.labs.envelope.derive.DataQualityDeriver;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import scala.collection.JavaConverters;

import java.util.List;
import java.util.Map;

import static com.cloudera.labs.envelope.validate.ValidationAssert.assertNoValidationFailures;
import static com.cloudera.labs.envelope.validate.ValidationAssert.assertValidationFailures;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestDataQualityDeriver {

  private static final StructType SCHEMA = new StructType(new StructField[] {
      new StructField("name", DataTypes.StringType, false, Metadata.empty()),
      new StructField("address", DataTypes.StringType, false, Metadata.empty()),
      new StructField("age", DataTypes.IntegerType, false, Metadata.empty())
  });

  private static final StructType DQ_SCHEMA = new StructType(new StructField[] {
      new StructField("count", DataTypes.LongType, false, Metadata.empty())
  });

  @Test
  public void testGoodDatasetConfig() {
    Config config = ConfigUtils.configFromResource("/dq/dq-dataset-good.conf").getConfig("steps.checkmydata");

    assertTrue("Config for step should declare dependencies", config.hasPath("dependencies"));
    assertEquals("Should be two step dependencies", 2, config.getStringList("dependencies").size());

    assertEquals("Step should have DQ deriver", "dq", config.getString("deriver.type"));

    DataQualityDeriver dq = new DataQualityDeriver();
    assertNoValidationFailures(dq, config.getConfig("deriver"));
    dq.configure(config.getConfig("deriver"));
  }

  @Test
  public void testBadConfig() {
    Config config = ConfigUtils.configFromResource("/dq/dq-dataset-bad.conf").getConfig("steps.checkmydata");

    assertTrue("Config for step should declare dependencies", config.hasPath("dependencies"));
    assertEquals("Should be two step dependencies", 2, config.getStringList("dependencies").size());

    assertEquals("Step should have DQ deriver", "dq", config.getString("deriver.type"));

    DataQualityDeriver dq = new DataQualityDeriver();
    assertValidationFailures(dq, config.getConfig("deriver"));
  }

  @Test
  public void testRowLevelRules() throws Exception {
    Config config = ConfigUtils.configFromResource("/dq/dq-dataset-good.conf").getConfig("steps.checkrows");

    SparkSession sparkSession = Contexts.getSparkSession();

    List<Row> dataList = Lists.newArrayList(
        new RowWithSchema(SCHEMA, "Apple", "One Infinite Loop", 151),
        (Row)new RowWithSchema(SCHEMA, "Microsoft", "One Microsoft Way", -1)
    );
    Dataset<Row> mydata = sparkSession.createDataFrame(dataList, SCHEMA);

    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("mydata", mydata);

    DataQualityDeriver dq = new DataQualityDeriver();
    assertNoValidationFailures(dq, config.getConfig("deriver"));
    dq.configure(config.getConfig("deriver"));
    Dataset<Row> dqResults = dq.derive(dependencies);
    List<Row> dqRows = dqResults.collectAsList();

    assertEquals("Should be two rows", 2, dqRows.size());
    for (Row row : dqRows) {
      scala.collection.immutable.Map<String, Boolean> scalaResults = row.getAs("results");
      Map<String, Boolean> ruleResults = fromScalaMap(scalaResults);
      assertEquals("Rule results map should have three entries", 3, ruleResults.size());
      assertTrue("Checkfields should pass", ruleResults.get("r1"));
      assertTrue("Regex should pass", ruleResults.get("r2"));
      assertFalse("Range should fail", ruleResults.get("r3"));
    }
  }

  @Test
  public void testDatasetLevelRules() throws Exception {
    Config config = ConfigUtils.configFromResource("/dq/dq-dataset-good.conf").getConfig("steps.checkmydata");

    SparkSession sparkSession = Contexts.getSparkSession();

    List<Row> dataList = Lists.newArrayList(
        new RowWithSchema(SCHEMA, "Apple", "One Infinite Loop", 40),
        (Row)new RowWithSchema(SCHEMA, "Microsoft", "One Microsoft Way", 42)
    );
    Dataset<Row> mydata = sparkSession.createDataFrame(dataList, SCHEMA);
    List<Row> dqparamsList = Lists.newArrayList((Row) new RowWithSchema(DQ_SCHEMA,2l));
    Dataset<Row> dqparams = sparkSession.createDataFrame(dqparamsList, DQ_SCHEMA);

    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("mydata", mydata);
    dependencies.put("dqparams", dqparams);

    DataQualityDeriver dq = new DataQualityDeriver();
    assertNoValidationFailures(dq, config.getConfig("deriver"));
    dq.configure(config.getConfig("deriver"));
    Dataset<Row> dqResults = dq.derive(dependencies);
    List<Row> dqRows = dqResults.collectAsList();

    assertEquals("Should be results from six rules", 6, dqRows.size());
    Map<String, Row> results = Maps.newHashMap();
    for (Row row : dqRows) {
      results.put(row.<String>getAs("name"), row);
    }
    assertEquals("Should be results from four different rules", 6, results.size());

    // Check count
    assertTrue("Count should have passed", results.get("r1").<Boolean>getAs("result"));
    assertTrue("Checknulls should have passed", results.get("r2").<Boolean>getAs("result"));
    assertTrue("Regex should have passed", results.get("r3").<Boolean>getAs("result"));
    assertFalse("Enum should not have passed", results.get("r4").<Boolean>getAs("result"));
    assertTrue("Checkschema should have passed", results.get("r5").<Boolean>getAs("result"));
    assertFalse("Checkschema should not have passed", results.get("r6").<Boolean>getAs("result"));
  }

  private static <A,B> java.util.Map<A,B> fromScalaMap(scala.collection.immutable.Map<A,B> sMap) {
    return JavaConverters.mapAsJavaMapConverter(sMap).asJava();
  }

}
