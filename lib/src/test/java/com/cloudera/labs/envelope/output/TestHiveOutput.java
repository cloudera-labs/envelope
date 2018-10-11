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

package com.cloudera.labs.envelope.output;

import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import mockit.Deencapsulation;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.cloudera.labs.envelope.validate.ValidationAssert.assertNoValidationFailures;
import static com.cloudera.labs.envelope.validate.ValidationAssert.assertValidationFailures;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestHiveOutput {

  private static String HIVE_DATA = "/hive/sample-hive.json";

  private static HiveMetaStoreClient metaStoreClient;

  private Config config;

  public ArrayList<Tuple2<MutationType, Dataset<Row>>> appendPlannerSetup() throws IOException {
    ArrayList<Tuple2<MutationType, Dataset<Row>>> plannedRows = new ArrayList<>();
    Dataset<Row> rowDataset = Contexts.getSparkSession().read().json(
        TestFileSystemOutput.class.getResource(HIVE_DATA).getPath());
    Tuple2<MutationType, Dataset<Row>> input = new Tuple2<>(MutationType.INSERT, rowDataset);
    plannedRows.add(input);
    return plannedRows;
  }

  @Ignore ("Needs review of Hive temporary folder creation")
  @BeforeClass
  public static void setupMetastore() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(Contexts.SPARK_CONF_PROPERTY_PREFIX + "hive.metastore.warehouse.dir", "target/spark-warehouse");
    Contexts.initialize(ConfigFactory.parseMap(paramMap), Contexts.ExecutionMode.BATCH);
  }

  @After
  public void teardown() {
    config = null;
  }

  @Test
  public void configureNoTable() {
    Map<String, Object> paramMap = new HashMap<>();
    config = ConfigFactory.parseMap(paramMap);

    HiveOutput hiveOutput = new HiveOutput();
    assertValidationFailures(hiveOutput, config);
  }

  @Test
  public void configure() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(HiveOutput.TABLE_CONFIG, "foo");
    config = ConfigFactory.parseMap(paramMap);

    HiveOutput hiveOutput = new HiveOutput();
    assertNoValidationFailures(hiveOutput, config);
    hiveOutput.configure(config);
  }

  @Test
  public void configureLocation() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(HiveOutput.TABLE_CONFIG, "foo");
    paramMap.put(HiveOutput.LOCATION_CONFIG, "bar");
    config = ConfigFactory.parseMap(paramMap);

    HiveOutput hiveOutput = new HiveOutput();
    assertNoValidationFailures(hiveOutput, config);
    hiveOutput.configure(config);

    Map<String, String> optionsMap = Deencapsulation.getField(hiveOutput, "options");
    assertNotNull("Options are null", optionsMap);
    assertEquals("Invalid option", "bar", optionsMap.get("path"));
  }

  @Test
  public void configureOtherOptions() throws Exception {
    Map<String, Object> otherMap = new HashMap<>();
    otherMap.put("hive-option", "badabing");

    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(HiveOutput.TABLE_CONFIG, "foo");
    paramMap.put(HiveOutput.OPTIONS_CONFIG, otherMap);
    config = ConfigFactory.parseMap(paramMap);

    HiveOutput hiveOutput = new HiveOutput();
    assertNoValidationFailures(hiveOutput, config);
    hiveOutput.configure(config);

    Map<String, String> optionsMap = Deencapsulation.getField(hiveOutput, "options");
    assertNotNull("Options are null", optionsMap);
    assertEquals("Invalid option", "badabing", optionsMap.get("hive-option"));
  }

  @Test
  public void alignColumns() throws Exception {
    SparkSession spark = Contexts.getSparkSession();
    String targetTable = "temp_target_table";
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(HiveOutput.TABLE_CONFIG, targetTable);
    config = ConfigFactory.parseMap(paramMap);
    HiveOutput hiveOutput = new HiveOutput();
    assertNoValidationFailures(hiveOutput, config);
    hiveOutput.configure(config);

    StructType targetSchema = RowUtils.structTypeFor(
        Lists.newArrayList("zip_code", "city", "state", "Fname", "lname"), 
        Lists.newArrayList("int", "string", "string", "string", "string"));
    spark.createDataFrame(spark.emptyDataFrame().rdd(), targetSchema)
         .createOrReplaceTempView(targetTable);

    String stepDef = "SELECT \"foo\" AS fname, \"bar\" AS lname, 18 AS age, 12345 AS ZIP_CODE";
    Dataset<Row> stepDataframe = spark.sql(stepDef);

    Dataset<Row> alignedDataframe =  hiveOutput.alignColumns(stepDataframe);

    Row alignedData = alignedDataframe.first();
    String expectedDef = "SELECT 12345 AS zip_code, NULL as city, NULL as state, " +
                         "\"foo\" AS fname, \"bar\" AS lname";
    Row expectedData = spark.sql(expectedDef).first();

    assertEquals(expectedData.size(), alignedData.size());
    for (int i = 0; i < alignedData.length(); i++) {
      assertEquals(alignedData.get(i), expectedData.get(i));
    }
    assertEquals(Arrays.asList(alignedData.schema().fieldNames()), 
                 Arrays.asList(expectedData.schema().fieldNames()));
  }

  @Ignore ("Needs review of Hive temporary folder creation")
  @Test
  public void applyBulkMutations() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(HiveOutput.TABLE_CONFIG, "default");
    config = ConfigFactory.parseMap(paramMap);

    HiveOutput hiveOutput = new HiveOutput();
    assertNoValidationFailures(hiveOutput, config);
    hiveOutput.configure(config);
    
    hiveOutput.applyBulkMutations(appendPlannerSetup());
  }

  @Test
  public void getSupportedBulkMutationTypes() throws Exception {
    HiveOutput hiveOutput = new HiveOutput();
    Set<MutationType> mutationTypes = hiveOutput.getSupportedBulkMutationTypes();

    assertEquals("Invalid number of mutation types", 2, mutationTypes.size());
    assertTrue("INSERT not supported", mutationTypes.contains(MutationType.INSERT));
    assertTrue("OVERWRITE not supported", mutationTypes.contains(MutationType.OVERWRITE));
  }

}
