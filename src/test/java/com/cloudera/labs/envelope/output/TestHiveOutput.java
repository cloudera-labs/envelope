/**
 * Copyright © 2016-2017 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.output;

import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.spark.Contexts;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import mockit.Deencapsulation;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import scala.Tuple2;

/**
 *
 */
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

  @Test (expected = RuntimeException.class)
  public void configureNoTable() {
    Map<String, Object> paramMap = new HashMap<>();
    config = ConfigFactory.parseMap(paramMap);

    HiveOutput hiveOutput = new HiveOutput();
    hiveOutput.configure(config);
  }

  @Test
  public void configure() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(HiveOutput.TABLE_CONFIG, "foo");
    config = ConfigFactory.parseMap(paramMap);

    HiveOutput hiveOutput = new HiveOutput();
    hiveOutput.configure(config);
  }

  @Test
  public void configureLocation() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(HiveOutput.TABLE_CONFIG, "foo");
    paramMap.put(HiveOutput.LOCATION_CONFIG, "bar");
    config = ConfigFactory.parseMap(paramMap);

    HiveOutput hiveOutput = new HiveOutput();
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
    hiveOutput.configure(config);

    Map<String, String> optionsMap = Deencapsulation.getField(hiveOutput, "options");
    assertNotNull("Options are null", optionsMap);
    assertEquals("Invalid option", "badabing", optionsMap.get("hive-option"));
  }

  @Ignore ("Needs review of Hive temporary folder creation")
  @Test
  public void applyBulkMutations() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(HiveOutput.TABLE_CONFIG, "default");
    config = ConfigFactory.parseMap(paramMap);

    HiveOutput hiveOutput = new HiveOutput();
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