/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.apache.spark.SparkConf;
import org.junit.Test;

import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestContexts {

  private static final String RESOURCES_PATH = "/spark";

  @Test
  public void testSparkPassthroughGood() {
    Config config = ConfigUtils.configFromPath(
      this.getClass().getResource(RESOURCES_PATH + "/spark-passthrough-good.conf").getPath());

    Contexts.closeSparkSession(true);
    Contexts.initialize(config, Contexts.ExecutionMode.UNIT_TEST);
    
    SparkConf sparkConf = Contexts.getSparkSession().sparkContext().getConf();

    assertTrue(sparkConf.contains("spark.driver.allowMultipleContexts"));
    assertEquals("true", sparkConf.get("spark.driver.allowMultipleContexts"));

    assertTrue(sparkConf.contains("spark.master"));
    assertEquals("local[1]", sparkConf.get("spark.master"));
  }
  
  @Test
  public void testApplicationNameProvided() {
    Properties props = new Properties();
    props.setProperty("application.name", "test");
    Config config = ConfigFactory.parseProperties(props);
    
    Contexts.closeSparkSession(true);
    Contexts.initialize(config, Contexts.ExecutionMode.UNIT_TEST);
    
    SparkConf sparkConf = Contexts.getSparkSession().sparkContext().getConf();
    
    assertEquals(sparkConf.get("spark.app.name"), "test");
  }
  
  @Test
  public void testApplicationNameNotProvided() {
    Config config = ConfigFactory.empty();
    
    Contexts.closeSparkSession(true);
    Contexts.initialize(config, Contexts.ExecutionMode.UNIT_TEST);
    
    SparkConf sparkConf = Contexts.getSparkSession().sparkContext().getConf();
    
    assertEquals(sparkConf.get("spark.app.name"), "");
  }
  
  @Test
  public void testDefaultBatchConfiguration() {
    Config config = ConfigFactory.empty();
    
    Contexts.closeSparkSession(true);
    Contexts.initialize(config, Contexts.ExecutionMode.BATCH);
    
    SparkConf sparkConf = Contexts.getSparkSession().sparkContext().getConf();
    
    assertTrue(!sparkConf.contains("spark.dynamicAllocation.enabled"));
    assertTrue(!sparkConf.contains("spark.sql.shuffle.partitions"));
    assertEquals(sparkConf.get("spark.sql.catalogImplementation"), "hive");
    
    Contexts.closeSparkSession(true);
  }
  
  @Test
  public void testDefaultStreamingConfiguration() {
    Config config = ConfigFactory.empty();
    
    Contexts.closeSparkSession(true);
    Contexts.initialize(config, Contexts.ExecutionMode.STREAMING);
    
    SparkConf sparkConf = Contexts.getSparkSession().sparkContext().getConf();
    
    assertTrue(sparkConf.contains("spark.dynamicAllocation.enabled"));
    assertTrue(sparkConf.contains("spark.sql.shuffle.partitions"));
    assertEquals(sparkConf.get("spark.sql.catalogImplementation"), "hive");
    
    Contexts.closeSparkSession(true);
  }
  
  @Test
  public void testDefaultUnitTestConfiguration() {
    Config config = ConfigFactory.empty();
    
    Contexts.closeSparkSession(true);
    Contexts.initialize(config, Contexts.ExecutionMode.UNIT_TEST);
    
    SparkConf sparkConf = Contexts.getSparkSession().sparkContext().getConf();
    
    assertEquals(sparkConf.get("spark.sql.catalogImplementation"), "in-memory");
    assertEquals(sparkConf.get("spark.sql.shuffle.partitions"), "1");
    
    Contexts.closeSparkSession(true);
  }

}
