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

package com.cloudera.labs.envelope.run;

import com.cloudera.labs.envelope.input.InputFactory;
import com.cloudera.labs.envelope.spark.Contexts;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import java.util.Map;

import static com.cloudera.labs.envelope.validate.ValidationAssert.assertValidationFailures;
import static org.junit.Assert.assertEquals;

public class TestBatchStep {

  @Test
  public void testInputRepartition() throws Exception {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(DataStep.INPUT_TYPE + "." + InputFactory.TYPE_CONFIG_NAME, DummyInput.class.getName());
    configMap.put(DataStep.INPUT_TYPE + "." + "starting.partitions", 5);
    configMap.put(BatchStep.REPARTITION_NUM_PARTITIONS_PROPERTY, 10);
    Config config = ConfigFactory.parseMap(configMap);
    
    BatchStep batchStep = new BatchStep("test");
    batchStep.configure(config);
    batchStep.submit(Sets.<Step>newHashSet());
    Dataset<Row> df = batchStep.getData();
    int numPartitions = df.javaRDD().getNumPartitions(); 
    
    assertEquals(numPartitions, 10);
  }

  @Test
  public void testInputRepartitionColumns() throws Exception {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(DataStep.INPUT_TYPE + "." + InputFactory.TYPE_CONFIG_NAME, DummyInput.class.getName());
    configMap.put(DataStep.INPUT_TYPE + "." + "starting.partitions", 10);
    configMap.put(BatchStep.REPARTITION_COLUMNS_PROPERTY, Lists.newArrayList("modulo"));
    Config config = ConfigFactory.parseMap(configMap);

    BatchStep batchStep = new BatchStep("test");
    batchStep.configure(config);
    batchStep.submit(Sets.<Step>newHashSet());
    Dataset<Row> df = batchStep.getData();

    int numPartitions = df.javaRDD().getNumPartitions();
    assertEquals(Contexts.getSparkSession().sqlContext().getConf("spark.sql.shuffle.partitions"),
        Integer.toString(numPartitions));
  }

  @Test
  public void testInputRepartitionColumnsAndPartitionCount() throws Exception {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(BatchStep.REPARTITION_COLUMNS_PROPERTY, Lists.newArrayList("modulo"));
    configMap.put(BatchStep.REPARTITION_NUM_PARTITIONS_PROPERTY, 5);
    configMap.put(DataStep.INPUT_TYPE + "." + InputFactory.TYPE_CONFIG_NAME, DummyInput.class.getName());
    configMap.put(DataStep.INPUT_TYPE + "." + "starting.partitions", 10);
    Config config = ConfigFactory.parseMap(configMap);

    BatchStep batchStep = new BatchStep("test");
    batchStep.configure(config);
    batchStep.submit(Sets.<Step>newHashSet());
    Dataset<Row> df = batchStep.getData();

    int numPartitions = df.javaRDD().getNumPartitions();
    assertEquals(5, numPartitions);
  }

  @Test (expected = AnalysisException.class)
  public void testInputRepartitionInvalidColumn() throws Exception {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(DataStep.INPUT_TYPE + "." + InputFactory.TYPE_CONFIG_NAME, DummyInput.class.getName());
    configMap.put(DataStep.INPUT_TYPE + "." + "starting.partitions", 10);
    configMap.put(BatchStep.REPARTITION_COLUMNS_PROPERTY, Lists.newArrayList("modulo == 0"));
    configMap.put(BatchStep.REPARTITION_NUM_PARTITIONS_PROPERTY, 5);
    Config config = ConfigFactory.parseMap(configMap);

    BatchStep batchStep = new BatchStep("test");
    batchStep.configure(config);
    batchStep.submit(Sets.<Step>newHashSet());
    batchStep.getData();
  }
  
  @Test
  public void testInputCoalesce() throws Exception {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(DataStep.INPUT_TYPE + "." + InputFactory.TYPE_CONFIG_NAME, DummyInput.class.getName());
    configMap.put(DataStep.INPUT_TYPE + "." + "starting.partitions", 10);
    configMap.put(BatchStep.COALESCE_NUM_PARTITIONS_PROPERTY, 5);
    Config config = ConfigFactory.parseMap(configMap);
    
    BatchStep batchStep = new BatchStep("test");
    batchStep.configure(config);
    batchStep.submit(Sets.<Step>newHashSet());
    Dataset<Row> df = batchStep.getData();
    int numPartitions = df.javaRDD().getNumPartitions(); 
    
    assertEquals(numPartitions, 5);
  }

  @Test
  public void testCantRepartitionAndCoalesceInputAtOnce() {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(DataStep.INPUT_TYPE + "." + InputFactory.TYPE_CONFIG_NAME, DummyInput.class.getName());
    configMap.put(DataStep.INPUT_TYPE + "." + "starting.partitions", 5);
    configMap.put(BatchStep.REPARTITION_NUM_PARTITIONS_PROPERTY, 10);
    configMap.put(BatchStep.COALESCE_NUM_PARTITIONS_PROPERTY, 3);
    Config config = ConfigFactory.parseMap(configMap);

    BatchStep batchStep = new BatchStep("test");
    assertValidationFailures(batchStep, config);
  }

}
