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
package com.cloudera.labs.envelope.run;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import com.cloudera.labs.envelope.derive.PassthroughDeriver;
import com.cloudera.labs.envelope.spark.Contexts;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestBatchStep {

  @Test
  public void testInputRepartition() throws Exception {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put("input.type", DummyInput.class.getName());
    configMap.put("input.starting.partitions", 5);
    configMap.put("input." + BatchStep.REPARTITION_NUM_PARTITIONS_PROPERTY, 10);
    Config config = ConfigFactory.parseMap(configMap);
    
    BatchStep batchStep = new BatchStep("test", config);
    batchStep.submit(Sets.<Step>newHashSet());
    Dataset<Row> df = batchStep.getData();
    int numPartitions = df.javaRDD().getNumPartitions(); 
    
    assertEquals(numPartitions, 10);
  }

  @Test
  public void testInputRepartitionColumns() throws Exception {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put("input.type", DummyInput.class.getName());
    configMap.put("input.starting.partitions", 10);
    configMap.put("input." + BatchStep.REPARTITION_COLUMNS_PROPERTY, Lists.newArrayList("modulo"));
    Config config = ConfigFactory.parseMap(configMap);

    BatchStep batchStep = new BatchStep("test", config);
    batchStep.submit(Sets.<Step>newHashSet());
    Dataset<Row> df = batchStep.getData();

    int numPartitions = df.javaRDD().getNumPartitions();
    assertEquals(Contexts.getSparkSession().sqlContext().getConf("spark.sql.shuffle.partitions"),
        Integer.toString(numPartitions));
  }

  @Test
  public void testInputRepartitionColumnsAndPartitionCount() throws Exception {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put("input.type", DummyInput.class.getName());
    configMap.put("input.starting.partitions", 10);
    configMap.put("input." + BatchStep.REPARTITION_COLUMNS_PROPERTY, Lists.newArrayList("modulo"));
    configMap.put("input." + BatchStep.REPARTITION_NUM_PARTITIONS_PROPERTY, 5);
    Config config = ConfigFactory.parseMap(configMap);

    BatchStep batchStep = new BatchStep("test", config);
    batchStep.submit(Sets.<Step>newHashSet());
    Dataset<Row> df = batchStep.getData();

    int numPartitions = df.javaRDD().getNumPartitions();
    assertEquals(5, numPartitions);
  }

  @Test (expected = AnalysisException.class)
  public void testInputRepartitionInvalidColumn() throws Exception {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put("input.type", DummyInput.class.getName());
    configMap.put("input.starting.partitions", 10);
    configMap.put("input." + BatchStep.REPARTITION_COLUMNS_PROPERTY, Lists.newArrayList("modulo == 0"));
    configMap.put("input." + BatchStep.REPARTITION_NUM_PARTITIONS_PROPERTY, 5);
    Config config = ConfigFactory.parseMap(configMap);

    BatchStep batchStep = new BatchStep("test", config);
    batchStep.submit(Sets.<Step>newHashSet());
    batchStep.getData();
  }
  
  @Test
  public void testInputCoalesce() throws Exception {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put("input.type", DummyInput.class.getName());
    configMap.put("input.starting.partitions", 10);
    configMap.put("input.repartition.partitions", 5);
    Config config = ConfigFactory.parseMap(configMap);
    
    BatchStep batchStep = new BatchStep("test", config);
    batchStep.submit(Sets.<Step>newHashSet());
    Dataset<Row> df = batchStep.getData();
    int numPartitions = df.javaRDD().getNumPartitions(); 
    
    assertEquals(numPartitions, 5);
  }
  
  @Test
  public void testDeriverRepartition() throws Exception {
    Map<String, Object> dependencyConfigMap = Maps.newHashMap();
    dependencyConfigMap.put("input.type", DummyInput.class.getName());
    dependencyConfigMap.put("input.starting.partitions", 5);
    Config dependencyConfig = ConfigFactory.parseMap(dependencyConfigMap);
    
    BatchStep dependencyStep = new BatchStep("hello", dependencyConfig);
    dependencyStep.submit(Sets.<Step>newHashSet());
    
    Map<String, Object> dependentConfigMap = Maps.newHashMap();
    dependentConfigMap.put("dependencies", Lists.newArrayList("hello"));
    dependentConfigMap.put("deriver.type", PassthroughDeriver.class.getName());
    dependentConfigMap.put("deriver.repartition.partitions", 10);
    Config dependentConfig = ConfigFactory.parseMap(dependentConfigMap);
    
    BatchStep dependentStep = new BatchStep("world", dependentConfig);
    dependentStep.submit(Sets.<Step>newHashSet(dependencyStep));
    Dataset<Row> df = dependentStep.getData();
    int numPartitions = df.javaRDD().getNumPartitions(); 
    
    assertEquals(numPartitions, 10);
  }
  
  @Test
  public void testDeriverCoalesce() throws Exception {
    Map<String, Object> dependencyConfigMap = Maps.newHashMap();
    dependencyConfigMap.put("input.type", DummyInput.class.getName());
    dependencyConfigMap.put("input.starting.partitions", 10);
    Config dependencyConfig = ConfigFactory.parseMap(dependencyConfigMap);
    
    BatchStep dependencyStep = new BatchStep("hello", dependencyConfig);
    dependencyStep.submit(Sets.<Step>newHashSet());
    
    Map<String, Object> dependentConfigMap = Maps.newHashMap();
    dependentConfigMap.put("dependencies", Lists.newArrayList("hello"));
    dependentConfigMap.put("deriver.type", PassthroughDeriver.class.getName());
    dependentConfigMap.put("deriver.coalesce.partitions", 5);
    Config dependentConfig = ConfigFactory.parseMap(dependentConfigMap);
    
    BatchStep dependentStep = new BatchStep("world", dependentConfig);
    dependentStep.submit(Sets.<Step>newHashSet(dependencyStep));
    Dataset<Row> df = dependentStep.getData();
    int numPartitions = df.javaRDD().getNumPartitions(); 
    
    assertEquals(numPartitions, 5);
  }
  
  @Test
  (expected = RuntimeException.class)
  public void testCantRepartitionAndCoalesceInputAtOnce() throws Exception {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put("input.type", DummyInput.class.getName());
    configMap.put("input.starting.partitions", 5);
    configMap.put("input.repartition.partitions", 10);
    configMap.put("input.coalesce.partitions", 3);
    Config config = ConfigFactory.parseMap(configMap);
    
    new BatchStep("test", config);
  }
  
  @Test
  (expected = RuntimeException.class)
  public void testCantRepartitionAndCoalesceDeriverAtOnce() throws Exception {
    Map<String, Object> dependencyConfigMap = Maps.newHashMap();
    dependencyConfigMap.put("input.type", DummyInput.class.getName());
    dependencyConfigMap.put("input.starting.partitions", 5);
    Config dependencyConfig = ConfigFactory.parseMap(dependencyConfigMap);
    
    BatchStep dependencyStep = new BatchStep("hello", dependencyConfig);
    dependencyStep.submit(Sets.<Step>newHashSet());
    
    Map<String, Object> dependentConfigMap = Maps.newHashMap();
    dependentConfigMap.put("dependencies", Lists.newArrayList("hello"));
    dependentConfigMap.put("deriver.type", PassthroughDeriver.class.getName());
    dependentConfigMap.put("deriver.repartition.partitions", 10);
    dependentConfigMap.put("deriver.coalesce.partitions", 3);
    Config dependentConfig = ConfigFactory.parseMap(dependentConfigMap);
    
    new BatchStep("world", dependentConfig);
  }

}
