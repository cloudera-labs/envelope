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
package com.cloudera.labs.envelope.run;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import com.cloudera.labs.envelope.derive.PassthroughDeriver;
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
    configMap.put("input.num.partitions", 5);
    configMap.put("input.repartition.partitions", 10);
    Config config = ConfigFactory.parseMap(configMap);
    
    BatchStep batchStep = new BatchStep("test", config);
    batchStep.runStep(Sets.<Step>newHashSet());
    Dataset<Row> df = batchStep.getData();
    int numPartitions = df.javaRDD().getNumPartitions(); 
    
    assertEquals(numPartitions, 10);
  }
  
  @Test
  public void testInputCoalesce() throws Exception {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put("input.type", DummyInput.class.getName());
    configMap.put("input.num.partitions", 10);
    configMap.put("input.repartition.partitions", 5);
    Config config = ConfigFactory.parseMap(configMap);
    
    BatchStep batchStep = new BatchStep("test", config);
    batchStep.runStep(Sets.<Step>newHashSet());
    Dataset<Row> df = batchStep.getData();
    int numPartitions = df.javaRDD().getNumPartitions(); 
    
    assertEquals(numPartitions, 5);
  }
  
  @Test
  public void testDeriverRepartition() throws Exception {
    Map<String, Object> dependencyConfigMap = Maps.newHashMap();
    dependencyConfigMap.put("input.type", DummyInput.class.getName());
    dependencyConfigMap.put("input.num.partitions", 5);
    Config dependencyConfig = ConfigFactory.parseMap(dependencyConfigMap);
    
    BatchStep dependencyStep = new BatchStep("hello", dependencyConfig);
    dependencyStep.runStep(Sets.<Step>newHashSet());
    
    Map<String, Object> dependentConfigMap = Maps.newHashMap();
    dependentConfigMap.put("dependencies", Lists.newArrayList("hello"));
    dependentConfigMap.put("deriver.type", PassthroughDeriver.class.getName());
    dependentConfigMap.put("deriver.repartition.partitions", 10);
    Config dependentConfig = ConfigFactory.parseMap(dependentConfigMap);
    
    BatchStep dependentStep = new BatchStep("world", dependentConfig);
    dependentStep.runStep(Sets.<Step>newHashSet(dependencyStep));
    Dataset<Row> df = dependentStep.getData();
    int numPartitions = df.javaRDD().getNumPartitions(); 
    
    assertEquals(numPartitions, 10);
  }
  
  @Test
  public void testDeriverCoalesce() throws Exception {
    Map<String, Object> dependencyConfigMap = Maps.newHashMap();
    dependencyConfigMap.put("input.type", DummyInput.class.getName());
    dependencyConfigMap.put("input.num.partitions", 10);
    Config dependencyConfig = ConfigFactory.parseMap(dependencyConfigMap);
    
    BatchStep dependencyStep = new BatchStep("hello", dependencyConfig);
    dependencyStep.runStep(Sets.<Step>newHashSet());
    
    Map<String, Object> dependentConfigMap = Maps.newHashMap();
    dependentConfigMap.put("dependencies", Lists.newArrayList("hello"));
    dependentConfigMap.put("deriver.type", PassthroughDeriver.class.getName());
    dependentConfigMap.put("deriver.coalesce.partitions", 5);
    Config dependentConfig = ConfigFactory.parseMap(dependentConfigMap);
    
    BatchStep dependentStep = new BatchStep("world", dependentConfig);
    dependentStep.runStep(Sets.<Step>newHashSet(dependencyStep));
    Dataset<Row> df = dependentStep.getData();
    int numPartitions = df.javaRDD().getNumPartitions(); 
    
    assertEquals(numPartitions, 5);
  }
  
  @Test
  (expected = RuntimeException.class)
  public void testCantRepartitionAndCoalesceInputAtOnce() throws Exception {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put("input.type", DummyInput.class.getName());
    configMap.put("input.num.partitions", 5);
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
    dependencyConfigMap.put("input.num.partitions", 5);
    Config dependencyConfig = ConfigFactory.parseMap(dependencyConfigMap);
    
    BatchStep dependencyStep = new BatchStep("hello", dependencyConfig);
    dependencyStep.runStep(Sets.<Step>newHashSet());
    
    Map<String, Object> dependentConfigMap = Maps.newHashMap();
    dependentConfigMap.put("dependencies", Lists.newArrayList("hello"));
    dependentConfigMap.put("deriver.type", PassthroughDeriver.class.getName());
    dependentConfigMap.put("deriver.repartition.partitions", 10);
    dependentConfigMap.put("deriver.coalesce.partitions", 3);
    Config dependentConfig = ConfigFactory.parseMap(dependentConfigMap);
    
    new BatchStep("world", dependentConfig);
  }

}
