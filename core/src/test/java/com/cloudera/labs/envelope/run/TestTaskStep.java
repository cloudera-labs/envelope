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

import com.cloudera.labs.envelope.run.TaskStep;
import com.cloudera.labs.envelope.task.Task;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestTaskStep {
  
  public static String customTaskGlobal;

  @Test
  public void testCustomTask() {
    customTaskGlobal = "";
    
    Map<String, Object> taskStepConfigMap = Maps.newHashMap();
    taskStepConfigMap.put("type", "task");
    taskStepConfigMap.put("class", CustomTask.class.getName());
    taskStepConfigMap.put("value", "hello");
    Config taskStepConfig = ConfigFactory.parseMap(taskStepConfigMap);
    
    TaskStep taskStep = new TaskStep("task_step", taskStepConfig);
    
    taskStep.run(Maps.<String, Dataset<Row>>newHashMap());
    
    assertEquals(customTaskGlobal, "hello");
  }
  
  public static class CustomTask implements Task {
    String value;
    
    @Override
    public void configure(Config config) {
      this.value = config.getString("value");
    }

    @Override
    public void run(Map<String, Dataset<Row>> dependencies) {
      customTaskGlobal = this.value;
    }

    @Override
    public String getAlias() {
      return "custom";
    }
  }
  
}
