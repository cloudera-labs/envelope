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

import com.cloudera.labs.envelope.task.Task;
import com.cloudera.labs.envelope.task.TaskFactory;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestTaskStep {

  public static String customTaskGlobal;

  @Test
  public void testCustomTask() {
    customTaskGlobal = "";

    Map<String, Object> taskStepConfigMap = Maps.newHashMap();
    taskStepConfigMap.put(TaskFactory.TYPE_CONFIG_NAME, "task");
    taskStepConfigMap.put(TaskFactory.CLASS_CONFIG_NAME, CustomTask.class.getName());
    taskStepConfigMap.put("value", "hello");
    Config taskStepConfig = ConfigFactory.parseMap(taskStepConfigMap);

    TaskStep taskStep = new TaskStep("task_step");
    taskStep.configure(taskStepConfig);

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
  }

}

