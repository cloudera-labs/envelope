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

package com.cloudera.labs.envelope.task;

import com.cloudera.labs.envelope.load.LoadableFactory;
import com.typesafe.config.Config;

public class TaskFactory extends LoadableFactory<Task> {

  public static final String CLASS_CONFIG_NAME = "class";

  public static Task create(Config taskConfig) {
    return create(taskConfig, true);
  }
  
  public static Task create(Config taskConfig, boolean configure) {
    if (!taskConfig.hasPath(CLASS_CONFIG_NAME)) {
      throw new RuntimeException("Task class not specified");
    }

    String taskType = taskConfig.getString(CLASS_CONFIG_NAME);

    Task task;
    try {
      task = loadImplementation(Task.class, taskType);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    if (configure) {
      task.configure(taskConfig);
    }

    return task;
  }
  
}
