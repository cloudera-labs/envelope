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

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.cloudera.labs.envelope.task.Task;
import com.cloudera.labs.envelope.task.TaskFactory;
import com.typesafe.config.Config;

public class TaskStep extends Step {

  public TaskStep(String name, Config config) {
    super(name, config);
  }
  
  public void run(Map<String, Dataset<Row>> dependencies) {
    Task task = TaskFactory.create(config);
    
    task.run(dependencies);
    
    this.setSubmitted(true);
  }

  @Override
  public Step copy() {
    Step copy = new TaskStep(name, config);
    
    copy.setSubmitted(hasSubmitted());
    
    return copy;
  }

}
