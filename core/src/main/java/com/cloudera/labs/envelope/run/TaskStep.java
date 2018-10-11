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
import com.cloudera.labs.envelope.component.InstantiatesComponents;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.component.InstantiatedComponent;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;
import java.util.Set;

public class TaskStep extends Step implements ProvidesValidations, InstantiatesComponents {

  public TaskStep(String name) {
    super(name);
  }
  
  public void run(Map<String, Dataset<Row>> dependencies) {
    Task task = TaskFactory.create(config);
    
    task.run(dependencies);
    
    this.setSubmitted(true);
  }

  @Override
  public Step copy() {
    Step copy = new TaskStep(name);
    copy.configure(config);
    
    copy.setSubmitted(hasSubmitted());
    
    return copy;
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .addAll(super.getValidations())
        .allowUnrecognizedPaths()
        .build();
  }

  @Override
  public Set<InstantiatedComponent> getComponents(Config config, boolean configure) throws Exception {
    Task task = TaskFactory.create(config, configure);

    Set<InstantiatedComponent> components = Sets.newHashSet(
        new InstantiatedComponent(task, config, "Task"));

    return components;
  }

}
