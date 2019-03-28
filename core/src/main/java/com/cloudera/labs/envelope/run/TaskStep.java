/*
 * Copyright (c) 2015-2019, Cloudera, Inc. All Rights Reserved.
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

import com.cloudera.labs.envelope.component.ComponentFactory;
import com.cloudera.labs.envelope.component.InstantiatedComponent;
import com.cloudera.labs.envelope.component.InstantiatesComponents;
import com.cloudera.labs.envelope.task.Task;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;
import java.util.Set;

public class TaskStep extends Step implements ProvidesValidations, InstantiatesComponents {

  public static final String TASK_CONFIG = "task";

  public TaskStep(String name) {
    super(name);
  }
  
  public void run(Map<String, Dataset<Row>> dependencies) {
    Task task = ComponentFactory.create(Task.class, getTaskConfig(config), true);
    
    task.run(dependencies);
    
    this.setState(StepState.FINISHED);
  }

  @Override
  public Step copy() {
    Step copy = new TaskStep(name);
    copy.configure(config);

    copy.setDependencyNames(getDependencyNames());
    copy.setState(getState());

    return copy;
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(TASK_CONFIG, ConfigValueType.OBJECT)
        .handlesOwnValidationPath(TASK_CONFIG)
        .addAll(super.getValidations())
        .build();
  }

  @Override
  public Set<InstantiatedComponent> getComponents(Config config, boolean configure) {
    Task task = ComponentFactory.create(Task.class, getTaskConfig(config), configure);

    Set<InstantiatedComponent> components = Sets.newHashSet(
        new InstantiatedComponent(task, getTaskConfig(config), "Task"));

    return components;
  }

  private Config getTaskConfig(Config config) {
    return config.getConfig(TASK_CONFIG);
  }

}
