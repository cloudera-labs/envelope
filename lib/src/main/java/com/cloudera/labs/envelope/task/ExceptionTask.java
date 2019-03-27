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

package com.cloudera.labs.envelope.task;

import com.cloudera.labs.envelope.component.ProvidesAlias;
import com.cloudera.labs.envelope.run.TaskStep;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

public class ExceptionTask implements Task, ProvidesAlias, ProvidesValidations {

  private String message;

  public static final String MESSAGE_CONFIG = "message";

  @Override
  public void configure(Config config) {
    this.message = config.getString(MESSAGE_CONFIG);
  }

  @Override
  public void run(Map<String, Dataset<Row>> dependencies) {
    throw new RuntimeException(message);
  }

  @Override
  public String getAlias() {
    return "exception";
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(MESSAGE_CONFIG, ConfigValueType.STRING)
        .optionalPath(TaskStep.DEPENDENCIES_CONFIG)
        .optionalPath(TaskStep.CLASS_CONFIG)
        .build();
  }

}
