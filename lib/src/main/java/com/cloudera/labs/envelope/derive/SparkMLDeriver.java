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

package com.cloudera.labs.envelope.derive;

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

public class SparkMLDeriver implements Deriver, ProvidesAlias, ProvidesValidations {

  private String dependencyStepName;
  private String modelPath;
  private PipelineModel model;

  public static final String DEPENDENCY_STEP_NAME_CONFIG = "step";
  public static final String MODEL_PATH_CONFIG = "model-path";

  @Override
  public void configure(Config config) {
    modelPath = config.getString(MODEL_PATH_CONFIG);
    dependencyStepName = ConfigUtils.getOrNull(config, DEPENDENCY_STEP_NAME_CONFIG);
  }

  @Override
  public Dataset<Row> derive(Map<String, Dataset<Row>> dependencies) {
    if (model == null) {
      model = PipelineModel.load(modelPath);
    }

    Dataset<Row> data = getData(dependencies);

    return model.transform(data);
  }

  private Dataset<Row> getData(Map<String, Dataset<Row>> dependencies) {
    if (dependencyStepName != null) {
      return dependencies.get(dependencyStepName);
    }
    else {
      if (dependencies.size() != 1) {
        throw new RuntimeException("Dependency step must be specified when step does not have one dependency");
      }
      return dependencies.values().iterator().next();
    }
  }

  @Override
  public String getAlias() {
    return "sparkml";
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(MODEL_PATH_CONFIG, ConfigValueType.STRING)
        .optionalPath(DEPENDENCY_STEP_NAME_CONFIG, ConfigValueType.STRING)
        .build();
  }

}
