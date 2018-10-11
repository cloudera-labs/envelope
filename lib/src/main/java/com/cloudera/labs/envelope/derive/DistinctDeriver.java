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

package com.cloudera.labs.envelope.derive;

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

/**
 * <p>
 * Returns new dataset containing only unique rows from the dataset specified in
 * "dependencies".
 * </p>
 * <p>
 * If "dependencies" is a list, a <code>step</code> config parameter is required
 * to disambiguate operand for distinct() operation.
 */

public class DistinctDeriver implements Deriver, ProvidesAlias, ProvidesValidations {

  public static final String DISTINCT_DERIVER_ALIAS = "distinct";
  public static final String DISTINCT_STEP_CONFIG = "step";

  private String stepName = null;

  @Override
  public void configure(Config config) {
    if (config.hasPath(DISTINCT_STEP_CONFIG)) {
      stepName = config.getString(DISTINCT_STEP_CONFIG);
    }
  }

  @Override
  public Dataset<Row> derive(Map<String, Dataset<Row>> dependencies) throws Exception {
    validate(dependencies);
    return dependencies.get(stepName).distinct();
  }

  @Override
  public String getAlias() {
    return DISTINCT_DERIVER_ALIAS;
  }

  private void validate(Map<String, Dataset<Row>> dependencies) {
    switch (dependencies.size()) {
    case 0:
      throw new RuntimeException("Distinct deriver requires at least one dependency");
    case 1:
      if (stepName == null) {
        stepName = dependencies.keySet().iterator().next();
      } else {
        if (!dependencies.containsKey(stepName)) {
          throw new RuntimeException(
              "Invalid \"step\" configuration: " + stepName + " is not a dependency: " + dependencies.keySet() + "");
        }
      }
      break;
    default:
      if (stepName == null)
        throw new RuntimeException(
            "Distinct deriver requires a \"step\" configuration when multiple dependencies have been listed: "
                + dependencies.keySet() + "");
      if (!dependencies.containsKey(stepName))
        throw new RuntimeException("Invalid \"step\" configuration: " + stepName + " is not listed as dependency: "
            + dependencies.keySet() + "");
    }
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .optionalPath(DISTINCT_STEP_CONFIG, ConfigValueType.STRING)
        .build();
  }

}
