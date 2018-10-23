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
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;

import java.util.List;
import java.util.Map;

public class LatestDeriver implements Deriver, ProvidesAlias, ProvidesValidations {

  public static final String STEP_NAME_CONFIG = "step";
  public static final String KEY_FIELD_NAMES_CONFIG = "key-fields";
  public static final String TIMESTAMP_FIELD_NAME_CONFIG = "timestamp-field";

  public static final String LATEST_DERIVER_ALIAS = "latest";

  private String stepName;
  private List<String> keyFieldNames;
  private String timestampFieldName;

  @Override
  public void configure(Config config) {
    stepName = ConfigUtils.getOrNull(config, STEP_NAME_CONFIG);
    keyFieldNames = config.getStringList(KEY_FIELD_NAMES_CONFIG);
    timestampFieldName = config.getString(TIMESTAMP_FIELD_NAME_CONFIG);
  }

  @Override
  public Dataset<Row> derive(Map<String, Dataset<Row>> dependencies) {
    String rowNumberFieldName = "_row_number";

    Dataset<Row> dependency = getStepDataFrame(dependencies);

    WindowSpec windowSpec;
    if (keyFieldNames.size() > 1) {
      List<String> keyFieldNamesTail = Lists.newArrayList(keyFieldNames);
      keyFieldNamesTail.remove(keyFieldNames.get(0));
      windowSpec = Window.partitionBy(keyFieldNames.get(0), keyFieldNamesTail.toArray(new String[0]));
    }
    else {
      windowSpec = Window.partitionBy(keyFieldNames.get(0));
    }
    windowSpec = windowSpec.orderBy(functions.desc(timestampFieldName));

    return dependency
        .withColumn(rowNumberFieldName, functions.row_number().over(windowSpec))
        .filter(functions.col(rowNumberFieldName).equalTo(1))
        .drop(rowNumberFieldName);
  }

  private Dataset<Row> getStepDataFrame(Map<String, Dataset<Row>> dependencies) {
    if (stepName != null) {
      if (!dependencies.containsKey(stepName)) {
        throw new RuntimeException("Latest deriver does not have step '" + stepName +
            "' in its dependencies");
      }
      return dependencies.get(stepName);
    }
    else {
      if (dependencies.size() != 1) {
        throw new RuntimeException(
            "Latest deriver must specify a step if it does not only have one dependency");
      }
      return dependencies.values().iterator().next();
    }
  }

  @Override
  public String getAlias() {
    return LATEST_DERIVER_ALIAS;
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .optionalPath(STEP_NAME_CONFIG, ConfigValueType.STRING)
        .mandatoryPath(KEY_FIELD_NAMES_CONFIG, ConfigValueType.LIST)
        .mandatoryPath(TIMESTAMP_FIELD_NAME_CONFIG, ConfigValueType.STRING)
        .build();
  }

}
