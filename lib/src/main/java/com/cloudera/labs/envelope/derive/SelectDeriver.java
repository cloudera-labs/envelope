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
import scala.collection.JavaConverters;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Deriver class for selecting specific columns from a dataset provided by "Dependencies".
 * Dependencies can be a list of one or more dependencies 
 * Configuration "step" is required in case dependencies has more than one dependency 
 * Deriver takes list of columns to be selected or rejected   
 *
 */
public class SelectDeriver implements Deriver, ProvidesAlias, ProvidesValidations {

  public static final String INCLUDE_FIELDS = "include-fields";
  public static final String EXCLUDE_FIELDS = "exclude-fields";
  public static final String STEP_NAME_CONFIG = "step";
  
  private String stepName;
  private List<String> includeFields;
  private List<String> excludeFields;
  private boolean useIncludeFields = true;

  
  @Override
  public void configure(Config config) {
    if (config.hasPath(STEP_NAME_CONFIG)) {
      stepName = config.getString(STEP_NAME_CONFIG);
    }

    if (config.hasPath(INCLUDE_FIELDS)) {
      includeFields = config.getStringList(INCLUDE_FIELDS);
      useIncludeFields = true;
    }
    else {
      excludeFields = config.getStringList(EXCLUDE_FIELDS);
      useIncludeFields = false;
    }
  }
  
  @Override
  public Dataset<Row> derive(Map<String, Dataset<Row>> dependencies) throws Exception {
    dependencyCheck(dependencies);
    Dataset<Row> sourceStep = dependencies.get(stepName);
    if (useIncludeFields){
        if (!Arrays.asList(sourceStep.columns()).containsAll(includeFields)){
            throw new RuntimeException("Columns specified in " + INCLUDE_FIELDS + " are not found in input dependency schema \n" +
            "Available columns: " + Arrays.toString(sourceStep.columns()));
        }
        String firstCol = includeFields.get(0);
        includeFields.remove(0);
        return sourceStep.select(firstCol, includeFields.toArray(new String[0]));
    } else {
        if (!Arrays.asList(sourceStep.columns()).containsAll(excludeFields)){
            throw new RuntimeException("Columns specified in " + EXCLUDE_FIELDS + " are not found in input dependency schema \n" +
            "Available columns: " + Arrays.toString(sourceStep.columns()));
        }
        return sourceStep.drop(JavaConverters.collectionAsScalaIterableConverter(excludeFields).asScala().toSeq());
    }
  }

  // Check and set step configuration based on dependency list 
  private void dependencyCheck(Map<String, Dataset<Row>> dependencies) {
    switch (dependencies.size()) {
      case 0:
        throw new RuntimeException("Select deriver requires at least one dependency");
      case 1:
        if (stepName == null || stepName.trim().length() == 0) {
            stepName = dependencies.entrySet().iterator().next().getKey();
        }
        break;
      default:
        if (stepName == null || stepName.trim().length() == 0){
            throw new RuntimeException("Select deriver requires a \"step\" configuration when multiple dependencies have been listed");
        }
     }

    if (!dependencies.containsKey(stepName)){
       throw new RuntimeException("Invalid \"step\" configuration: " + stepName + " is not listed as dependency");
    }
  }
  
  @Override
  public String getAlias() {
    return "select";
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .optionalPath(STEP_NAME_CONFIG, ConfigValueType.STRING)
        .exactlyOnePathExists(ConfigValueType.LIST, INCLUDE_FIELDS, EXCLUDE_FIELDS)
        .build();
  }

}