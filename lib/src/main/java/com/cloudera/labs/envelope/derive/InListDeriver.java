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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * Returns new dataset by filtering the dependency on a field being in the list
 * of possible provided values. The list can be either a literal or extracted
 * from another dependency step.
 * </p>
 * <p>
 * If "dependencies" is a list, a <code>step</code> config parameter is required
 * to disambiguate operand for the in-list() operation, as is the
 * <code>field</code> if the <code>step</code>'s schema contians more than one
 * field.
 * </p>
 * <p>
 * Either <code>values</code> parameter or a combination of
 * <code>values-step</code>/<code>values-field</code> is required.
 * </p>
 */

public class InListDeriver implements Deriver, ProvidesAlias, ProvidesValidations {

  public static final int INLIST_MAX_LIST_SIZE = 1000;

  public static final String INLIST_DERIVER_ALIAS = "in-list";
  public static final String INLIST_STEP_CONFIG = "step";
  public static final String INLIST_FIELD_CONFIG = "field";
  public static final String INLIST_VALUES_CONFIG = "values";
  public static final String INLIST_REFSTEP_CONFIG = "values-step";
  public static final String INLIST_REFFIELD_CONFIG = "values-field";

  private enum InListType {
    LITERAL, REFERENCE
  }

  private InListType inListType;
  private String stepName;
  private String fieldName;
  private String refStepName;
  private String refFieldName;
  private List<Object> inList;

  private static final Logger LOGGER = LoggerFactory.getLogger(InListDeriver.class);

  @Override
  public void configure(Config config) {
    LOGGER.debug("Configuring in-list deriver with " + config.toString());

    if (config.hasPath(INLIST_STEP_CONFIG)) {
      stepName = config.getString(INLIST_STEP_CONFIG);
    }
    if (config.hasPath(INLIST_FIELD_CONFIG)) {
      fieldName = config.getString(INLIST_FIELD_CONFIG);
    }
    if (config.hasPath(INLIST_VALUES_CONFIG)) {
      inList = config.getList(INLIST_VALUES_CONFIG).unwrapped();
      inListType = InListType.LITERAL;
    } else if (config.hasPath(INLIST_REFSTEP_CONFIG)) {
      refStepName = config.getString(INLIST_REFSTEP_CONFIG);
      if (config.hasPath(INLIST_REFFIELD_CONFIG)) {
        refFieldName = config.getString(INLIST_REFFIELD_CONFIG);
      }
      inListType = InListType.REFERENCE;
    }
  }

  @Override
  public Dataset<Row> derive(Map<String, Dataset<Row>> dependencies) throws Exception {
    LOGGER.debug("Derive: Validating dependencies map " + dependencies.toString());
    validate(dependencies);
    String step = getStepName(dependencies);
    String field = getFieldName(dependencies);
    Object[] inList = getInList(dependencies);
    LOGGER.debug("Derive: Filtering dataset " + step + " by field " + field + " being IN "
        + Arrays.asList(inList).toString() + "");
    return dependencies.get(step).filter(dependencies.get(step).col(field).isin(inList));
  }

  @Override
  public String getAlias() {
    return INLIST_DERIVER_ALIAS;
  }

  void validate(Map<String, Dataset<Row>> dependencies) {
    if (dependencies.size() == 0) {
      throw new RuntimeException("Validate: In-List deriver requires at least one dependency");
    } else if (dependencies.size() == 1) {
      if (stepName != null && !dependencies.containsKey(stepName)) {
        throw new RuntimeException("Validate: \"" + INLIST_STEP_CONFIG + "\" " + stepName
            + " is not listed as dependency in " + dependencies.keySet() + "");
      }
    } else {
      if (stepName == null) {
        throw new RuntimeException("Validate: In-List deriver requires a \"" + INLIST_STEP_CONFIG
            + "\" configuration when multiple dependencies have been listed: " + dependencies.keySet() + "");
      }
      if (!dependencies.containsKey(stepName)) {
        throw new RuntimeException("Validate: \"" + INLIST_STEP_CONFIG + "\" " + stepName
            + " is not listed as dependency in " + dependencies.keySet() + "");
      }
    }
    String step = stepName == null ? dependencies.keySet().iterator().next() : stepName;
    if (dependencies.get(step) == null) {
      throw new RuntimeException("Validation: the dataset for \"" + INLIST_STEP_CONFIG + "\" " + step + " is NULL");
    }
    String[] fields = dependencies.get(step).columns();
    if (fields.length == 0) {
      throw new RuntimeException("Validation: the schema for \"" + INLIST_STEP_CONFIG + "\" " + step + " is empty");
    }
    if (fieldName == null && fields.length > 1) {
      throw new RuntimeException(
          "Validate: \"" + INLIST_FIELD_CONFIG + "\" parameter should be specified to apply the In-List deriver to \""
              + INLIST_STEP_CONFIG + "\" " + step + "");
    }
    if (fieldName != null && !Arrays.asList(fields).contains(fieldName)) {
      throw new RuntimeException("Validate: the schema for \"" + INLIST_STEP_CONFIG + "\" " + step
          + " doesn't contain \"" + INLIST_FIELD_CONFIG + "\" " + fieldName + "");
    }

    if (inListType == InListType.REFERENCE) {
      if (!dependencies.containsKey(refStepName)) {
        throw new RuntimeException("Validate: \"" + INLIST_REFSTEP_CONFIG + "\" " + refStepName
            + " is not listed as dependency in " + dependencies.keySet() + "");
      }
      fields = dependencies.get(refStepName).columns();
      if (fields.length == 0) {
        throw new RuntimeException(
            "Validate: the schema for \"" + INLIST_REFSTEP_CONFIG + "\" " + refStepName + " is empty");
      }
      if (refFieldName == null && fields.length > 1) {
        throw new RuntimeException(
            "Validate: \"" + INLIST_REFFIELD_CONFIG + "\" parameter should be specified to generate in-list from the \""
                + INLIST_REFSTEP_CONFIG + "\" " + refStepName + "");
      }
      if (refFieldName != null && !Arrays.asList(fields).contains(refFieldName)) {
        throw new RuntimeException("Validate: the schema for \"" + INLIST_REFSTEP_CONFIG + "\" " + refStepName
            + " doesn't contain \"" + INLIST_REFFIELD_CONFIG + "\" " + refFieldName + "");
      }
      String refField = refFieldName == null ? fields[0] : refFieldName;
      long refListSize = dependencies.get(refStepName).select(refField).distinct().count();
      if (refListSize > INLIST_MAX_LIST_SIZE) {
        throw new RuntimeException("Validate: \"" + INLIST_REFFIELD_CONFIG + "\" " + refStepName + "." + refField
            + " contains " + refListSize + " elements. Maximum allowed size is " + INLIST_MAX_LIST_SIZE + ".");
      }
    }
  }

  private String getStepName(Map<String, Dataset<Row>> dependencies) {
    return stepName == null ? dependencies.keySet().iterator().next() : stepName;
  }

  private String getFieldName(Map<String, Dataset<Row>> dependencies) {
    return fieldName == null ? dependencies.get(getStepName(dependencies)).columns()[0] : fieldName;
  }

  private String getRefFieldName(Map<String, Dataset<Row>> dependencies) {
    return refFieldName == null ? dependencies.get(refStepName).columns()[0] : refFieldName;
  }

  Object[] getInList(Map<String, Dataset<Row>> dependencies) {
    if (inListType == InListType.REFERENCE) {
      List<Row> t = dependencies.get(refStepName).select(getRefFieldName(dependencies)).distinct().collectAsList();
      inList = new ArrayList<Object>(t.size());
      for (Row r : t) {
        inList.add(r.get(0));
      }
    }
    return inList.toArray(new Object[0]);
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .optionalPath(INLIST_STEP_CONFIG, ConfigValueType.STRING)
        .optionalPath(INLIST_FIELD_CONFIG, ConfigValueType.STRING)
        .optionalPath(INLIST_REFSTEP_CONFIG, ConfigValueType.STRING)
        .optionalPath(INLIST_VALUES_CONFIG, ConfigValueType.LIST)
        .exactlyOnePathExists(INLIST_REFSTEP_CONFIG, INLIST_VALUES_CONFIG)
        .ifPathExists(INLIST_REFSTEP_CONFIG,
            Validations.single().optionalPath(INLIST_REFFIELD_CONFIG, ConfigValueType.STRING))
        .build();
  }

}
