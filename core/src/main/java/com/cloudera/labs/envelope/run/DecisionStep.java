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

import com.cloudera.labs.envelope.utils.StepUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.cloudera.labs.envelope.validate.MandatoryPathValidation;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

import java.util.List;
import java.util.Set;

public class DecisionStep extends RefactorStep implements ProvidesValidations {
  
  public static final String IF_TRUE_STEP_NAMES_PROPERTY = "if-true-steps";
  public static final String DECISION_METHOD_PROPERTY = "method";
  public static final String LITERAL_DECISION_METHOD = "literal";
  public static final String LITERAL_RESULT_PROPERTY = "result";
  public static final String STEP_BY_KEY_DECISION_METHOD = "step_by_key";
  public static final String STEP_BY_KEY_STEP_PROPERTY = "step";
  public static final String STEP_BY_KEY_KEY_PROPERTY = "key";
  public static final String STEP_BY_VALUE_DECISION_METHOD = "step_by_value";
  public static final String STEP_BY_VALUE_STEP_PROPERTY = "step";

  private enum DecisionMethod {
    STEP_BY_VALUE,
    STEP_BY_KEY,
    LITERAL
  }
  
  private List<String> ifTrueStepNames;
  private DecisionMethod decisionMethod;
  private boolean literalResult;
  private String stepByKeyStepName;
  private String stepByKeyKey;
  private String stepByValueStepName;

  public DecisionStep(String name) {
    super(name);
  }

  @Override
  public void configure(Config config) {
    super.configure(config);
    
    this.ifTrueStepNames = config.getStringList(IF_TRUE_STEP_NAMES_PROPERTY);
    this.decisionMethod = DecisionMethod.valueOf(config.getString(DECISION_METHOD_PROPERTY).toUpperCase());
    
    switch (decisionMethod) {
      case LITERAL:
        this.literalResult = config.getBoolean(LITERAL_RESULT_PROPERTY);
        break;
      case STEP_BY_KEY:
        this.stepByKeyStepName = config.getString(STEP_BY_KEY_STEP_PROPERTY);
        this.stepByKeyKey = config.getString(STEP_BY_KEY_KEY_PROPERTY);
        break;
      case STEP_BY_VALUE:
        this.stepByValueStepName = config.getString(STEP_BY_VALUE_STEP_PROPERTY);
        break;
    }
  }

  // Envelope runs decision steps by pruning out the steps of the pipeline that can not be
  // submitted as a result of a decision. This allows multiple sub-graphs to depend on the decision
  // step but only a subset of them to continue based on the decision result.
  // The configuration of a decision step defines how to make a boolean decision
  // (i.e. true or false), and then which of the immediately dependent steps to allow to run if the
  // decision result is true. Inversely, only the remaining immediately dependent steps will be
  // allowed to run if the decision result is false. Subsequent steps that can never be submitted
  // as a result of the pruning of immediately dependent steps are also pruned.
  @Override
  public Set<Step> refactor(Set<Step> steps) {
    Set<Step> decisionDependentSteps = StepUtils.getImmediateDependentSteps(this, steps);
    Set<Step> pruneSteps = getPruneSteps(decisionDependentSteps, steps);
    
    steps.removeAll(pruneSteps);
    
    this.setSubmitted(true);

    return steps;
  }
  
  private Set<Step> getPruneSteps(Set<Step> decideSteps, Set<Step> allSteps) {
    Set<Step> pruneSteps = Sets.newHashSet();
    
    boolean decision = evaluateDecision(allSteps);
    
    for (Step decideStep : decideSteps) {
      if (decision != this.ifTrueStepNames.contains(decideStep.getName())) {
        pruneSteps.add(decideStep);
        pruneSteps.addAll(StepUtils.getAllDependentSteps(decideStep, allSteps));
      }
    }
    
    return pruneSteps;
  }
  
  private boolean evaluateDecision(Set<Step> steps) {
    switch (decisionMethod) {
      case LITERAL:
        return evaluateLiteralDecision();
      case STEP_BY_KEY:
        return evaluateStepByKeyDecision(steps);
      case STEP_BY_VALUE:
        return evaluateStepByValueDecision(steps);
      default:
        throw new RuntimeException("Decision step's decision method was not initialized");
    }
  }
  
  private boolean evaluateLiteralDecision() {
    return literalResult;
  }
  
  private boolean evaluateStepByKeyDecision(Set<Step> steps) {
    Optional<Step> optionalStep = StepUtils.getStepForName(stepByKeyStepName, steps);
    
    if (!optionalStep.isPresent()) {
      throw new RuntimeException("Unknown decision step's key step: " + stepByValueStepName);
    }
    
    if (!(optionalStep.get() instanceof DataStep)) {
      throw new RuntimeException("Decision step's key step is not a data step: " + optionalStep.get().getName());
    }
    
    Dataset<Row> keyDataset = ((DataStep)optionalStep.get()).getData();
    
    if (keyDataset.schema().fields().length != 2 ||
        keyDataset.schema().fields()[0].dataType() != DataTypes.StringType ||
        keyDataset.schema().fields()[1].dataType() != DataTypes.BooleanType)
    {
      throw new RuntimeException("Decision step's key step must contain a string column and then a boolean column");
    }
    
    String keyColumnName = keyDataset.schema().fieldNames()[0];
    String whereClause = keyColumnName + " = '" + stepByKeyKey + "'";
    Dataset<Row> decisionDataset = keyDataset.where(whereClause);
    
    if (decisionDataset.count() != 1) {
      throw new RuntimeException("Decision step's key step must contain a single record for the given key");
    }
    
    boolean decision = decisionDataset.collectAsList().get(0).getBoolean(1);
    
    return decision;
  }
  
  private boolean evaluateStepByValueDecision(Set<Step> steps) {
    Optional<Step> optionalStep = StepUtils.getStepForName(stepByValueStepName, steps);
    
    if (!optionalStep.isPresent()) {
      throw new RuntimeException("Unknown decision step's value step: " + stepByValueStepName);
    }
    
    if (!(optionalStep.get() instanceof DataStep)) {
      throw new RuntimeException("Decision step's value step is not a data step: " + optionalStep.get().getName());
    }
    
    Dataset<Row> valueDataset = ((DataStep)optionalStep.get()).getData();
    
    if (valueDataset.schema().fields().length != 1 ||
        valueDataset.schema().fields()[0].dataType() != DataTypes.BooleanType ||
        valueDataset.count() != 1)
    {
      throw new RuntimeException("Decision step's value step must contain a single boolean column with a single row");
    }
    
    boolean decision = valueDataset.collectAsList().get(0).getBoolean(0);
    
    return decision;
  }

  @Override
  public Step copy() {
    Step copy = new DecisionStep(name);
    copy.configure(config);
    
    copy.setSubmitted(hasSubmitted());
    
    return copy;
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(IF_TRUE_STEP_NAMES_PROPERTY, ConfigValueType.LIST)
        .mandatoryPath(DECISION_METHOD_PROPERTY, ConfigValueType.STRING)
        .allowedValues(DECISION_METHOD_PROPERTY, LITERAL_DECISION_METHOD, STEP_BY_KEY_DECISION_METHOD, STEP_BY_VALUE_DECISION_METHOD)
        .ifPathHasValue(DECISION_METHOD_PROPERTY, LITERAL_DECISION_METHOD, 
            new MandatoryPathValidation(LITERAL_RESULT_PROPERTY, ConfigValueType.BOOLEAN))
        .ifPathHasValue(DECISION_METHOD_PROPERTY, STEP_BY_KEY_DECISION_METHOD, 
            new MandatoryPathValidation(STEP_BY_KEY_KEY_PROPERTY, ConfigValueType.STRING))
        .ifPathHasValue(DECISION_METHOD_PROPERTY, STEP_BY_KEY_DECISION_METHOD, 
            new MandatoryPathValidation(STEP_BY_KEY_STEP_PROPERTY, ConfigValueType.STRING))
        .ifPathHasValue(DECISION_METHOD_PROPERTY, STEP_BY_VALUE_DECISION_METHOD, 
            new MandatoryPathValidation(STEP_BY_VALUE_STEP_PROPERTY, ConfigValueType.STRING))
        .addAll(super.getValidations())
        .build();
  }

}
