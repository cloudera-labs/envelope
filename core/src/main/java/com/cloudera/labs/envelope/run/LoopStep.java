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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.utils.StepUtils;
import com.cloudera.labs.envelope.validate.MandatoryPathValidation;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.common.base.Optional;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;

public class LoopStep extends RefactorStep implements ProvidesValidations {

  public static final String MODE_PROPERTY = "mode";
  public static final String MODE_SERIAL = "serial";
  public static final String MODE_PARALLEL = "parallel";
  public static final String PARAMETER_PROPERTY = "parameter";
  public static final String SOURCE_PROPERTY = "source";
  public static final String SOURCE_RANGE = "range";
  public static final String SOURCE_LIST = "list";
  public static final String SOURCE_STEP = "step";
  public static final String RANGE_START_PROPERTY = "range.start";
  public static final String RANGE_END_PROPERTY = "range.end";
  public static final String LIST_PROPERTY = "list";
  public static final String STEP_PROPERTY = "step";
  public static final String SUFFIX_PROPERTY = "suffix";
  public static final String DEFAULT_SUFFIX_PROPERTY = "value";

  private static Logger LOG = LoggerFactory.getLogger(LoopStep.class);

  public LoopStep(String name) {
    super(name);
  }
  
  // Envelope runs loops by unrolling the loop when the loop step is run. This means
  // that the iterations of the loop must be known when the loop step runs, either from
  // static values in the configuration or from dynamic values provided by previous steps.
  @Override
  public Set<Step> refactor(Set<Step> steps) {
    // The rows that the loop iterates over
    List<Row> iterationRows = getIterationRows(steps);
    
    // The mode that the loop runs as, either 'parallel' or 'serial'
    String mode = getMode();
    
    // The graph of steps that will be run through once per loop iteration.
    // This is defined as all steps that are immediately dependent on the loop step.
    // Steps that are indirectly dependent on the loop step will not be looped over,
    // and will run when the steps of the unrolled loop has completed.
    Set<Step> loopGraphSteps = StepUtils.getImmediateDependentSteps(this, steps);
    LOG.debug("Loop graph steps found: " + StepUtils.stepNamesAsString(loopGraphSteps));
    
    // Determine which steps are directly dependent on any loop graph steps, which we will
    // only allow to run when all iterations of the loop have completed.
    Set<Step> loopGraphStepDependents = Sets.newHashSet();
    for (Step loopGraphStep : loopGraphSteps) {
      for (Step loopGraphStepDependentCandidate : StepUtils.getImmediateDependentSteps(loopGraphStep, steps)) {
        if (!loopGraphSteps.contains(loopGraphStepDependentCandidate)) {
          loopGraphStepDependents.add(loopGraphStepDependentCandidate);
        }
      }
    }
    LOG.debug("Loop graph step dependents found: " + StepUtils.stepNamesAsString(loopGraphStepDependents));
    
    // Remove the loop graph steps from the full graph so that we can re-insert it
    // once per loop iteration
    steps.removeAll(loopGraphSteps);
    
    // Iterate through the loop, adding the loop graph steps each time.
    // Each iteration of the loop is a copy of the loop graph steps, where the names of
    // the steps are suffixed with the value of the iteration.
    Set<Step> previousIterationSteps = null;
    for (Row iterationRow : iterationRows) {
      String suffix = getSuffix(iterationRow) ;
      LOG.debug("Constructing loop for iteration value: {}", suffix);
      // Make a copy of the loop graph steps for this iteration
      Set<Step> iterationSteps = StepUtils.copySteps(loopGraphSteps);
      
      // Go through the iteration steps and adjust the dependency names to have
      // the iteration value suffix for dependencies within the loop graph steps
      for (Step iterationStep : iterationSteps) {
        LOG.debug("Adjusting dependencies for iteration step: " + iterationStep.getName());
        Set<String> dependencies = iterationStep.getDependencyNames();
        Set<String> dependenciesToAdd = Sets.newHashSet();
        Set<String> dependenciesToRemove = Sets.newHashSet();
        
        for (String dependency : dependencies) {
          if (StepUtils.getStepForName(dependency, loopGraphSteps).isPresent()) {
            dependenciesToAdd.add(dependency + "_" + suffix);
            dependenciesToRemove.add(dependency);
          }
        }
        
        dependencies.addAll(dependenciesToAdd);
        LOG.debug("Added dependencies: " + dependenciesToAdd);
        dependencies.removeAll(dependenciesToRemove);
        LOG.debug("Removed dependencies: " + dependenciesToRemove);
      }
      
      // Adjust the iteration step names to have the iteration value suffix and to
      // have the parameter replaced with the iteration value
      for (Step iterationStep : iterationSteps) {
        String adjustedIterationStepName = iterationStep.getName() + "_" + suffix; 
        LOG.debug("Renaming iteration step {} to {}", iterationStep.getName(), adjustedIterationStepName);
        iterationStep.setName(adjustedIterationStepName);
        
        // Change steps that are dependent on the loop graph steps, but are not part of the
        // loop graph steps itself, to run after all iterations
        for (Step loopGraphStepDependent : loopGraphStepDependents) {
          if (!(iterationStep instanceof LoopStep)) {
            LOG.debug("Adding dependency {} to {}", iterationStep.getName(), loopGraphStepDependent.getName());
            loopGraphStepDependent.getDependencyNames().add(adjustedIterationStepName);
          }
        }
        
        Config parameterSubstConfig = performSubstitutions(iterationStep.getConfig(), iterationRow);
        iterationStep.setConfig(parameterSubstConfig);
        LOG.debug("Parameter substitutions completed: {} ", parameterSubstConfig.toString());
      }
      
      // In serial mode we want all non-first iterations to be dependent on the previous
      // iteration so that they run in serial order
      if (mode.equals(MODE_SERIAL) && previousIterationSteps != null) {
        LOG.debug("Adjusting iteration steps to be dependent on previous iteration's steps");
        // Add all previous iteration's step names to all this iteration's step dependencies
        for (Step iterationStep : iterationSteps) {
          for (Step previousIterationStep : previousIterationSteps) {
            if (!(previousIterationStep instanceof LoopStep)) {
              LOG.debug("Adding dependency {} to iteration step {}", previousIterationStep.getName(), iterationStep.getName());
              iterationStep.getDependencyNames().add(previousIterationStep.getName());
            }
          }
        }
      }
      previousIterationSteps = iterationSteps;

      steps.addAll(iterationSteps);
    }
    
    // Remove original loop graph step dependencies from loop graph step dependents
    for (Step loopGraphStepDependent : loopGraphStepDependents) {
      for (Step loopGraphStep : loopGraphSteps) {
        loopGraphStepDependent.getDependencyNames().remove(loopGraphStep.getName());
      }
    }
    
    LOG.debug("Unrolled steps: " + StepUtils.stepNamesAsString(steps));
    
    this.setState(StepState.FINISHED);
    
    return steps;
  }
  
  private Config performSubstitutions(Config c, Row row) {
    Config substConfig = c;
    for (StructField field : row.schema().fields()) {
      Object value = row.getAs(field.name());
      substConfig = ConfigUtils.findReplaceStringValues(substConfig, "\\$\\{" + field.name() + "\\}",
        value != null ? value : "null");
    }
    return substConfig;
  }

  private String getSuffix(Row row) {
    Object suffix;
    if (SOURCE_STEP.equals(config.getString(SOURCE_PROPERTY)) && config.hasPath(SUFFIX_PROPERTY)) {
      String suffixColumn = config.getString(SUFFIX_PROPERTY);
      if (row.schema().fieldIndex(suffixColumn) < 0) {
        throw new RuntimeException("Column '" + SUFFIX_PROPERTY + "=" + suffixColumn
            + " does not exist in source step's schema " + row.schema().toString());
      }
      suffix = row.getAs(suffixColumn);
    } else {
      suffix = row.getAs(0);
    }
    return suffix == null ? "null" : suffix.toString();
  }
  
  private List<Row> getIterationRows(Set<Step> steps) {
    List<Row> rows;

    String source = config.getString(SOURCE_PROPERTY);
    switch (source) {
      case SOURCE_RANGE:
        rows = getRowsFromRange();
        break;
      case SOURCE_LIST:
        rows = getRowsFromList();
        break;
      case SOURCE_STEP:
        rows = getRowsFromStep(steps);
        break;
      default:
        throw new RuntimeException("Invalid source for loop step '" + getName() + "'");
    }
    
    return rows;
  }
  
  private List<Row> getRowsFromRange() {
    int rangeStart = config.getInt(RANGE_START_PROPERTY);
    int rangeEnd = config.getInt(RANGE_END_PROPERTY);
    List<Object> l = Lists.<Object>newArrayList(ImmutableList.copyOf(ContiguousSet.create(Range.closed(rangeStart, rangeEnd), DiscreteDomain.integers()))); 
    return toRowList(l);
  }

  private List<Row> getRowsFromList() {
    return toRowList(config.getAnyRefList(LIST_PROPERTY));
  }
  
  private List<Row> toRowList(List<? extends Object> substitutionValues) {
    String columnName = hasParameter() ? getParameter() : DEFAULT_SUFFIX_PROPERTY;
    StructType schema = DataTypes.createStructType(
      new StructField[] {
        DataTypes.createStructField(columnName, DataTypes.StringType, false)
      });
    List<Row> substitutionRows = new ArrayList<Row>();
    for(Object value : substitutionValues) {
      substitutionRows.add(new RowWithSchema(schema, new Object[] {value}));
    }
    return substitutionRows;
  }

  private List<Row> getRowsFromStep(Set<Step> steps) {
    String stepName = config.getString(STEP_PROPERTY);
    Optional<Step> optionalStep = StepUtils.getStepForName(stepName, steps);
    
    if (!optionalStep.isPresent()) {
      throw new RuntimeException("Step source for loop step '" + getName() + "' does not exist.");
    }
    
    Step step = optionalStep.get();
    
    if (!(step instanceof DataStep)) {
      throw new RuntimeException("Step source for loop step '" + getName() + "' is not a data step.");
    }
    
    Dataset<Row> stepRows = ((DataStep)step).getData();
    
    if (stepRows.count() > 1000) {
      throw new RuntimeException("Step source for loop step '" + getName() + "' can not provide more than 1000 values to loop over");
    }

    return stepRows.collectAsList();
  }

  private String getMode() {
    String mode;
    
    if (!config.hasPath(MODE_PROPERTY)) {
      throw new RuntimeException("Loop step '" + getName() + "' must provide mode");
    }
    
    mode = config.getString(MODE_PROPERTY);
    
    if (!mode.equals(MODE_PARALLEL) && !mode.equals(MODE_SERIAL)) {
      throw new RuntimeException("Loop step '" + getName() + "' must provide mode as '" + MODE_PARALLEL + "' or '" + MODE_SERIAL + "'");
    }
    
    return mode;
  }
  
  private boolean hasParameter() {
    return config.hasPath(PARAMETER_PROPERTY);
  }
  
  private String getParameter() {
    return config.getString(PARAMETER_PROPERTY);
  }

  @Override
  public Step copy() {
    Step copy = new LoopStep(name);
    copy.configure(config);

    copy.setDependencyNames(getDependencyNames());
    copy.setState(getState());

    return copy;
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(MODE_PROPERTY, ConfigValueType.STRING)
        .mandatoryPath(SOURCE_PROPERTY, ConfigValueType.STRING)
        .allowedValues(SOURCE_PROPERTY, SOURCE_RANGE, SOURCE_LIST, SOURCE_STEP)
        .ifPathHasValue(SOURCE_PROPERTY, SOURCE_RANGE,
            new MandatoryPathValidation(RANGE_START_PROPERTY))
        .ifPathHasValue(SOURCE_PROPERTY, SOURCE_RANGE,
            new MandatoryPathValidation(RANGE_END_PROPERTY))
        .ifPathHasValue(SOURCE_PROPERTY, SOURCE_LIST, 
            new MandatoryPathValidation(LIST_PROPERTY, ConfigValueType.LIST))
        .ifPathHasValue(SOURCE_PROPERTY, SOURCE_STEP, 
            new MandatoryPathValidation(STEP_PROPERTY, ConfigValueType.STRING))
        .ifPathHasValue(SOURCE_PROPERTY, SOURCE_RANGE, 
            new MandatoryPathValidation(PARAMETER_PROPERTY, ConfigValueType.STRING))
        .ifPathHasValue(SOURCE_PROPERTY, SOURCE_LIST, 
            new MandatoryPathValidation(PARAMETER_PROPERTY, ConfigValueType.STRING))
        .ifPathHasValue(SOURCE_PROPERTY, SOURCE_STEP, 
            new MandatoryPathValidation(SUFFIX_PROPERTY, ConfigValueType.STRING))
        .addAll(super.getValidations())
        .build();
  }
}
