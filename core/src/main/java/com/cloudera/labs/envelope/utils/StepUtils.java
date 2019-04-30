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

package com.cloudera.labs.envelope.utils;

import com.cloudera.labs.envelope.component.ComponentFactory;
import com.cloudera.labs.envelope.configuration.ConfigLoader;
import com.cloudera.labs.envelope.event.CoreEventMetadataKeys;
import com.cloudera.labs.envelope.event.CoreEventTypes;
import com.cloudera.labs.envelope.event.Event;
import com.cloudera.labs.envelope.event.EventManager;
import com.cloudera.labs.envelope.input.BatchInput;
import com.cloudera.labs.envelope.input.Input;
import com.cloudera.labs.envelope.input.StreamInput;
import com.cloudera.labs.envelope.repetition.Repetitions;
import com.cloudera.labs.envelope.run.BatchStep;
import com.cloudera.labs.envelope.run.DataStep;
import com.cloudera.labs.envelope.run.DecisionStep;
import com.cloudera.labs.envelope.run.LoopStep;
import com.cloudera.labs.envelope.run.Runner;
import com.cloudera.labs.envelope.run.Step;
import com.cloudera.labs.envelope.run.StepState;
import com.cloudera.labs.envelope.run.StreamingStep;
import com.cloudera.labs.envelope.run.TaskStep;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

public class StepUtils {

  private static final Logger LOG = LoggerFactory.getLogger(StepUtils.class);
  
  public static boolean allStepsSubmitted(Set<Step> steps) {
    for (Step step : steps) {
      if (step.getState() == StepState.WAITING) {
        return false;
      }
    }

    return true;
  }

  public static boolean allStepsFinished(Set<Step> steps) {
    for (Step step : steps) {
      if (step.getState() != StepState.FINISHED) {
        return false;
      }
    }

    return true;
  }

  public static Set<Step> getDependencies(Step step, Set<Step> steps) {
    Set<Step> dependencies = Sets.newHashSet();

    Set<String> dependencyNames = step.getDependencyNames();
    for (Step candidate : steps) {
      String candidateName = candidate.getName();
      if (dependencyNames.contains(candidateName)) {
        dependencies.add(candidate);
      }
    }

    return dependencies;
  }

  public static boolean hasStreamingStep(Set<Step> steps) {
    for (Step step : steps) {
      if (step instanceof StreamingStep) {
        return true;
      }
    }

    return false;
  }

  public static Set<StreamingStep> getStreamingSteps(Set<Step> steps) {
    Set<StreamingStep> steamingSteps = Sets.newHashSet();

    for (Step step : steps) {
      if (step instanceof StreamingStep) {
        steamingSteps.add((StreamingStep)step);
      }
    }

    return steamingSteps;
  }

  public static Set<Step> getAllDependentSteps(Step rootStep, Set<Step> steps) {
    Set<Step> dependencies = Sets.newHashSet();

    Set<Step> immediateDependents = getImmediateDependentSteps(rootStep, steps);
    for (Step immediateDependent : immediateDependents) {
      dependencies.add(immediateDependent);
      dependencies.addAll(getAllDependentSteps(immediateDependent, steps));
    }

    return dependencies;
  }

  public static Set<Step> getImmediateDependentSteps(Step step, Set<Step> steps) {
    Set<Step> dependencies = Sets.newHashSet();

    for (Step candidateStep : steps) {
      if (candidateStep.getDependencyNames().contains(step.getName())) {
        dependencies.add(candidateStep);
      }
    }

    return dependencies;
  }

  public static Set<Step> getIndependentNonStreamingSteps(Set<Step> steps) {
    // Independent non-streaming steps are all steps that are not streaming and
    // are not ultimately dependent on a streaming step

    Set<Step> streamsAndDependents = Sets.newHashSet();

    for (Step step : steps) {
      if (step instanceof StreamingStep) {
        streamsAndDependents.add(step);
        streamsAndDependents.addAll(StepUtils.getAllDependentSteps(step, steps));
      }
    }

    return Sets.difference(steps, streamsAndDependents);
  }

  public static String stepNamesAsString(Set<? extends Step> steps) {
    StringBuilder sb = new StringBuilder();

    for (Step step : steps) {
      sb.append(step.getName());
      sb.append(", ");
    }

    if (sb.length() > 0) {
      sb.setLength(sb.length() - ", ".length());
    }

    return sb.toString();
  }

  public static void resetSteps(Set<Step> steps) {
    for (Step step : steps) {
      step.reset();
    }
  }

  public static void resetRepeatingSteps(Set<Step> allSteps) {
    // Get all DataSteps that need to be reset
    Set<Step> resetSteps = Sets.newHashSet();
    Set<DataStep> repeatingSteps = Repetitions.get().getAndClearRepeatingSteps();
    LOG.info("Resetting {} repeating steps and their dependents", repeatingSteps.size());
    for (DataStep step : repeatingSteps) {
      resetSteps.add(step);
      resetSteps.addAll(getAllDependentSteps(step, allSteps));
    }
    resetSteps(resetSteps);
  }
  
  public static Set<DataStep> getDataSteps(Set<Step> steps) {
    Set<DataStep> dataSteps = Sets.newHashSet();
    
    for (Step step : steps) {
      if (step instanceof DataStep) {
        dataSteps.add((DataStep)step);
      }
    }
    
    return dataSteps;
  }

  public static Optional<Step> getStepForName(String name, Set<Step> steps) {
    for (Step step : steps) {
      if (step.getName().equals(name)) {
        return Optional.of(step);
      }
    }
    
    return Optional.absent();
  }

  public static Set<Step> copySteps(Set<Step> steps) {
    Set<Step> stepsCopy = Sets.newHashSet();
    
    for (Step step : steps) {
      stepsCopy.add(step.copy());
    }
    
    return stepsCopy;
  }
  
  public static Map<String, Dataset<Row>> getStepDataFrames(Set<Step> steps) {
    Map<String, Dataset<Row>> stepDFs = Maps.newHashMap();

    for (Step step : steps) {
      if (step instanceof DataStep) {
        stepDFs.put(step.getName(), ((DataStep)step).getData());
      }
    }

    return stepDFs;
  }

  public static Set<Step> extractSteps(Config config, boolean configure, boolean notify) {
    LOG.debug("Starting extracting steps");

    long startTime = System.nanoTime();

    Set<Step> steps = Sets.newHashSet();

    Set<String> stepNames = config.getObject(Runner.STEPS_SECTION_CONFIG).keySet();
    for (String stepName : stepNames) {
      Config stepConfig = config.getConfig(Runner.STEPS_SECTION_CONFIG).getConfig(stepName);

      Step step;

      if (!stepConfig.hasPath(Runner.TYPE_PROPERTY) ||
          stepConfig.getString(Runner.TYPE_PROPERTY).equals(Runner.DATA_TYPE))
      {
        if (stepConfig.hasPath(DataStep.INPUT_TYPE)) {
          Config stepInputConfig = stepConfig.getConfig(DataStep.INPUT_TYPE);
          Input stepInput = ComponentFactory.create(Input.class, stepInputConfig, false);

          if (stepInput instanceof BatchInput) {
            LOG.debug("Adding batch step: " + stepName);
            step = new BatchStep(stepName);
          }
          else if (stepInput instanceof StreamInput) {
            LOG.debug("Adding streaming step: " + stepName);
            step = new StreamingStep(stepName);
          }
          else {
            throw new RuntimeException("Invalid step input sub-class for: " + stepName);
          }
        }
        else {
          LOG.debug("Adding batch step: " + stepName);
          step = new BatchStep(stepName);
        }
      }
      else if (stepConfig.getString(Runner.TYPE_PROPERTY).equals(Runner.LOOP_TYPE)) {
        LOG.debug("Adding loop step: " + stepName);
        step = new LoopStep(stepName);
      }
      else if (stepConfig.getString(Runner.TYPE_PROPERTY).equals(Runner.TASK_TYPE)) {
        LOG.debug("Adding task step: " + stepName);
        step = new TaskStep(stepName);
      }
      else if (stepConfig.getString(Runner.TYPE_PROPERTY).equals(Runner.DECISION_TYPE)) {
        LOG.debug("Adding decision step: " + stepName);
        step = new DecisionStep(stepName);
      }
      else {
        throw new RuntimeException("Unknown step type: " + stepConfig.getString(Runner.TYPE_PROPERTY));
      }

      if (configure) {
        step.configure(stepConfig);
        LOG.debug("With configuration: " + stepConfig);
      }

      steps.add(step);
    }

    if (notify) {
      Map<String, Object> metadata = Maps.newHashMap();
      metadata.put(CoreEventMetadataKeys.STEPS_EXTRACTED_CONFIG, config);
      metadata.put(CoreEventMetadataKeys.STEPS_EXTRACTED_STEPS, steps);
      metadata.put(CoreEventMetadataKeys.STEPS_EXTRACTED_TIME_TAKEN_NS, System.nanoTime() - startTime);

      Event event = new Event(
          CoreEventTypes.STEPS_EXTRACTED,
          "Steps extracted: " + StepUtils.stepNamesAsString(steps),
          metadata);

      EventManager.notify(event);
    }

    return steps;
  }

  /**
   * Merges the base steps of the pipeline (as directly defined in the pipeline configuration) that
   * are dependent (directly or indirectly) on the parent step with the steps loaded by the
   * config loader of the pipeline. Steps from the config loader that do not match a base step will
   * be added. Base steps that do not match a step from the config loader will be removed. Steps from
   * the config loader with the same name as base steps will replace the base steps but only if
   * they have a different configuration. If the pipeline does not have a config loader then no
   * merging takes place.
   * @param baseSteps The steps from the pipeline configuration
   * @param parentStep The parent step that steps must be dependent on to be considered for
   *                   replacement
   * @param baseConfig The pipeline configuration, which may contain a config loader that can provide
   *                   mergeable steps
   * @return The merged steps
   */
  public static Set<Step> mergeLoadedSteps(Set<Step> baseSteps, Step parentStep, Config baseConfig) {
    if (ConfigUtils.getApplicationConfig(baseConfig).hasPath(Runner.CONFIG_LOADER_PROPERTY)) {
      Config loaderConfig = ConfigUtils.getApplicationConfig(baseConfig).getConfig(Runner.CONFIG_LOADER_PROPERTY);
      Config loadedConfig = ComponentFactory.create(ConfigLoader.class, loaderConfig, true).getConfig();
      Set<Step> loadedSteps = extractSteps(loadedConfig, true, false);
      Set<Step> mergeableSteps = StepUtils.getAllDependentSteps(parentStep, loadedSteps);

      Set<Step> mergedSteps = Sets.newHashSet(baseSteps);
      Set<Step> stepsToAdd = Sets.newHashSet();
      Set<Step> stepsToRemove = Sets.newHashSet();

      for (Step mergeableStep : mergeableSteps) {
        Optional<Step> correspondingBaseStep =
            StepUtils.getStepForName(mergeableStep.getName(), baseSteps);

        if (correspondingBaseStep.isPresent() &&
            !correspondingBaseStep.get().getConfig().equals(mergeableStep.getConfig())) {
          stepsToAdd.add(mergeableStep);
          stepsToRemove.add(correspondingBaseStep.get());
        } else if (!correspondingBaseStep.isPresent()) {
          stepsToAdd.add(mergeableStep);
        }
      }

      for (Step baseStep : baseSteps) {
        if (StepUtils.getAllDependentSteps(parentStep, baseSteps).contains(baseStep) &&
            !mergeableSteps.contains(baseStep)) {
          stepsToRemove.add(baseStep);
        }
      }

      mergedSteps.removeAll(stepsToRemove);
      mergedSteps.addAll(stepsToAdd);

      LOG.debug(
          "Merged loaded steps into stream batch steps: added ({}), removed ({})",
          StepUtils.stepNamesAsString(stepsToAdd),
          StepUtils.stepNamesAsString(stepsToRemove));

      return mergedSteps;
    }
    else {
      return Sets.newHashSet(baseSteps);
    }
  }

  public static Map<String, StepState> getStepStates(Set<Step> steps) {
    Map<String, StepState> stepStates = Maps.newHashMap();

    for (Step step : steps) {
      stepStates.put(step.getName(), step.getState());
    }

    return stepStates;
  }

  public static Set<Step> getStepsMatchingState(Set<Step> steps, StepState matching) {
    Set<Step> matchingSteps = Sets.newHashSet();

    for (Step step : steps) {
      if (step.getState() == matching) {
        matchingSteps.add(step);
      }
    }

    return matchingSteps;
  }

}
