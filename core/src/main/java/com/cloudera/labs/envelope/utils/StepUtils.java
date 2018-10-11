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

package com.cloudera.labs.envelope.utils;

import com.cloudera.labs.envelope.repetition.Repetitions;
import com.cloudera.labs.envelope.run.DataStep;
import com.cloudera.labs.envelope.run.Step;
import com.cloudera.labs.envelope.run.StreamingStep;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StepUtils {

  private static final Logger LOG = LoggerFactory.getLogger(StepUtils.class);
  
  public static boolean allStepsSubmitted(Set<Step> steps) {
    for (Step step : steps) {
      if (!step.hasSubmitted()) {
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
    Set<Step> independents = Sets.newHashSet();

    for (Step step : steps) {
      if (!(step instanceof StreamingStep) && step.getDependencyNames().isEmpty()) {
        independents.add(step);
      }
    }

    return independents;
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

}
