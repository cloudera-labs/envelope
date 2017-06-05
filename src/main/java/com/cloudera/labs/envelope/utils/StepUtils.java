/**
 * Copyright © 2016-2017 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.utils;

import java.util.Set;

import com.cloudera.labs.envelope.run.DataStep;
import com.cloudera.labs.envelope.run.Step;
import com.cloudera.labs.envelope.run.StreamingStep;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;

public class StepUtils {
  
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
      sb.append(step.getName() + ", ");
    }

    if (sb.length() > 0) {
      sb.setLength(sb.length() - ", ".length());
    }

    return sb.toString();
  }

  public static void resetDataSteps(Set<Step> steps) {
    for (Step step : steps) {
      if (step instanceof DataStep) {
        ((DataStep)step).clearCache();
        ((DataStep)step).setSubmitted(false);
      }
    }
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
  
}
