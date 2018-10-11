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

import com.cloudera.labs.envelope.run.BatchStep;
import com.cloudera.labs.envelope.run.DataStep;
import com.cloudera.labs.envelope.run.LoopStep;
import com.cloudera.labs.envelope.run.Step;
import com.cloudera.labs.envelope.run.StreamingStep;
import com.cloudera.labs.envelope.spark.Contexts;
import com.google.common.collect.Sets;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestStepUtils {

  @Test
  public void testAllStepsSubmitted() {
    Set<Step> steps = Sets.newHashSet();
    BatchStep step1 = new BatchStep("step1");
    BatchStep step2 = new BatchStep("step2");
    step1.configure(ConfigFactory.empty());
    step2.configure(ConfigFactory.empty());
    steps.add(step1);
    steps.add(step2);
    
    step1.setSubmitted(true);
    step2.setSubmitted(true);
    
    assertTrue(StepUtils.allStepsSubmitted(steps));
  }
  
  @Test
  public void testNotAllStepsSubmitted() {
    Set<Step> steps = Sets.newHashSet();
    BatchStep step1 = new BatchStep("step1");
    BatchStep step2 = new BatchStep("step2");
    step1.configure(ConfigFactory.empty());
    step2.configure(ConfigFactory.empty());
    steps.add(step1);
    steps.add(step2);
    
    step1.setSubmitted(true);
    
    assertFalse(StepUtils.allStepsSubmitted(steps));
  }

  @Test
  public void testGetDependencies() {
    Set<Step> steps = Sets.newHashSet();
    BatchStep step1 = new BatchStep("step1");
    BatchStep step2 = new BatchStep("step2");
    BatchStep step3 = new BatchStep("step3");
    step1.configure(ConfigFactory.empty());
    step2.configure(ConfigFactory.empty());
    step3.configure(ConfigFactory.empty());
    steps.add(step1);
    steps.add(step2);
    steps.add(step3);
    
    step2.setDependencyNames(Sets.newHashSet("step1"));
    step3.setDependencyNames(Sets.newHashSet("step1", "step2"));
    
    Set<Step> step1Dependencies = StepUtils.getDependencies(step1, steps);
    Set<Step> step2Dependencies = StepUtils.getDependencies(step2, steps);
    Set<Step> step3Dependencies = StepUtils.getDependencies(step3, steps);
    
    assertEquals(step1Dependencies.size(), 0);
    assertEquals(step2Dependencies.size(), 1);
    assertEquals(step3Dependencies.size(), 2);
    
    assertEquals(step1Dependencies, Sets.newHashSet());
    assertEquals(step2Dependencies, Sets.newHashSet(step1));
    assertEquals(step3Dependencies, Sets.newHashSet(step1, step2));
  }

  @Test
  public void testHasStreamingStep() {
    Set<Step> steps = Sets.newHashSet();
    BatchStep step1 = new BatchStep("step1");
    StreamingStep step2 = new StreamingStep("step2");
    step1.configure(ConfigFactory.empty());
    step2.configure(
        ConfigFactory.empty().withValue("input.translator", ConfigFactory.empty().root()));
    steps.add(step1);
    steps.add(step2);
    
    assertTrue(StepUtils.hasStreamingStep(steps));
  }
  
  @Test
  public void testHasNoStreamingStep() {
    Set<Step> steps = Sets.newHashSet();
    BatchStep step1 = new BatchStep("step1");
    BatchStep step2 = new BatchStep("step2");
    step1.configure(ConfigFactory.empty());
    step2.configure(ConfigFactory.empty());
    steps.add(step1);
    steps.add(step2);
    
    assertFalse(StepUtils.hasStreamingStep(steps));
  }

  @Test
  public void testGetStreamingSteps() {
    Set<Step> steps = Sets.newHashSet();
    BatchStep step1 = new BatchStep("step1");
    StreamingStep step2 = new StreamingStep("step2");
    step1.configure(ConfigFactory.empty());
    step2.configure(
        ConfigFactory.empty().withValue("input.translator", ConfigFactory.empty().root()));
    
    steps.add(step1);
    assertEquals(StepUtils.getStreamingSteps(steps), Sets.newHashSet());
    
    steps.add(step2);
    assertEquals(StepUtils.getStreamingSteps(steps), Sets.newHashSet(step2));
  }

  @Test
  public void testGetAllDependentSteps() {
    Step step1 = new BatchStep("step1");
    Step step2 = new BatchStep("step2");
    Step step3 = new BatchStep("step3");
    Step step4 = new BatchStep("step4");
    Step step5 = new BatchStep("step5");
    Step step6 = new BatchStep("step6");
    step1.configure(ConfigFactory.empty().withValue("dependencies",
        ConfigValueFactory.fromIterable(Sets.newHashSet())));
    step2.configure(ConfigFactory.empty().withValue("dependencies",
        ConfigValueFactory.fromIterable(Sets.newHashSet("step1"))));
    step3.configure(ConfigFactory.empty().withValue("dependencies",
        ConfigValueFactory.fromIterable(Sets.newHashSet("step2"))));
    step4.configure(ConfigFactory.empty().withValue("dependencies",
        ConfigValueFactory.fromIterable(Sets.newHashSet("step3"))));
    step5.configure(ConfigFactory.empty().withValue("dependencies",
        ConfigValueFactory.fromIterable(Sets.newHashSet())));
    step6.configure(ConfigFactory.empty().withValue("dependencies",
        ConfigValueFactory.fromIterable(Sets.newHashSet("step5"))));
    
    Set<Step> steps = Sets.newHashSet(step1, step2, step3, step4, step5, step6);
    
    Set<Step> step1AllDependents = StepUtils.getAllDependentSteps(step1, steps);
    Set<Step> step2AllDependents = StepUtils.getAllDependentSteps(step2, steps);
    Set<Step> step3AllDependents = StepUtils.getAllDependentSteps(step3, steps);
    Set<Step> step4AllDependents = StepUtils.getAllDependentSteps(step4, steps);
    Set<Step> step5AllDependents = StepUtils.getAllDependentSteps(step5, steps);
    Set<Step> step6AllDependents = StepUtils.getAllDependentSteps(step6, steps);

    assertEquals(step1AllDependents.size(), 3);
    assertEquals(step2AllDependents.size(), 2);
    assertEquals(step3AllDependents.size(), 1);
    assertEquals(step4AllDependents.size(), 0);
    assertEquals(step5AllDependents.size(), 1);
    assertEquals(step6AllDependents.size(), 0);
    
    assertEquals(step1AllDependents, Sets.newHashSet(step2, step3, step4));
    assertEquals(step2AllDependents, Sets.newHashSet(step3, step4));
    assertEquals(step3AllDependents, Sets.newHashSet(step4));
    assertEquals(step4AllDependents, Sets.newHashSet());
    assertEquals(step5AllDependents, Sets.newHashSet(step6));
    assertEquals(step6AllDependents, Sets.newHashSet());
  }

  @Test
  public void testGetImmediateDependentSteps() {
    Step step1 = new BatchStep("step1");
    Step step2 = new BatchStep("step2");
    Step step3 = new BatchStep("step3");
    step1.configure(ConfigFactory.empty().withValue("dependencies",
        ConfigValueFactory.fromIterable(Sets.newHashSet())));
    step2.configure(ConfigFactory.empty().withValue("dependencies",
        ConfigValueFactory.fromIterable(Sets.newHashSet("step1"))));
    step3.configure(ConfigFactory.empty().withValue("dependencies",
        ConfigValueFactory.fromIterable(Sets.newHashSet("step2"))));
    Set<Step> steps = Sets.newHashSet(step1, step2, step3);

    Set<Step> step1ImmediateDependents = StepUtils.getImmediateDependentSteps(step1, steps);
    Set<Step> step2ImmediateDependents = StepUtils.getImmediateDependentSteps(step2, steps);
    Set<Step> step3ImmediateDependents = StepUtils.getImmediateDependentSteps(step3, steps);
    
    assertEquals(step1ImmediateDependents.size(), 1);
    assertEquals(step2ImmediateDependents.size(), 1);
    assertEquals(step3ImmediateDependents.size(), 0);
    
    assertEquals(step1ImmediateDependents, Sets.newHashSet(step2));
    assertEquals(step2ImmediateDependents, Sets.newHashSet(step3));
    assertEquals(step3ImmediateDependents, Sets.newHashSet());
  }

  @Test
  public void testGetIndependentNonStreamingSteps() {
    Step step1 = new BatchStep("step1");
    Step step2 = new StreamingStep("step2");
    Step step3 = new BatchStep("step3");
    Step step4 = new StreamingStep("step4");
    step1.configure(ConfigFactory.empty().withValue("dependencies",
        ConfigValueFactory.fromIterable(Sets.newHashSet("step3"))));
    step2.configure(ConfigFactory.empty()
        .withValue("dependencies", ConfigValueFactory.fromIterable(Sets.newHashSet("step4")))
        .withValue("input.translator", ConfigFactory.empty().root()));
    step3.configure(ConfigFactory.empty());
    step4.configure(
        ConfigFactory.empty().withValue("input.translator", ConfigFactory.empty().root()));
    Set<Step> steps = Sets.newHashSet(step1, step2, step3, step4);
    
    assertEquals(StepUtils.getIndependentNonStreamingSteps(steps), Sets.newHashSet(step3));
  }

  @Test
  public void testStepNamesAsString() {
    Step step1 = new BatchStep("step1");
    Step step2 = new BatchStep("step2");
    Step step3 = new BatchStep("step3");
    step1.configure(ConfigFactory.empty());
    step2.configure(ConfigFactory.empty());
    step3.configure(ConfigFactory.empty());
    Set<Step> steps = Sets.newHashSet(step1, step2, step3);
    
    assertTrue(StepUtils.stepNamesAsString(steps).equals("step1, step2, step3") ||
               StepUtils.stepNamesAsString(steps).equals("step1, step3, step2") ||
               StepUtils.stepNamesAsString(steps).equals("step2, step1, step3") ||
               StepUtils.stepNamesAsString(steps).equals("step2, step3, step1") ||
               StepUtils.stepNamesAsString(steps).equals("step3, step1, step2") ||
               StepUtils.stepNamesAsString(steps).equals("step3, step2, step1"));
  }

  @Test
  public void testResetSteps() throws Exception {
    DataStep step1 = new BatchStep("step1");
    DataStep step2 = new BatchStep("step2");
    DataStep step3 = new BatchStep("step3");
    step1.configure(ConfigFactory.empty());
    step2.configure(ConfigFactory.empty());
    step3.configure(ConfigFactory.empty());
    
    step1.setData(Contexts.getSparkSession().emptyDataFrame());
    step2.setData(Contexts.getSparkSession().emptyDataFrame());
    step3.setData(Contexts.getSparkSession().emptyDataFrame());
    
    step1.setSubmitted(true);
    step2.setSubmitted(true);
    step3.setSubmitted(true);
    
    Set<Step> steps = Sets.<Step>newHashSet(step1, step2, step3);
    
    StepUtils.resetSteps(steps);
    
    assertFalse(step1.hasSubmitted());
    assertFalse(step2.hasSubmitted());
    assertFalse(step3.hasSubmitted());
  }

  @Test
  public void testGetDataSteps() {
    Step step1 = new BatchStep("step1");
    Step step2 = new StreamingStep("step2");
    Step step3 = new LoopStep("step3");
    step1.configure(ConfigFactory.empty());
    step2.configure(ConfigFactory.empty().withValue("input.translator", ConfigFactory.empty().root()));
    step3.configure(ConfigFactory.empty());
    Set<Step> steps = Sets.<Step>newHashSet(step1, step2, step3);
    
    assertEquals(StepUtils.getDataSteps(steps), Sets.newHashSet(step1, step2));
  }

  @Test
  public void testGetStepForName() {
    Step step1 = new BatchStep("step1");
    Step step2 = new StreamingStep("step2");
    Step step3 = new LoopStep("step3");
    step1.configure(ConfigFactory.empty());
    step2.configure(ConfigFactory.empty().withValue("input.translator", ConfigFactory.empty().root()));
    step3.configure(ConfigFactory.empty());
    Set<Step> steps = Sets.<Step>newHashSet(step1, step2, step3);
    
    assertEquals(StepUtils.getStepForName("step1", steps).get(), step1);
    assertEquals(StepUtils.getStepForName("step2", steps).get(), step2);
    assertEquals(StepUtils.getStepForName("step3", steps).get(), step3);
  }

  @Test
  public void testCopySteps() {
    Step step1 = new BatchStep("step1");
    Step step2 = new StreamingStep("step2");
    Step step3 = new LoopStep("step3");
    step1.configure(ConfigFactory.empty());
    step2.configure(ConfigFactory.empty().withValue("input.translator", ConfigFactory.empty().root()));
    step3.configure(ConfigFactory.empty());
    Set<Step> steps = Sets.<Step>newHashSet(step1, step2, step3);
    
    Set<Step> copiedSteps = StepUtils.copySteps(steps);
    
    for (Step copiedStep : copiedSteps) {
      assertFalse(steps.contains(copiedStep));
    }
  }
  
}
