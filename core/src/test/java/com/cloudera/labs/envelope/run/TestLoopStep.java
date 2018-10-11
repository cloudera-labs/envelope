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

import com.cloudera.labs.envelope.derive.Deriver;
import com.cloudera.labs.envelope.derive.DeriverFactory;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.utils.StepUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestLoopStep {

  @Test
  public void testRangeValues() {
    Set<Step> steps = Sets.newHashSet();

    Map<String, Object> loopStepConfigMap = Maps.newHashMap();
    loopStepConfigMap.put(LoopStep.MODE_PROPERTY, LoopStep.MODE_PARALLEL);
    loopStepConfigMap.put(LoopStep.SOURCE_PROPERTY, LoopStep.SOURCE_RANGE);
    loopStepConfigMap.put(LoopStep.RANGE_START_PROPERTY, 5);
    loopStepConfigMap.put(LoopStep.RANGE_END_PROPERTY, 7);
    Config loopStepConfig = ConfigFactory.parseMap(loopStepConfigMap);
    RefactorStep loopStep = new LoopStep("loop_step");
    loopStep.configure(loopStepConfig);
    steps.add(loopStep);

    Map<String, Object> step1ConfigMap = Maps.newHashMap();
    step1ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("loop_step"));
    Config step1Config = ConfigFactory.parseMap(step1ConfigMap);
    Step step1 = new BatchStep("step1");
    step1.configure(step1Config);
    steps.add(step1);

    Set<Step> unrolled = loopStep.refactor(steps);

    assertEquals(unrolled.size(), 4);

    assertNotNull(StepUtils.getStepForName("loop_step", unrolled).get());
    assertNotNull(StepUtils.getStepForName("step1_5", unrolled).get());
    assertNotNull(StepUtils.getStepForName("step1_6", unrolled).get());
    assertNotNull(StepUtils.getStepForName("step1_7", unrolled).get());

    assertEquals(StepUtils.getStepForName("loop_step", unrolled).get().getDependencyNames(), Sets.newHashSet());
    assertEquals(StepUtils.getStepForName("step1_5", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
    assertEquals(StepUtils.getStepForName("step1_6", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
    assertEquals(StepUtils.getStepForName("step1_7", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
  }

  @Test
  public void testListIntegerValues() {
    Set<Step> steps = Sets.newHashSet();

    Map<String, Object> loopStepConfigMap = Maps.newHashMap();
    loopStepConfigMap.put(LoopStep.MODE_PROPERTY, LoopStep.MODE_PARALLEL);
    loopStepConfigMap.put(LoopStep.SOURCE_PROPERTY, LoopStep.SOURCE_LIST);
    loopStepConfigMap.put(LoopStep.LIST_PROPERTY, Lists.newArrayList(1, 10, 100));
    Config loopStepConfig = ConfigFactory.parseMap(loopStepConfigMap);
    RefactorStep loopStep = new LoopStep("loop_step");
    loopStep.configure(loopStepConfig);
    steps.add(loopStep);

    Map<String, Object> step1ConfigMap = Maps.newHashMap();
    step1ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("loop_step"));
    Config step1Config = ConfigFactory.parseMap(step1ConfigMap);
    Step step1 = new BatchStep("step1");
    step1.configure(step1Config);
    steps.add(step1);

    Set<Step> unrolled = loopStep.refactor(steps);

    assertEquals(unrolled.size(), 4);

    assertEquals(StepUtils.getStepForName("loop_step", unrolled).get().getDependencyNames(), Sets.newHashSet());
    assertEquals(StepUtils.getStepForName("step1_1", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
    assertEquals(StepUtils.getStepForName("step1_10", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
    assertEquals(StepUtils.getStepForName("step1_100", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
  }

  @Test
  public void testListStringValues() {
    Set<Step> steps = Sets.newHashSet();

    Map<String, Object> loopStepConfigMap = Maps.newHashMap();
    loopStepConfigMap.put(LoopStep.MODE_PROPERTY, LoopStep.MODE_PARALLEL);
    loopStepConfigMap.put(LoopStep.SOURCE_PROPERTY, LoopStep.SOURCE_LIST);
    loopStepConfigMap.put(LoopStep.LIST_PROPERTY, Lists.newArrayList("hello", "world", "bloop", "gloop"));
    Config loopStepConfig = ConfigFactory.parseMap(loopStepConfigMap);
    RefactorStep loopStep = new LoopStep("loop_step");
    loopStep.configure(loopStepConfig);
    steps.add(loopStep);

    Map<String, Object> step1ConfigMap = Maps.newHashMap();
    step1ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("loop_step"));
    Config step1Config = ConfigFactory.parseMap(step1ConfigMap);
    Step step1 = new BatchStep("step1");
    step1.configure(step1Config);
    steps.add(step1);

    Set<Step> unrolled = loopStep.refactor(steps);

    assertEquals(unrolled.size(), 5);

    assertEquals(StepUtils.getStepForName("loop_step", unrolled).get().getDependencyNames(), Sets.newHashSet());
    assertEquals(StepUtils.getStepForName("step1_hello", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
    assertEquals(StepUtils.getStepForName("step1_world", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
    assertEquals(StepUtils.getStepForName("step1_bloop", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
    assertEquals(StepUtils.getStepForName("step1_gloop", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
  }

  @Test
  public void testStepValues() throws Exception {
    Set<Step> steps = Sets.newHashSet();

    Map<String, Object> sourceStepConfigMap = Maps.newHashMap();
    Config sourceStepConfig = ConfigFactory.parseMap(sourceStepConfigMap);
    BatchStep sourceStep = new BatchStep("source_step");
    sourceStep.configure(sourceStepConfig);
    Dataset<Row> sourceDF = Contexts.getSparkSession().range(5, 8).map(new LongToRowFunction(),
        RowEncoder.apply(DataTypes.createStructType(Lists.newArrayList(DataTypes.createStructField("value", DataTypes.LongType, false)))));
    sourceStep.setData(sourceDF);
    steps.add(sourceStep);

    Map<String, Object> loopStepConfigMap = Maps.newHashMap();
    loopStepConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("source_step"));
    loopStepConfigMap.put(LoopStep.MODE_PROPERTY, LoopStep.MODE_PARALLEL);
    loopStepConfigMap.put(LoopStep.SOURCE_PROPERTY, LoopStep.SOURCE_STEP);
    loopStepConfigMap.put(LoopStep.STEP_PROPERTY, "source_step");
    Config loopStepConfig = ConfigFactory.parseMap(loopStepConfigMap);
    RefactorStep loopStep = new LoopStep("loop_step");
    loopStep.configure(loopStepConfig);
    steps.add(loopStep);

    Map<String, Object> step1ConfigMap = Maps.newHashMap();
    step1ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("loop_step"));
    Config step1Config = ConfigFactory.parseMap(step1ConfigMap);
    Step step1 = new BatchStep("step1");
    step1.configure(step1Config);
    steps.add(step1);

    Set<Step> unrolled = loopStep.refactor(steps);

    assertEquals(unrolled.size(), 5);

    assertEquals(StepUtils.getStepForName("source_step", unrolled).get().getDependencyNames(), Sets.newHashSet());
    assertEquals(StepUtils.getStepForName("loop_step", unrolled).get().getDependencyNames(), Sets.newHashSet("source_step"));
    assertEquals(StepUtils.getStepForName("step1_5", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
    assertEquals(StepUtils.getStepForName("step1_6", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
    assertEquals(StepUtils.getStepForName("step1_7", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
  }

  @Test
  public void testGraphOneLoopOnce() {
    Set<Step> steps = Sets.newHashSet();

    Map<String, Object> loopStepConfigMap = Maps.newHashMap();
    loopStepConfigMap.put(LoopStep.MODE_PROPERTY, LoopStep.MODE_PARALLEL);
    loopStepConfigMap.put(LoopStep.SOURCE_PROPERTY, LoopStep.SOURCE_RANGE);
    loopStepConfigMap.put(LoopStep.RANGE_START_PROPERTY, 5);
    loopStepConfigMap.put(LoopStep.RANGE_END_PROPERTY, 5);
    Config loopStepConfig = ConfigFactory.parseMap(loopStepConfigMap);
    RefactorStep loopStep = new LoopStep("loop_step");
    loopStep.configure(loopStepConfig);
    steps.add(loopStep);

    Map<String, Object> step1ConfigMap = Maps.newHashMap();
    step1ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("loop_step"));
    Config step1Config = ConfigFactory.parseMap(step1ConfigMap);
    Step step1 = new BatchStep("step1");
    step1.configure(step1Config);
    steps.add(step1);

    Set<Step> unrolled = loopStep.refactor(steps);

    assertEquals(unrolled.size(), 2);

    assertEquals(StepUtils.getStepForName("loop_step", unrolled).get().getDependencyNames(), Sets.newHashSet());
    assertEquals(StepUtils.getStepForName("step1_5", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
  }

  @Test
  public void testGraphMultipleLoopOnce() {
    Set<Step> steps = Sets.newHashSet();

    Map<String, Object> loopStepConfigMap = Maps.newHashMap();
    loopStepConfigMap.put(LoopStep.MODE_PROPERTY, LoopStep.MODE_PARALLEL);
    loopStepConfigMap.put(LoopStep.SOURCE_PROPERTY, LoopStep.SOURCE_RANGE);
    loopStepConfigMap.put(LoopStep.RANGE_START_PROPERTY, 100);
    loopStepConfigMap.put(LoopStep.RANGE_END_PROPERTY, 100);
    Config loopStepConfig = ConfigFactory.parseMap(loopStepConfigMap);
    RefactorStep loopStep = new LoopStep("loop_step");
    loopStep.configure(loopStepConfig);
    steps.add(loopStep);

    Map<String, Object> step1ConfigMap = Maps.newHashMap();
    step1ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("loop_step"));
    Config step1Config = ConfigFactory.parseMap(step1ConfigMap);
    Step step1 = new BatchStep("step1");
    step1.configure(step1Config);
    steps.add(step1);

    Map<String, Object> step2ConfigMap = Maps.newHashMap();
    step2ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("loop_step"));
    Config step2Config = ConfigFactory.parseMap(step2ConfigMap);
    Step step2 = new BatchStep("step2");
    step2.configure(step2Config);
    steps.add(step2);

    Map<String, Object> step3ConfigMap = Maps.newHashMap();
    step3ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("loop_step", "step1", "step2"));
    Config step3Config = ConfigFactory.parseMap(step3ConfigMap);
    Step step3 = new BatchStep("step3");
    step3.configure(step3Config);
    steps.add(step3);

    Map<String, Object> step4ConfigMap = Maps.newHashMap();
    step4ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("loop_step", "step3"));
    Config step4Config = ConfigFactory.parseMap(step4ConfigMap);
    Step step4 = new BatchStep("step4");
    step4.configure(step4Config);
    steps.add(step4);

    Map<String, Object> step5ConfigMap = Maps.newHashMap();
    step5ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("loop_step", "step3"));
    Config step5Config = ConfigFactory.parseMap(step5ConfigMap);
    Step step5 = new BatchStep("step5");
    step5.configure(step5Config);
    steps.add(step5);

    Set<Step> unrolled = loopStep.refactor(steps);

    assertEquals(unrolled.size(), 6);

    assertEquals(StepUtils.getStepForName("loop_step", unrolled).get().getDependencyNames(), Sets.newHashSet());
    assertEquals(StepUtils.getStepForName("step1_100", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
    assertEquals(StepUtils.getStepForName("step2_100", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
    assertEquals(StepUtils.getStepForName("step3_100", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step", "step1_100", "step2_100"));
    assertEquals(StepUtils.getStepForName("step4_100", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step", "step3_100"));
    assertEquals(StepUtils.getStepForName("step5_100", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step", "step3_100"));
  }

  @Test
  public void testGraphOneLoopMany() {
    Set<Step> steps = Sets.newHashSet();

    Map<String, Object> loopStepConfigMap = Maps.newHashMap();
    loopStepConfigMap.put(LoopStep.MODE_PROPERTY, LoopStep.MODE_PARALLEL);
    loopStepConfigMap.put(LoopStep.SOURCE_PROPERTY, LoopStep.SOURCE_RANGE);
    loopStepConfigMap.put(LoopStep.RANGE_START_PROPERTY, 5);
    loopStepConfigMap.put(LoopStep.RANGE_END_PROPERTY, 7);
    Config loopStepConfig = ConfigFactory.parseMap(loopStepConfigMap);
    RefactorStep loopStep = new LoopStep("loop_step");
    loopStep.configure(loopStepConfig);
    steps.add(loopStep);

    Map<String, Object> step1ConfigMap = Maps.newHashMap();
    step1ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("loop_step"));
    Config step1Config = ConfigFactory.parseMap(step1ConfigMap);
    Step step1 = new BatchStep("step1");
    step1.configure(step1Config);
    steps.add(step1);

    Set<Step> unrolled = loopStep.refactor(steps);

    assertEquals(unrolled.size(), 4);

    assertEquals(StepUtils.getStepForName("loop_step", unrolled).get().getDependencyNames(), Sets.newHashSet());
    assertEquals(StepUtils.getStepForName("step1_5", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
    assertEquals(StepUtils.getStepForName("step1_6", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
    assertEquals(StepUtils.getStepForName("step1_7", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
  }

  @Test
  public void testGraphMultipleLoopMany() {
    Set<Step> steps = Sets.newHashSet();

    Map<String, Object> loopStepConfigMap = Maps.newHashMap();
    loopStepConfigMap.put(LoopStep.MODE_PROPERTY, LoopStep.MODE_PARALLEL);
    loopStepConfigMap.put(LoopStep.SOURCE_PROPERTY, LoopStep.SOURCE_RANGE);
    loopStepConfigMap.put(LoopStep.RANGE_START_PROPERTY, 100);
    loopStepConfigMap.put(LoopStep.RANGE_END_PROPERTY, 102);
    Config loopStepConfig = ConfigFactory.parseMap(loopStepConfigMap);
    RefactorStep loopStep = new LoopStep("loop_step");
    loopStep.configure(loopStepConfig);
    steps.add(loopStep);

    Map<String, Object> step1ConfigMap = Maps.newHashMap();
    step1ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("loop_step"));
    Config step1Config = ConfigFactory.parseMap(step1ConfigMap);
    Step step1 = new BatchStep("step1");
    step1.configure(step1Config);
    steps.add(step1);

    Map<String, Object> step2ConfigMap = Maps.newHashMap();
    step2ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("loop_step"));
    Config step2Config = ConfigFactory.parseMap(step2ConfigMap);
    Step step2 = new BatchStep("step2");
    step2.configure(step2Config);
    steps.add(step2);

    Map<String, Object> step3ConfigMap = Maps.newHashMap();
    step3ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("loop_step", "step1", "step2"));
    Config step3Config = ConfigFactory.parseMap(step3ConfigMap);
    Step step3 = new BatchStep("step3");
    step3.configure(step3Config);
    steps.add(step3);

    Map<String, Object> step4ConfigMap = Maps.newHashMap();
    step4ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("loop_step", "step3"));
    Config step4Config = ConfigFactory.parseMap(step4ConfigMap);
    Step step4 = new BatchStep("step4");
    step4.configure(step4Config);
    steps.add(step4);

    Map<String, Object> step5ConfigMap = Maps.newHashMap();
    step5ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("loop_step", "step3"));
    Config step5Config = ConfigFactory.parseMap(step5ConfigMap);
    Step step5 = new BatchStep("step5");
    step5.configure(step5Config);
    steps.add(step5);

    Set<Step> unrolled = loopStep.refactor(steps);

    assertEquals(unrolled.size(), 16);

    assertEquals(StepUtils.getStepForName("loop_step", unrolled).get().getDependencyNames(), Sets.newHashSet());
    assertEquals(StepUtils.getStepForName("step1_100", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
    assertEquals(StepUtils.getStepForName("step2_100", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
    assertEquals(StepUtils.getStepForName("step3_100", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step", "step1_100", "step2_100"));
    assertEquals(StepUtils.getStepForName("step4_100", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step", "step3_100"));
    assertEquals(StepUtils.getStepForName("step5_100", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step", "step3_100"));
    assertEquals(StepUtils.getStepForName("step1_101", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
    assertEquals(StepUtils.getStepForName("step2_101", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
    assertEquals(StepUtils.getStepForName("step3_101", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step", "step1_101", "step2_101"));
    assertEquals(StepUtils.getStepForName("step4_101", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step", "step3_101"));
    assertEquals(StepUtils.getStepForName("step5_101", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step", "step3_101"));
    assertEquals(StepUtils.getStepForName("step1_102", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
    assertEquals(StepUtils.getStepForName("step2_102", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
    assertEquals(StepUtils.getStepForName("step3_102", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step", "step1_102", "step2_102"));
    assertEquals(StepUtils.getStepForName("step4_102", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step", "step3_102"));
    assertEquals(StepUtils.getStepForName("step5_102", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step", "step3_102"));
  }

  @Test
  public void testParallelMode() {
    Set<Step> steps = Sets.newHashSet();

    Map<String, Object> loopStepConfigMap = Maps.newHashMap();
    loopStepConfigMap.put(LoopStep.MODE_PROPERTY, LoopStep.MODE_PARALLEL);
    loopStepConfigMap.put(LoopStep.SOURCE_PROPERTY, LoopStep.SOURCE_RANGE);
    loopStepConfigMap.put(LoopStep.RANGE_START_PROPERTY, 5);
    loopStepConfigMap.put(LoopStep.RANGE_END_PROPERTY, 7);
    Config loopStepConfig = ConfigFactory.parseMap(loopStepConfigMap);
    RefactorStep loopStep = new LoopStep("loop_step");
    loopStep.configure(loopStepConfig);
    steps.add(loopStep);

    Map<String, Object> step1ConfigMap = Maps.newHashMap();
    step1ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("loop_step"));
    Config step1Config = ConfigFactory.parseMap(step1ConfigMap);
    Step step1 = new BatchStep("step1");
    step1.configure(step1Config);
    steps.add(step1);

    Set<Step> unrolled = loopStep.refactor(steps);

    assertEquals(unrolled.size(), 4);

    assertEquals(StepUtils.getStepForName("loop_step", unrolled).get().getDependencyNames(), Sets.newHashSet());
    assertEquals(StepUtils.getStepForName("step1_5", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
    assertEquals(StepUtils.getStepForName("step1_6", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
    assertEquals(StepUtils.getStepForName("step1_7", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
  }

  @Test
  public void testSerialMode() {
    Set<Step> steps = Sets.newHashSet();

    Map<String, Object> loopStepConfigMap = Maps.newHashMap();
    loopStepConfigMap.put(LoopStep.MODE_PROPERTY, LoopStep.MODE_SERIAL);
    loopStepConfigMap.put(LoopStep.SOURCE_PROPERTY, LoopStep.SOURCE_RANGE);
    loopStepConfigMap.put(LoopStep.RANGE_START_PROPERTY, 5);
    loopStepConfigMap.put(LoopStep.RANGE_END_PROPERTY, 7);
    Config loopStepConfig = ConfigFactory.parseMap(loopStepConfigMap);
    RefactorStep loopStep = new LoopStep("loop_step");
    loopStep.configure(loopStepConfig);
    steps.add(loopStep);

    Map<String, Object> step1ConfigMap = Maps.newHashMap();
    step1ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("loop_step"));
    Config step1Config = ConfigFactory.parseMap(step1ConfigMap);
    Step step1 = new BatchStep("step1");
    step1.configure(step1Config);
    steps.add(step1);

    Set<Step> unrolled = loopStep.refactor(steps);

    assertEquals(unrolled.size(), 4);

    assertEquals(StepUtils.getStepForName("loop_step", unrolled).get().getDependencyNames(), Sets.newHashSet());
    assertEquals(StepUtils.getStepForName("step1_5", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
    assertEquals(StepUtils.getStepForName("step1_6", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step", "step1_5"));
    assertEquals(StepUtils.getStepForName("step1_7", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step", "step1_6"));
  }

  @Test
  public void testAfterLoop() {
    Set<Step> steps = Sets.newHashSet();

    Map<String, Object> loopStepConfigMap = Maps.newHashMap();
    loopStepConfigMap.put(LoopStep.MODE_PROPERTY, LoopStep.MODE_PARALLEL);
    loopStepConfigMap.put(LoopStep.SOURCE_PROPERTY, LoopStep.SOURCE_RANGE);
    loopStepConfigMap.put(LoopStep.RANGE_START_PROPERTY, 5);
    loopStepConfigMap.put(LoopStep.RANGE_END_PROPERTY, 7);
    Config loopStepConfig = ConfigFactory.parseMap(loopStepConfigMap);
    RefactorStep loopStep = new LoopStep("loop_step");
    loopStep.configure(loopStepConfig);
    steps.add(loopStep);

    Map<String, Object> step1ConfigMap = Maps.newHashMap();
    step1ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("loop_step"));
    Config step1Config = ConfigFactory.parseMap(step1ConfigMap);
    Step step1 = new BatchStep("step1");
    step1.configure(step1Config);
    steps.add(step1);

    Map<String, Object> afterLoopConfigMap = Maps.newHashMap();
    afterLoopConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("step1"));
    Config afterLoopConfig = ConfigFactory.parseMap(afterLoopConfigMap);
    Step afterLoop = new BatchStep("after_loop");
    afterLoop.configure(afterLoopConfig);
    steps.add(afterLoop);

    Map<String, Object> afterAfterLoopConfigMap = Maps.newHashMap();
    afterAfterLoopConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("after_loop"));
    Config afterAfterLoopConfig = ConfigFactory.parseMap(afterAfterLoopConfigMap);
    Step afterAfterLoop = new BatchStep("after_after_loop");
    afterAfterLoop.configure(afterAfterLoopConfig);
    steps.add(afterAfterLoop);

    Set<Step> unrolled = loopStep.refactor(steps);

    assertEquals(unrolled.size(), 6);

    assertEquals(StepUtils.getStepForName("loop_step", unrolled).get().getDependencyNames(), Sets.newHashSet());
    assertEquals(StepUtils.getStepForName("step1_5", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
    assertEquals(StepUtils.getStepForName("step1_6", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
    assertEquals(StepUtils.getStepForName("step1_7", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
    assertEquals(StepUtils.getStepForName("after_loop", unrolled).get().getDependencyNames(), Sets.newHashSet("step1_5", "step1_6", "step1_7"));
    assertEquals(StepUtils.getStepForName("after_after_loop", unrolled).get().getDependencyNames(), Sets.newHashSet("after_loop"));
  }

  @Test
  public void testWithoutParameter() {
    Set<Step> steps = Sets.newHashSet();

    Map<String, Object> loopStepConfigMap = Maps.newHashMap();
    loopStepConfigMap.put(LoopStep.MODE_PROPERTY, LoopStep.MODE_PARALLEL);
    loopStepConfigMap.put(LoopStep.SOURCE_PROPERTY, LoopStep.SOURCE_RANGE);
    loopStepConfigMap.put(LoopStep.RANGE_START_PROPERTY, 5);
    loopStepConfigMap.put(LoopStep.RANGE_END_PROPERTY, 5);
    Config loopStepConfig = ConfigFactory.parseMap(loopStepConfigMap);
    RefactorStep loopStep = new LoopStep("loop_step");
    loopStep.configure(loopStepConfig);
    steps.add(loopStep);

    Map<String, Object> step1ConfigMap = Maps.newHashMap();
    step1ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("loop_step"));
    Config step1Config = ConfigFactory.parseMap(step1ConfigMap);
    Step step1 = new BatchStep("step1");
    step1.configure(step1Config);
    steps.add(step1);

    Set<Step> unrolled = loopStep.refactor(steps);

    assertEquals(unrolled.size(), 2);

    assertEquals(StepUtils.getStepForName("loop_step", unrolled).get().getDependencyNames(), Sets.newHashSet());
    assertEquals(StepUtils.getStepForName("step1_5", unrolled).get().getDependencyNames(), Sets.newHashSet("loop_step"));
  }

  @Test
  public void testWithParameter() throws Exception {
    Set<Step> steps = Sets.newHashSet();

    Map<String, Object> loopStepConfigMap = Maps.newHashMap();
    loopStepConfigMap.put(LoopStep.MODE_PROPERTY, LoopStep.MODE_PARALLEL);
    loopStepConfigMap.put(LoopStep.SOURCE_PROPERTY, LoopStep.SOURCE_RANGE);
    loopStepConfigMap.put(LoopStep.RANGE_START_PROPERTY, 5);
    loopStepConfigMap.put(LoopStep.RANGE_END_PROPERTY, 7);
    loopStepConfigMap.put(LoopStep.PARAMETER_PROPERTY, "loop_value");
    Config loopStepConfig = ConfigFactory.parseMap(loopStepConfigMap);
    RefactorStep loopStep = new LoopStep("loop_step");
    loopStep.configure(loopStepConfig);
    steps.add(loopStep);

    Map<String, Object> step1ConfigMap = Maps.newHashMap();
    step1ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("loop_step"));
    step1ConfigMap.put("deriver." + DeriverFactory.TYPE_CONFIG_NAME, SQLDeriver.class.getName());
    step1ConfigMap.put("deriver.query.literal", "SELECT ${loop_value}");

    Config step1Config = ConfigFactory.parseMap(step1ConfigMap);
    Step step1 = new BatchStep("step1");
    step1.configure(step1Config);
    steps.add(step1);

    Set<Step> unrolled = loopStep.refactor(steps);

    assertEquals(unrolled.size(), 4);

    BatchStep step1_5 = (BatchStep)StepUtils.getStepForName("step1_5", unrolled).get();
    BatchStep step1_6 = (BatchStep)StepUtils.getStepForName("step1_6", unrolled).get();
    BatchStep step1_7 = (BatchStep)StepUtils.getStepForName("step1_7", unrolled).get();

    assertNotNull(StepUtils.getStepForName("loop_step", unrolled).get());
    assertNotNull(step1_5);
    assertNotNull(step1_6);
    assertNotNull(step1_7);

    step1_5.submit(Sets.<Step>newHashSet());
    step1_6.submit(Sets.<Step>newHashSet());
    step1_7.submit(Sets.<Step>newHashSet());

    assertEquals(step1_5.getData().collectAsList().get(0), RowFactory.create(5));
    assertEquals(step1_6.getData().collectAsList().get(0), RowFactory.create(6));
    assertEquals(step1_7.getData().collectAsList().get(0), RowFactory.create(7));
  }

  @Test
  public void testLoopWithinLoop() {
    Map<String, Object> step1ConfigMap = Maps.newHashMap();
    step1ConfigMap.put(LoopStep.MODE_PROPERTY, LoopStep.MODE_PARALLEL);
    step1ConfigMap.put(LoopStep.SOURCE_PROPERTY, LoopStep.SOURCE_RANGE);
    step1ConfigMap.put(LoopStep.RANGE_START_PROPERTY, 10);
    step1ConfigMap.put(LoopStep.RANGE_END_PROPERTY, 11);
    Config step1Config = ConfigFactory.parseMap(step1ConfigMap);
    RefactorStep step1 = new LoopStep("loop_step1");
    step1.configure(step1Config);

    Map<String, Object> step2ConfigMap = Maps.newHashMap();
    step2ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("loop_step1"));
    Config step2Config = ConfigFactory.parseMap(step2ConfigMap);
    Step step2 = new BatchStep("data_step2");
    step2.configure(step2Config);

    Map<String, Object> step3ConfigMap = Maps.newHashMap();
    step3ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("loop_step1"));
    step3ConfigMap.put(LoopStep.MODE_PROPERTY, LoopStep.MODE_PARALLEL);
    step3ConfigMap.put(LoopStep.SOURCE_PROPERTY, LoopStep.SOURCE_RANGE);
    step3ConfigMap.put(LoopStep.RANGE_START_PROPERTY, 12);
    step3ConfigMap.put(LoopStep.RANGE_END_PROPERTY, 13);
    Config step3Config = ConfigFactory.parseMap(step3ConfigMap);
    RefactorStep step3 = new LoopStep("loop_step3");
    step3.configure(step3Config);

    Map<String, Object> step4ConfigMap = Maps.newHashMap();
    step4ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("loop_step1", "loop_step3"));
    Config step4Config = ConfigFactory.parseMap(step4ConfigMap);
    Step step4 = new BatchStep("data_step4");
    step4.configure(step4Config);

    Map<String, Object> step5ConfigMap = Maps.newHashMap();
    step5ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("loop_step1", "loop_step3"));
    Config step5Config = ConfigFactory.parseMap(step5ConfigMap);
    Step step5 = new BatchStep("data_step5");
    step5.configure(step5Config);

    Map<String, Object> step6ConfigMap = Maps.newHashMap();
    step6ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("data_step5"));
    Config step6Config = ConfigFactory.parseMap(step6ConfigMap);
    Step step6 = new BatchStep("data_step6");
    step6.configure(step6Config);

    Set<Step> steps = Sets.newHashSet(step1, step2, step3, step4, step5, step6);

    // Unroll the top level loop
    Set<Step> unrolled = step1.refactor(steps);
    // Unroll the unrolled two bottom level loops
    for (Step unrolledStep : unrolled) {
      if (unrolledStep instanceof LoopStep && !unrolledStep.hasSubmitted()) {
        unrolled = ((LoopStep)unrolledStep).refactor(unrolled);
        break;
      }
    }
    for (Step unrolledStep : unrolled) {
      if (unrolledStep instanceof LoopStep && !unrolledStep.hasSubmitted()) {
        unrolled = ((LoopStep)unrolledStep).refactor(unrolled);
        break;
      }
    }

    assertEquals(unrolled.size(), 14);

    assertTrue(StepUtils.getStepForName("loop_step1", unrolled).isPresent());
    assertTrue(StepUtils.getStepForName("data_step2_10", unrolled).isPresent());
    assertTrue(StepUtils.getStepForName("data_step2_11", unrolled).isPresent());
    assertTrue(StepUtils.getStepForName("loop_step3_10", unrolled).isPresent());
    assertTrue(StepUtils.getStepForName("loop_step3_11", unrolled).isPresent());
    assertTrue(StepUtils.getStepForName("data_step4_10_12", unrolled).isPresent());
    assertTrue(StepUtils.getStepForName("data_step4_10_13", unrolled).isPresent());
    assertTrue(StepUtils.getStepForName("data_step4_11_12", unrolled).isPresent());
    assertTrue(StepUtils.getStepForName("data_step4_11_13", unrolled).isPresent());
    assertTrue(StepUtils.getStepForName("data_step5_10_12", unrolled).isPresent());
    assertTrue(StepUtils.getStepForName("data_step5_10_13", unrolled).isPresent());
    assertTrue(StepUtils.getStepForName("data_step5_11_12", unrolled).isPresent());
    assertTrue(StepUtils.getStepForName("data_step5_11_13", unrolled).isPresent());
    assertTrue(StepUtils.getStepForName("data_step6", unrolled).isPresent());
  }

  @SuppressWarnings("serial")
  private static class LongToRowFunction implements MapFunction<Long, Row> {
    @Override
    public Row call(Long longValue) throws Exception {
      return RowFactory.create(longValue);
    }
  }

  public static class SQLDeriver implements Deriver {
    private Config config;

    @Override
    public void configure(Config config) {
      this.config = config;
    }

    @Override
    public Dataset<Row> derive(Map<String, Dataset<Row>> dependencies) throws Exception {
      String query = config.getString("query.literal");
      Dataset<Row> derived = Contexts.getSparkSession().sql(query);
      return derived;
    }
  }

}

