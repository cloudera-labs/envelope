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

import com.cloudera.labs.envelope.spark.Contexts;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class TestDecisionStep {

  private BatchStep step1, step3, step4, step5, step6, step7, step8;
  private Set<Step> steps = Sets.newHashSet();

  @Before
  public void initializeSteps() {
    /*
     * Batch -> Decision -true> Batch -> Batch
     *                   -false> Batch -> Batch
     *                   -true> Batch -> Batch
     */

    Map<String, Object> step1ConfigMap = Maps.newHashMap();
    step1ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList());
    step1ConfigMap.put(DataStep.CACHE_ENABLED_PROPERTY, false);
    step1ConfigMap.put(DataStep.CACHE_STORAGE_LEVEL_PROPERTY, "MEMORY_ONLY");
    Config step1Config = ConfigFactory.parseMap(step1ConfigMap);
    step1 = new BatchStep("step1");
    step1.configure(step1Config);
    steps.add(step1);

    // step2 is the decision step added in the tests

    Map<String, Object> step3ConfigMap = Maps.newHashMap();
    step3ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("step2"));
    Config step3Config = ConfigFactory.parseMap(step3ConfigMap);
    step3 = new BatchStep("step3");
    step3.configure(step3Config);
    steps.add(step3);

    Map<String, Object> step4ConfigMap = Maps.newHashMap();
    step4ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("step3"));
    Config step4Config = ConfigFactory.parseMap(step4ConfigMap);
    step4 = new BatchStep("step4");
    step4.configure(step4Config);
    steps.add(step4);

    Map<String, Object> step5ConfigMap = Maps.newHashMap();
    step5ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("step2"));
    Config step5Config = ConfigFactory.parseMap(step5ConfigMap);
    step5 = new BatchStep("step5");
    step5.configure(step5Config);
    steps.add(step5);

    Map<String, Object> step6ConfigMap = Maps.newHashMap();
    step6ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("step5"));
    Config step6Config = ConfigFactory.parseMap(step6ConfigMap);
    step6 = new BatchStep("step6");
    step6.configure(step6Config);
    steps.add(step6);

    Map<String, Object> step7ConfigMap = Maps.newHashMap();
    step7ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("step2"));
    Config step7Config = ConfigFactory.parseMap(step7ConfigMap);
    step7 = new BatchStep("step7");
    step7.configure(step7Config);
    steps.add(step7);

    Map<String, Object> step8ConfigMap = Maps.newHashMap();
    step8ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("step7"));
    Config step8Config = ConfigFactory.parseMap(step8ConfigMap);
    step8 = new BatchStep("step8");
    step8.configure(step8Config);
    steps.add(step8);
  }

  @Test
  public void testPruneByLiteralTrue() {
    Map<String, Object> step2ConfigMap = Maps.newHashMap();
    step2ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("step1"));
    step2ConfigMap.put(DecisionStep.IF_TRUE_STEP_NAMES_PROPERTY, Lists.newArrayList("step3", "step7"));
    step2ConfigMap.put(DecisionStep.DECISION_METHOD_PROPERTY, DecisionStep.LITERAL_DECISION_METHOD);
    step2ConfigMap.put(DecisionStep.LITERAL_RESULT_PROPERTY, true);
    Config step2Config = ConfigFactory.parseMap(step2ConfigMap);
    RefactorStep step2 = new DecisionStep("step2");
    step2.configure(step2Config);
    steps.add(step2);

    Set<Step> refactored = step2.refactor(steps);

    assertEquals(refactored, Sets.newHashSet(step1, step2, step3, step4, step7, step8));
  }

  @Test
  public void testPruneByLiteralFalse() {
    Map<String, Object> step2ConfigMap = Maps.newHashMap();
    step2ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("step1"));
    step2ConfigMap.put(DecisionStep.IF_TRUE_STEP_NAMES_PROPERTY, Lists.newArrayList("step3", "step7"));
    step2ConfigMap.put(DecisionStep.DECISION_METHOD_PROPERTY, DecisionStep.LITERAL_DECISION_METHOD);
    step2ConfigMap.put(DecisionStep.LITERAL_RESULT_PROPERTY, false);
    Config step2Config = ConfigFactory.parseMap(step2ConfigMap);
    RefactorStep step2 = new DecisionStep("step2");
    step2.configure(step2Config);
    steps.add(step2);

    Set<Step> refactored = step2.refactor(steps);

    assertEquals(refactored, Sets.newHashSet(step1, step2, step5, step6));
  }

  @Test
  public void testPruneByStepKeyTrue() {
    StructType schema = new StructType(new StructField[] {
        new StructField("name", DataTypes.StringType, false, Metadata.empty()),
        new StructField("result", DataTypes.BooleanType, false, Metadata.empty())
    });
    List<Row> rows = Lists.newArrayList(
        RowFactory.create("namecheck", false),
        RowFactory.create("agerange", true)
    );
    Dataset<Row> ds = Contexts.getSparkSession().createDataFrame(rows, schema);
    step1.setData(ds);

    Map<String, Object> step2ConfigMap = Maps.newHashMap();
    step2ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("step1"));
    step2ConfigMap.put(DecisionStep.IF_TRUE_STEP_NAMES_PROPERTY, Lists.newArrayList("step3", "step7"));
    step2ConfigMap.put(DecisionStep.DECISION_METHOD_PROPERTY, DecisionStep.STEP_BY_KEY_DECISION_METHOD);
    step2ConfigMap.put(DecisionStep.STEP_BY_KEY_STEP_PROPERTY, "step1");
    step2ConfigMap.put(DecisionStep.STEP_BY_KEY_KEY_PROPERTY, "agerange");
    Config step2Config = ConfigFactory.parseMap(step2ConfigMap);
    RefactorStep step2 = new DecisionStep("step2");
    step2.configure(step2Config);
    steps.add(step2);

    Set<Step> refactored = step2.refactor(steps);

    assertEquals(refactored, Sets.newHashSet(step1, step2, step3, step4, step7, step8));
  }

  @Test
  public void testPruneByStepKeyFalse() {
    StructType schema = new StructType(new StructField[] {
        new StructField("name", DataTypes.StringType, false, Metadata.empty()),
        new StructField("result", DataTypes.BooleanType, false, Metadata.empty())
    });
    List<Row> rows = Lists.newArrayList(
        RowFactory.create("namecheck", false),
        RowFactory.create("agerange", true)
    );
    Dataset<Row> ds = Contexts.getSparkSession().createDataFrame(rows, schema);
    step1.setData(ds);

    Map<String, Object> step2ConfigMap = Maps.newHashMap();
    step2ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("step1"));
    step2ConfigMap.put(DecisionStep.IF_TRUE_STEP_NAMES_PROPERTY, Lists.newArrayList("step3", "step7"));
    step2ConfigMap.put(DecisionStep.DECISION_METHOD_PROPERTY, DecisionStep.STEP_BY_KEY_DECISION_METHOD);
    step2ConfigMap.put(DecisionStep.STEP_BY_KEY_STEP_PROPERTY, "step1");
    step2ConfigMap.put(DecisionStep.STEP_BY_KEY_KEY_PROPERTY, "namecheck");
    Config step2Config = ConfigFactory.parseMap(step2ConfigMap);
    RefactorStep step2 = new DecisionStep("step2");
    step2.configure(step2Config);
    steps.add(step2);

    Set<Step> refactored = step2.refactor(steps);

    assertEquals(refactored, Sets.newHashSet(step1, step2, step5, step6));
  }

  @Test
  public void testPruneByStepValueTrue() {
    StructType schema = new StructType(new StructField[] {
        new StructField("outcome", DataTypes.BooleanType, false, Metadata.empty())
    });
    List<Row> rows = Lists.newArrayList(
        RowFactory.create(true)
    );
    Dataset<Row> ds = Contexts.getSparkSession().createDataFrame(rows, schema);
    step1.setData(ds);

    Map<String, Object> step2ConfigMap = Maps.newHashMap();
    step2ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("step1"));
    step2ConfigMap.put(DecisionStep.IF_TRUE_STEP_NAMES_PROPERTY, Lists.newArrayList("step3", "step7"));
    step2ConfigMap.put(DecisionStep.DECISION_METHOD_PROPERTY, DecisionStep.STEP_BY_VALUE_DECISION_METHOD);
    step2ConfigMap.put(DecisionStep.STEP_BY_VALUE_STEP_PROPERTY, "step1");
    Config step2Config = ConfigFactory.parseMap(step2ConfigMap);
    RefactorStep step2 = new DecisionStep("step2");
    step2.configure(step2Config);
    steps.add(step2);

    Set<Step> refactored = step2.refactor(steps);

    assertEquals(refactored, Sets.newHashSet(step1, step2, step3, step4, step7, step8));
  }

  @Test
  public void testPruneByStepValueFalse() {
    StructType schema = new StructType(new StructField[] {
        new StructField("outcome", DataTypes.BooleanType, false, Metadata.empty())
    });
    List<Row> rows = Lists.newArrayList(
        RowFactory.create(false)
    );
    Dataset<Row> ds = Contexts.getSparkSession().createDataFrame(rows, schema);
    step1.setData(ds);

    Map<String, Object> step2ConfigMap = Maps.newHashMap();
    step2ConfigMap.put(Step.DEPENDENCIES_CONFIG, Lists.newArrayList("step1"));
    step2ConfigMap.put(DecisionStep.IF_TRUE_STEP_NAMES_PROPERTY, Lists.newArrayList("step3", "step7"));
    step2ConfigMap.put(DecisionStep.DECISION_METHOD_PROPERTY, DecisionStep.STEP_BY_VALUE_DECISION_METHOD);
    step2ConfigMap.put(DecisionStep.STEP_BY_VALUE_STEP_PROPERTY, "step1");
    Config step2Config = ConfigFactory.parseMap(step2ConfigMap);
    RefactorStep step2 = new DecisionStep("step2");
    step2.configure(step2Config);
    steps.add(step2);

    Set<Step> refactored = step2.refactor(steps);

    assertEquals(refactored, Sets.newHashSet(step1, step2, step5, step6));
  }

}

