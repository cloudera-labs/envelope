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

import com.cloudera.labs.envelope.component.ComponentFactory;
import com.cloudera.labs.envelope.derive.Deriver;
import com.cloudera.labs.envelope.event.CoreEventTypes;
import com.cloudera.labs.envelope.event.Event;
import com.cloudera.labs.envelope.event.EventManager;
import com.cloudera.labs.envelope.event.TestingEventHandler;
import com.cloudera.labs.envelope.output.BulkOutput;
import com.cloudera.labs.envelope.output.RandomOutput;
import com.cloudera.labs.envelope.plan.BulkPlanner;
import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.task.Task;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF1;
import org.junit.Test;
import scala.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestRunner {

  @Test
  public void testValidUDFs() throws Exception {
    Contexts.closeSparkSession();
    Config config = ConfigUtils.configFromResource("/udf/udf_valid.conf");

    new Runner().initializeUDFs(config);
    Deriver deriver = ComponentFactory.create(
        Deriver.class, config.getConfig(Runner.STEPS_SECTION_CONFIG + ".runudf.deriver"), true);
    Dataset<Row> derived = deriver.derive(Maps.<String, Dataset<Row>>newHashMap());

    assertEquals(RowFactory.create("hello", 1), derived.collectAsList().get(0));
  }
  
  @Test
  (expected = AnalysisException.class)
  public void testNoUDFs() throws Throwable {
    Contexts.closeSparkSession();
    Config config = ConfigUtils.configFromResource("/udf/udf_none.conf");

    new Runner().initializeUDFs(config);
    Deriver deriver = ComponentFactory.create(
        Deriver.class, config.getConfig("steps.runudf.deriver"), true);
    deriver.derive(Maps.<String, Dataset<Row>>newHashMap());
  }
  
  @SuppressWarnings("serial")
  public static class TestUDF1 implements UDF1<String, String> {
    @Override
    public String call(String arg) throws Exception {
      return arg;
    }
  }
  
  @SuppressWarnings("serial")
  public static class TestUDF2 implements UDF1<Integer, Integer> {
    @Override
    public Integer call(Integer arg) throws Exception {
      return arg;
    }
  }
  
  public static class TestingSQLDeriver implements Deriver {
    public static final String QUERY_LITERAL_CONFIG = "query.literal";
    private Config config;

    @Override
    public void configure(Config config) {
      this.config = config;
    }

    @Override
    public Dataset<Row> derive(Map<String, Dataset<Row>> dependencies) {
      String query = config.getString(QUERY_LITERAL_CONFIG);
      Dataset<Row> derived = Contexts.getSparkSession().sql(query);
      return derived;
    }
  }

  public static class TestingAppendPlanner implements BulkPlanner {
    @Override
    public List<Tuple2<MutationType, Dataset<Row>>> planMutationsForSet(Dataset<Row> arriving) {
      return Lists.newArrayList(Tuple2.apply(MutationType.INSERT, arriving));
    }

    @Override
    public void configure(Config config) { }

    @Override
    public Set<MutationType> getEmittedMutationTypes() {
      return Sets.newHashSet(MutationType.INSERT);
    }
  }

  public static class TestingMemoryOutput implements BulkOutput, RandomOutput {
    public static List<Row> rows = Lists.newArrayList();

    @Override
    public Set<MutationType> getSupportedBulkMutationTypes() {
      return Sets.newHashSet(MutationType.INSERT);
    }

    @Override
    public void applyBulkMutations(List<Tuple2<MutationType, Dataset<Row>>> planned) {
      applyRandomMutations(planned.get(0)._2().collectAsList());
    }

    @Override
    public Set<MutationType> getSupportedRandomMutationTypes() {
      return Sets.newHashSet(MutationType.INSERT);
    }

    @Override
    public void applyRandomMutations(List<Row> planned) {
      rows.addAll(planned);
    }

    @Override
    public Iterable<Row> getExistingForFilters(Iterable<Row> filters) {
      return rows;
    }

    @Override
    public void configure(Config config) { }

    public static List<Row> getRows() {
      return rows;
    }

    public static void reset() { rows.clear(); }
  }

  @Test
  public void testExpectedCoreEvents() throws Exception {
    EventManager.reset();

    String executionKey = UUID.randomUUID().toString();
    Config executionKeyConfig = ConfigFactory.parseString("execution_key = " + executionKey);
    Config config = ConfigUtils.configFromResource("/event/expected_core_events.conf");
    config = config.withFallback(executionKeyConfig).resolve();

    new Runner().run(config);

    List<Event> events = TestingEventHandler.getHandledEvents(executionKey);

    List<String> eventTypes = Lists.newArrayList();
    for (Event event : events) {
      eventTypes.add(event.getEventType());
    }

    List<String> expectedEventTypes = Lists.newArrayList(
        CoreEventTypes.PIPELINE_STARTED,
        CoreEventTypes.PIPELINE_FINISHED,
        CoreEventTypes.DATA_STEP_DATA_GENERATED,
        CoreEventTypes.DATA_STEP_WRITTEN_TO_OUTPUT,
        CoreEventTypes.EXECUTION_MODE_DETERMINED,
        CoreEventTypes.STEPS_EXTRACTED
    );

    for (String expectedEventType : expectedEventTypes) {
      assertTrue("Does not contain event type: " + expectedEventType, eventTypes.contains(expectedEventType));
    }
  }

  @Test
  public void testExceptionEvent() {
    EventManager.reset();

    String executionKey = UUID.randomUUID().toString();
    Config executionKeyConfig = ConfigFactory.parseString("execution_key = " + executionKey);
    Config config = ConfigUtils.configFromResource("/event/exception_event.conf");
    config = config.withFallback(executionKeyConfig).resolve();

    try {
      new Runner().run(config);
    }
    catch (Exception e) {
      // Ignore the exception as we should see it in the handled event below
    }

    List<Event> events = TestingEventHandler.getHandledEvents(executionKey);

    List<String> eventTypes = Lists.newArrayList();
    for (Event event : events) {
      eventTypes.add(event.getEventType());
    }

    assertTrue(eventTypes.contains(CoreEventTypes.PIPELINE_EXCEPTION_OCCURRED));
  }

  public static class CheckRefactoredStepsContainedTask implements Task {
    private static int runCount = 0;

    @Override
    public void configure(Config config) { }

    @Override
    public void run(Map<String, Dataset<Row>> dependencies) {
      if (dependencies.size() > 2) {
        throw new RuntimeException("Too many loop iterations!");
      }

      if (++runCount == 2) {
        throw new RuntimeException("End of stream");
      }
    }
  }

  @Test
  public void testRefactoredStepsContainedToStreamBatch() throws Exception {
    Config config = ConfigUtils.configFromResource("/run/refactored_contained.conf");

    try {
      new Runner().run(config);
    }
    catch (Exception e) {
      if (e.getMessage().equals("End of stream")) {
        return;
      }
      else if (e.getMessage().equals("Too many loop iterations!")) {
        fail("Micro-batch loop ran too many iterations");
      }
      else {
        throw e;
      }
    }

    fail("Pipeline expected to throw an exception");
  }
  
}
