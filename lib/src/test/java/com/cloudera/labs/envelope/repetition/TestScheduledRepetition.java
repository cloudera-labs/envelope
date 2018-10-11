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

package com.cloudera.labs.envelope.repetition;

import com.cloudera.labs.envelope.run.BatchStep;
import com.cloudera.labs.envelope.run.DataStep;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.typesafe.config.Config;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestScheduledRepetition {

  @Test
  public void testRepeatStep() {

    Config config = ConfigUtils.configFromResource("/repetitions/repetitions-test-config.conf");

    try {
      BatchStep step = new BatchStep("testRepetition");
      step.configure(config.getConfig("steps.repeater"));
      Thread.sleep(20);
      Set<DataStep> steps = Repetitions.get().getAndClearRepeatingSteps();
      assertTrue("Repeating steps should not be populated", steps.isEmpty());
      for (int i = 0; i < 5; i++) {
        Thread.sleep(500);
        steps = Repetitions.get().getAndClearRepeatingSteps();
        assertFalse("Repeating steps should be populated", steps.isEmpty());
        assertEquals("Step should be called 'testRepetition'", "testRepetition", steps.iterator().next().getName());
        steps = Repetitions.get().getAndClearRepeatingSteps();
        assertTrue("Repeating steps should be empty", steps.isEmpty());
      }
    } catch (Exception e) {
      fail();
    }

  }

//  @Test
//  public void testRepeatingApp() throws InterruptedException {
//    Config appConfig = ConfigUtils.configFromResource("/repetitions/repetitions-schedule-test.conf");
//    TestRunner runner = new TestRunner(appConfig);
//    ExecutorService service = Executors.newSingleThreadExecutor();
//    service.execute(runner);
//    Thread.sleep(30000);
//    Repetitions.get().shutdownTasks();
//    service.shutdownNow();
//
//    assertFalse("Should be some messages in output", DummyBatchOutput.getOutputs().isEmpty());
//  }

  @After
  public void after() throws IOException {
    Repetitions.get(true);
    Contexts.closeSparkSession(true);
  }

//  @AfterClass
//  public static void afterClass() {
//    Contexts.closeSparkSession(true);
//  }
//
//  public static class TestRunner implements Runnable {
//
//    private Config config;
//
//    TestRunner(Config config) {
//      this.config = config;
//    }
//
//    @Override
//    public void run() {
//      try {
//        Runner.run(config);
//      } catch (Exception e) {
//        // swallow
//      }
//    }
//
//  }

}
