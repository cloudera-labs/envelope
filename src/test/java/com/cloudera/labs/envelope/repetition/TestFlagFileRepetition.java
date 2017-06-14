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
package com.cloudera.labs.envelope.repetition;

import com.cloudera.labs.envelope.run.BatchStep;
import com.cloudera.labs.envelope.run.DataStep;
import com.cloudera.labs.envelope.run.Runner;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestFlagFileRepetition {

  private File relativeFlagFile = new File("flag");
  private Path flagFile = new Path(relativeFlagFile.toURI().toString());
  private FileSystem fs = FileSystem.get(flagFile.toUri(),
      Contexts.getSparkSession().sparkContext().hadoopConfiguration());

  public TestFlagFileRepetition() throws IOException {
  }

  @Test
  public void testRepeatStepPresent() throws IOException {

    Config config = ConfigUtils.configFromResource("/repetitions/repetitions-flag-config.conf");
    File relativeFlagFile = new File("flag");
    config = config.withValue("steps.repeater.repetitions.hdfsinnit.file",
        ConfigValueFactory.fromAnyRef(relativeFlagFile.toURI().toString()));

    try {
      BatchStep step = new BatchStep("testFlagRepetition", config.getConfig("steps.repeater"));
      Set<DataStep> steps = Repetitions.get().getAndClearRepeatingSteps();
      assertTrue("Repeating steps should not be populated", steps.isEmpty());

      // Place flag file
      assertTrue(fs.createNewFile(flagFile));
      Thread.sleep(300);
      steps = Repetitions.get().getAndClearRepeatingSteps();
      assertFalse("Repeating steps should be populated", steps.isEmpty());
      steps = Repetitions.get().getAndClearRepeatingSteps();
      assertTrue("Repeating steps should not be populated", steps.isEmpty());

      // Repeat again
      assertTrue(fs.createNewFile(flagFile));
      Thread.sleep(300);
      steps = Repetitions.get().getAndClearRepeatingSteps();
      assertFalse("Repeating steps should be populated", steps.isEmpty());
      steps = Repetitions.get().getAndClearRepeatingSteps();
      assertTrue("Repeating steps should not be populated", steps.isEmpty());
    } catch (Exception e) {
      System.err.println(e.getMessage());
      fail();
    }

  }

  @Test
  public void testRepeatStepModified() throws IOException {

    Config config = ConfigUtils.configFromResource("/repetitions/repetitions-flag-config-modified.conf");
    File relativeFlagFile = new File("flag");
    config = config.withValue("steps.repeater.repetitions.hdfsinnit.file",
        ConfigValueFactory.fromAnyRef(relativeFlagFile.toURI().toString()));

    try {
      BatchStep step = new BatchStep("testFlagRepetitionMod", config.getConfig("steps.repeater"));
      Set<DataStep> steps = Repetitions.get().getAndClearRepeatingSteps();
      assertTrue("Repeating steps should not be populated", steps.isEmpty());

      // Place flag file
      assertTrue(fs.createNewFile(flagFile));
      Thread.sleep(300);
      steps = Repetitions.get().getAndClearRepeatingSteps();
      assertFalse("Repeating steps should be populated", steps.isEmpty());
      steps = Repetitions.get().getAndClearRepeatingSteps();
      assertTrue("Repeating steps should not be populated", steps.isEmpty());
      Thread.sleep(300);
      steps = Repetitions.get().getAndClearRepeatingSteps();
      assertTrue("Repeating steps should not be populated", steps.isEmpty());

      // Repeat again
      long mTime = System.currentTimeMillis();
      fs.setTimes(flagFile, mTime, mTime);
      Thread.sleep(300);
      steps = Repetitions.get().getAndClearRepeatingSteps();
      assertFalse("Repeating steps should be populated", steps.isEmpty());
      steps = Repetitions.get().getAndClearRepeatingSteps();
      assertTrue("Repeating steps should not be populated", steps.isEmpty());
    } catch (Exception e) {
      System.err.println(e.getMessage());
      fail();
    }

  }

  @After
  public void after() throws IOException {
    if (fs.exists(flagFile)) {
      fs.delete(flagFile, true);
    }
    Repetitions.get(true);
    Contexts.closeSparkSession(true);
  }

}
