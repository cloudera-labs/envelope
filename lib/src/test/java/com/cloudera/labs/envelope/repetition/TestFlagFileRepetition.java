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
import com.cloudera.labs.envelope.validate.ValidationAssert;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Set;

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
      BatchStep step = new BatchStep("testFlagRepetition");
      ValidationAssert.assertNoValidationFailures(step, config.getConfig("steps.repeater"));
      step.configure(config.getConfig("steps.repeater"));
      Set<DataStep> steps = Repetitions.get().getAndClearRepeatingSteps();
      assertTrue("Repeating steps should not be populated", steps.isEmpty());

      // Place flag file
      assertTrue(fs.createNewFile(flagFile));

      // Should _not_ be empty
      waitForResponse(300, false, 10);

      // Should immediately be empty
      waitForResponse(300, true, 1);

      // Repeat again - add the flag file back
      assertTrue(fs.createNewFile(flagFile));

      // Should _not_ be empty
      waitForResponse(300, false, 10);

      // Should immediately be empty
      waitForResponse(300, true, 1);
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
      BatchStep step = new BatchStep("testFlagRepetitionMod");
      step.configure(config.getConfig("steps.repeater"));
      Set<DataStep> steps = Repetitions.get().getAndClearRepeatingSteps();
      assertTrue("Repeating steps should not be populated", steps.isEmpty());

      // Place flag file
      assertTrue(fs.createNewFile(flagFile));
      System.out.println("File stat: " + fs.getFileStatus(flagFile));

      // Should _not_ be empty
      waitForResponse(300, false, 10);

      // Should immediately be empty
      waitForResponse(300, true, 1);

      // Repeat again and add 1000 as the local filesystem might only have a granularity of 1 second
      long mTime = System.currentTimeMillis() + 1000;
      System.out.println("Modifying file time to " + mTime);
      fs.setTimes(flagFile, mTime, mTime);

      // Should _not_ be empty as we have changed the file
      waitForResponse(300, false, 10);

      // Should immediately be empty
      waitForResponse(300, true, 1);
    } catch (Exception e) {
      System.err.println(e.getMessage());
      fail();
    }
  }

  private Set<DataStep> waitForResponse(long waitFor, boolean response, int maxTimes) throws InterruptedException {
    Set<DataStep> steps = null;
    for (int i = 0; i < maxTimes; i++) {
      Thread.sleep(waitFor);
      steps = Repetitions.get().getAndClearRepeatingSteps();
      if (steps.isEmpty() == response) {
        return steps;
      }
    }
    fail("Waited " + waitFor + "ms for repeating steps empty to be " + response);
    return steps;
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
