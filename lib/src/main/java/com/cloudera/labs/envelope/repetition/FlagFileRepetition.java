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

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.run.BatchStep;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Repeat the associated step when a flag file is either present or modified on a Hadoop compatible file system.
 */
public class FlagFileRepetition
    extends AbstractRepetition implements Runnable, ProvidesAlias, ProvidesValidations {

  private static final String POLL_INTERVAL_CONFIG = "poll-interval";
  private static final String FLAG_FILE_LOCATION_CONFIG = "file";
  private static final String TRIGGER_MODE_CONFIG = "trigger";
  private static final String NUM_ALLOWED_FAILURES_CONFIG = "fail-after";

  private static final long DEFAULT_POLL_INTERVAL = 10000;
  private static final TriggerMode DEFAULT_TRIGGER_MODE = TriggerMode.PRESENT;
  private static final int DEFAULT_NUM_FAILURES = 10;

  private static final Logger LOG = LoggerFactory.getLogger(FlagFileRepetition.class);

  private long pollInterval = DEFAULT_POLL_INTERVAL;
  private TriggerMode triggerMode = DEFAULT_TRIGGER_MODE;
  private int numAllowedFailures = DEFAULT_NUM_FAILURES;
  private Path flagFile;

  private FileSystem fs;
  private int currentFailures = 0;
  private long lastModTime = 0;

  private enum TriggerMode {
    PRESENT,
    MODIFIED
  }

  @Override
  public void configure(BatchStep step, String name, Config config) {
    super.configure(step, name, config);
    flagFile = new Path(config.getString(FLAG_FILE_LOCATION_CONFIG));
    if (config.hasPath(POLL_INTERVAL_CONFIG)) {
      pollInterval = config.getDuration(POLL_INTERVAL_CONFIG, TimeUnit.MILLISECONDS);
    }
    if (config.hasPath(TRIGGER_MODE_CONFIG)) {
      triggerMode = TriggerMode.valueOf(config.getString(TRIGGER_MODE_CONFIG).toUpperCase());
    }
    if (config.hasPath(NUM_ALLOWED_FAILURES_CONFIG)) {
      numAllowedFailures = config.getInt(NUM_ALLOWED_FAILURES_CONFIG);
    }
    Repetitions.get().submitRegularTask(this, pollInterval);

    try {
      fs = FileSystem.get(flagFile.toUri(), Contexts.getSparkSession().sparkContext().hadoopConfiguration());
    } catch (IOException e) {
      throw new RuntimeException("Could not instantiate Hadoop FileSystem");
    }
  }

  private void handlePresentFile() throws IOException {
    LOG.info("Flag file [{}] is present, repeating step", flagFile);
    repeatStep();
    // We delete as soon as we see the flag file in PRESENT mode
    fs.delete(flagFile, true);
  }

  private void handleModifiedFile(FileStatus stat) throws IOException {
    long modTime = stat.getModificationTime();
    if (modTime > lastModTime) {
      LOG.info("Flag file [{}] is present and has been modified since [{}], repeating step", flagFile, lastModTime);
      repeatStep();
      lastModTime = modTime;
    }
  }

  @Override
  public void run() {
    LOG.trace("Firing flag-file repetition check");
    try {
      if (fs.exists(flagFile)) {
        FileStatus stat = fs.getFileStatus(flagFile);
        // Successfully got file status so reset failure counter
        LOG.trace("Got flag-file stat: {}", stat);
        currentFailures = 0;
        switch (triggerMode) {
          case PRESENT:
            handlePresentFile();
            break;
          case MODIFIED:
            handleModifiedFile(stat);
            break;
        }
      }
    } catch (Exception e) {
      currentFailures++;
      if (currentFailures > numAllowedFailures) {
        throw new RuntimeException("Too many consecutive exceptions while checking flag file " + flagFile +
            ": " + currentFailures);
      }
    }
  }

  @Override
  public String getAlias() {
    return "flagfile";
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .addAll(super.getValidations())
        .mandatoryPath(FLAG_FILE_LOCATION_CONFIG, ConfigValueType.STRING)
        .optionalPath(POLL_INTERVAL_CONFIG)
        .optionalPath(TRIGGER_MODE_CONFIG, ConfigValueType.STRING)
        .optionalPath(NUM_ALLOWED_FAILURES_CONFIG, ConfigValueType.NUMBER)
        .build();
  }
  
}
