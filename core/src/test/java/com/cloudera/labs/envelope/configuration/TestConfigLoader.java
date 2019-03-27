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

package com.cloudera.labs.envelope.configuration;

import com.cloudera.labs.envelope.input.StreamInput;
import com.cloudera.labs.envelope.run.Runner;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.task.Task;
import com.cloudera.labs.envelope.translate.Translator;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.utils.SchemaUtils;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestConfigLoader {

  @Test
  public void testMergeConfigLoaderBatch() throws Exception {
    assertFalse(BatchDummyTask.hasRun());

    Config batchConfig = ConfigUtils.configFromResource("/configuration/run/batch.conf");
    new Runner().run(batchConfig);

    assertTrue(BatchDummyTask.hasRun());
  }

  @Test
  public void testMergeConfigLoaderStream() throws Exception {
    Config streamConfig = ConfigUtils.configFromResource("/configuration/run/stream.conf");
    try {
      new Runner().run(streamConfig);
    }
    // Swallow the exception because we only threw it as a way of stopping the stream
    // after three micro-batches
    catch (RuntimeException e) {
      if (!e.getMessage().equals("Crash!")) {
        throw e;
      }
    }

    // One for the initial merge, and three for the three micro-batches
    assertEquals(4, StreamDummyTask.getLoadCount());

    // We have bailed out of the stream, so we should reset for the next test
    Contexts.closeJavaStreamingContext();
  }

  public static class BatchDummyTaskConfigLoader implements ConfigLoader {
    @Override
    public void configure(Config config) {}

    @Override
    public Config getConfig() {
      return ConfigUtils.configFromResource("/configuration/run/merge-into-batch-steps.conf");
    }
  }

  public static class StreamDummyTaskConfigLoader implements ConfigLoader {
    private static int loadCount = 0;

    @Override
    public void configure(Config config) {}

    @Override
    public Config getConfig() {
      Config stepsConfig = ConfigUtils.configFromResource("/configuration/run/merge-into-stream-steps.conf");
      stepsConfig = stepsConfig.withValue("steps.dummy_task.task.load-count", ConfigValueFactory.fromAnyRef(++loadCount));

      return stepsConfig;
    }
  }

  public static class BatchDummyTask implements Task {
    private static boolean hasRun = false;

    @Override
    public void configure(Config config) { }

    @Override
    public void run(Map<String, Dataset<Row>> dependencies) {
      hasRun = true;
    }

    public static boolean hasRun() {
      return hasRun;
    }
  }

  public static class StreamDummyTask implements Task {
    private static int loadCount;

    @Override
    public void configure(Config config) {
      loadCount = config.getInt("load-count");
    }

    @Override
    public void run(Map<String, Dataset<Row>> dependencies) { }

    public static int getLoadCount() {
      return loadCount;
    }
  }

  public static class DummyStreamInput implements StreamInput {
    @Override
    public JavaDStream<?> getDStream() {
      return Contexts.getJavaStreamingContext().queueStream(
          Lists.<JavaRDD<Object>>newLinkedList(),
          true,
          Contexts.getJavaStreamingContext().sparkContext().emptyRDD()
      );
    }

    @Override
    public Function<?, Row> getMessageEncoderFunction() {
      return null;
    }

    @Override
    public void configure(Config config) { }

    @Override
    public StructType getProvidingSchema() {
      return SchemaUtils.stringValueSchema();
    }
  }

  public static class DummyTranslator implements Translator {
    @Override
    public void configure(Config config) { }

    @Override
    public Iterable<Row> translate(Row message) throws Exception {
      return null;
    }

    @Override
    public StructType getExpectingSchema() {
      return SchemaUtils.stringValueSchema();
    }

    @Override
    public StructType getProvidingSchema() {
      return SchemaUtils.stringValueSchema();
    }
  }

  public static class CrashOutOfStreamAfterThreeMicrobatchesTask implements Task {
    private static int runCount = 0;

    @Override
    public void configure(Config config) { }

    @Override
    public void run(Map<String, Dataset<Row>> dependencies) {
      runCount++;

      if (runCount == 3) {
        throw new RuntimeException("Crash!");
      }
    }
  }

}
