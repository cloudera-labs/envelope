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
package com.cloudera.labs.envelope.spark;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Used as a singleton for any driver code in Envelope to retrieve the various Spark contexts,
 * and have them instantiated automatically if they have not already been created.
 */
public enum Contexts {

  INSTANCE;

  private static final Logger LOG = LoggerFactory.getLogger(Contexts.class);

  public static final String APPLICATION_NAME_PROPERTY = "application.name";
  public static final String BATCH_MILLISECONDS_PROPERTY = "application.batch.milliseconds";
  public static final String CHECKPOINT_ENABLED_PROPERTY = "application.checkpoint.enabled";
  public static final String CHECKPOINT_PATH_PROPERTY = "application.checkpoint.path";
  public static final String NUM_EXECUTORS_PROPERTY = "application.executors";
  public static final String NUM_EXECUTOR_CORES_PROPERTY = "application.executor.cores";
  public static final String EXECUTOR_MEMORY_PROPERTY = "application.executor.memory";
  public static final String SPARK_CONF_PROPERTY_PREFIX = "application.spark.conf";

  private Config config = ConfigFactory.empty();

  private JavaStreamingContext jssc;
  private JavaSparkContext jsc;
  private SQLContext sqlc;
  private HiveContext hc;

  public static synchronized JavaStreamingContext getJavaStreamingContext() {
    if (INSTANCE.jssc == null) {
      initializeStreamingJob();
    }

    return INSTANCE.jssc;
  }

  public static synchronized JavaSparkContext getJavaSparkContext() {
    if (INSTANCE.jsc == null) {
      initializeBatchJob();
    }

    return INSTANCE.jsc;
  }

  public static synchronized SQLContext getSQLContext() {
    if (INSTANCE.sqlc == null) {
      initializeSQLContext();
    }

    return INSTANCE.sqlc;
  }

  public static synchronized HiveContext getHiveContext() {
    if (INSTANCE.hc == null) {
      initializeHiveContext();
    }

    return INSTANCE.hc;
  }

  public static void initialize(Config config) {
    INSTANCE.config = config;
  }

  private static void initializeStreamingJob() {
    final SparkConf sparkConf = getSparkConfiguration(INSTANCE.config);

    int batchMilliseconds = INSTANCE.config.getInt(BATCH_MILLISECONDS_PROPERTY);
    final Duration batchDuration = Durations.milliseconds(batchMilliseconds);

    JavaStreamingContext jssc;
    boolean toCheckpoint = doesCheckpoint(INSTANCE.config);
    if (toCheckpoint) {
      String checkpointPath = INSTANCE.config.getString(CHECKPOINT_PATH_PROPERTY);
      JavaStreamingContextFactory factory = new JavaStreamingContextFactory() {
        @Override
        public JavaStreamingContext create() {
          return new JavaStreamingContext(sparkConf, batchDuration);
        }
      };
      jssc = JavaStreamingContext.getOrCreate(checkpointPath, factory);
      jssc.checkpoint(checkpointPath);
    }
    else {
      if (INSTANCE.jsc != null) {
        jssc = new JavaStreamingContext(INSTANCE.jsc, batchDuration);
      }
      else {
        jssc = new JavaStreamingContext(sparkConf, batchDuration);
      }
    }

    INSTANCE.jssc = jssc;
    INSTANCE.jsc = jssc.sparkContext();
  }

  private static void initializeBatchJob() {
    SparkConf sparkConf = getSparkConfiguration(INSTANCE.config);
    
    if (!sparkConf.contains("spark.master")) {
      LOG.warn("Spark master not provided, instead using local mode");
      sparkConf.setMaster("local[*]");
    }
    if (!sparkConf.contains("spark.app.name")) {
      LOG.warn("Spark application name not provided, instead using empty string");
      sparkConf.setAppName("");
    }

    INSTANCE.jsc = new JavaSparkContext(sparkConf);
  }

  private static void initializeSQLContext() {    
    INSTANCE.sqlc = new SQLContext(getJavaSparkContext());
  }

  private static void initializeHiveContext() {
    INSTANCE.hc = new HiveContext(getJavaSparkContext());
  }

  static synchronized SparkConf getSparkConfiguration(Config config) {
    SparkConf sparkConf = new SparkConf();
    
    if (config.hasPath(APPLICATION_NAME_PROPERTY)) {
      String applicationName = config.getString(APPLICATION_NAME_PROPERTY);
      sparkConf.setAppName(applicationName);
    }

    // Dynamic allocation should not be used for Spark Streaming jobs because the latencies
    // of the resource requests are too long.
    sparkConf.set("spark.dynamicAllocation.enabled", "false");
    // Spark Streaming back-pressure helps automatically tune the size of the micro-batches so
    // that they don't breach the micro-batch length.
    sparkConf.set("spark.streaming.backpressure.enabled", "true");
    // Rate limit the micro-batches when using Apache Kafka to 2000 records per Kafka topic partition
    // per second. Without this we could end up with arbitrarily large initial micro-batches
    // for existing topics.
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "2000");
    // Override the Spark SQL shuffle partitions with the default number of cores. Otherwise
    // the default is typically 200 partitions, which is very high for micro-batches.
    sparkConf.set("spark.sql.shuffle.partitions", "2");

    if (config.hasPath(NUM_EXECUTORS_PROPERTY)) {
      sparkConf.set("spark.executor.instances", config.getString(NUM_EXECUTORS_PROPERTY));
    }
    if (config.hasPath(NUM_EXECUTOR_CORES_PROPERTY)) {
      sparkConf.set("spark.executor.cores", config.getString(NUM_EXECUTOR_CORES_PROPERTY));
    }
    if (config.hasPath(EXECUTOR_MEMORY_PROPERTY)) {
      sparkConf.set("spark.executor.memory", config.getString(EXECUTOR_MEMORY_PROPERTY));
    }
    // Override the Spark SQL shuffle partitions with the number of cores, if known.
    if (config.hasPath(NUM_EXECUTORS_PROPERTY) && config.hasPath(NUM_EXECUTOR_CORES_PROPERTY)) {
      int executors = config.getInt(NUM_EXECUTORS_PROPERTY);
      int executorCores = config.getInt(NUM_EXECUTOR_CORES_PROPERTY);
      Integer shufflePartitions = executors * executorCores;

      sparkConf.set("spark.sql.shuffle.partitions", shufflePartitions.toString());
    }

    // Allow the user to provide any Spark configuration and we will just pass it on. These can
    // also override any of the configurations above.
    if (config.hasPath(SPARK_CONF_PROPERTY_PREFIX)) {
      Config sparkConfigs = config.getConfig(SPARK_CONF_PROPERTY_PREFIX);
      for (Map.Entry<String, ConfigValue> entry : sparkConfigs.entrySet()) {
        String param = entry.getKey();
        String value = null;
        switch (entry.getValue().valueType()) {
          case STRING:
            value = (String) entry.getValue().unwrapped();
            break;
          default:
            LOG.warn("Only string parameters currently " +
              "supported, enclose {} in quotes", param);
        }
        if (value != null) {
          sparkConf.set(param, value);
        }
      }
    }

    return sparkConf;
  }

  private static boolean doesCheckpoint(Config config) {
    if (!config.hasPath(CHECKPOINT_ENABLED_PROPERTY)) return false;

    return INSTANCE.config.getBoolean(CHECKPOINT_ENABLED_PROPERTY);
  }

}
