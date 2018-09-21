/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.kafka;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.labs.envelope.input.CanRecordProgress;
import com.cloudera.labs.envelope.input.StreamInput;
import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.output.Output;
import com.cloudera.labs.envelope.output.OutputFactory;
import com.cloudera.labs.envelope.output.RandomOutput;
import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.PlannerUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

import scala.Tuple2;

public class KafkaInput implements StreamInput, CanRecordProgress, ProvidesAlias {

  private static Logger LOG = LoggerFactory.getLogger(KafkaInput.class);

  public static final String BROKERS_CONFIG = "brokers";
  public static final String TOPICS_CONFIG = "topics";
  public static final String ENCODING_CONFIG = "encoding";
  public static final String WINDOW_ENABLED_CONFIG = "window.enabled";
  public static final String WINDOW_MILLISECONDS_CONFIG = "window.milliseconds";
  public static final String WINDOW_SLIDE_MILLISECONDS_CONFIG = "window.slide.milliseconds";
  public static final String OFFSETS_MANAGE_CONFIG = "offsets.manage";
  public static final String OFFSETS_OUTPUT_CONFIG = "offsets.output";
  public static final String GROUP_ID_CONFIG = "group.id";

  @VisibleForTesting
  String groupID;
  @VisibleForTesting
  Set<String> topics;
  @VisibleForTesting
  RandomOutput offsetsOutput;
  private Config config;
  private String encoding;
  private Map<String, Object> kafkaParams;
  private JavaDStream<?> dStream;

  @Override
  public void configure(Config config) {
    this.config = config;
    kafkaParams = Maps.newHashMap();
    String brokers = config.getString(BROKERS_CONFIG);
    kafkaParams.put("bootstrap.servers", brokers);

    topics = Sets.newHashSet();
    if (config.hasPath(TOPICS_CONFIG)) {
      topics.addAll(config.getStringList(TOPICS_CONFIG));
    }

    if (topics.size() == 0) {
      throw new RuntimeException("Invalid Kafka configuration. At least " +
          "one topic must be specified");
    }

    if (!config.hasPath(ENCODING_CONFIG)) {
      throw new RuntimeException("Kafka input encoding type configuration is required.");
    }

    if (config.hasPath(WINDOW_ENABLED_CONFIG) && (config.getBoolean(WINDOW_ENABLED_CONFIG) == true) &&
        doesRecordProgress()) {
      throw new RuntimeException("Kafka input offset management not currently compatible with stream windowing.");
    }
    
    encoding = config.getString(ENCODING_CONFIG);
    if (encoding.equals("string")) {
      kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    } else if (encoding.equals("bytearray")) {
      kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    } else {
      throw new RuntimeException("Invalid Kafka input encoding type. Valid types are 'string' and 'bytearray'.");
    }

    if (config.hasPath(GROUP_ID_CONFIG)) {
      groupID = config.getString(GROUP_ID_CONFIG);
    } else {
      groupID = UUID.randomUUID().toString();
    }

    kafkaParams.put("group.id", groupID);
    kafkaParams.put("enable.auto.commit", "false");
    KafkaCommon.addCustomParams(kafkaParams, config);
  }

  @Override
  public JavaDStream<?> getDStream() throws Exception {
    if (dStream == null)
    {
      JavaStreamingContext jssc = Contexts.getJavaStreamingContext();
      Map<TopicPartition, Long> lastOffsets = null;
      if (doesRecordProgress() && !usingKafkaManagedOffsets()) {
        lastOffsets = getLastOffsets();
      }

      if (encoding.equals("string")) {
        if (lastOffsets != null) {
          dStream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
              ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams, lastOffsets));
        } else {
          dStream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
              ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
        }
      } else if (encoding.equals("bytearray")) {
        if (lastOffsets != null) {
          dStream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
              ConsumerStrategies.<byte[], byte[]>Subscribe(topics, kafkaParams, lastOffsets));
        } else {
          dStream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
              ConsumerStrategies.<byte[], byte[]>Subscribe(topics, kafkaParams));
        }
      } else {
        throw new RuntimeException("Invalid Kafka input encoding type. Valid types are 'string' and 'bytearray'.");
      }

      if (config.hasPath(WINDOW_ENABLED_CONFIG) && config.getBoolean(WINDOW_ENABLED_CONFIG)) {
        int windowDuration = config.getInt(WINDOW_MILLISECONDS_CONFIG);
        if (config.hasPath(WINDOW_SLIDE_MILLISECONDS_CONFIG)) {
          int slideDuration = config.getInt(WINDOW_SLIDE_MILLISECONDS_CONFIG);
          dStream = dStream.window(new Duration(windowDuration), new Duration(slideDuration));
        } else {
          dStream = dStream.window(new Duration(windowDuration));
        }
      }
    }
    return dStream;
  }


  @Override
  public PairFunction<?, ?, ?> getPrepareFunction() {
    return new UnwrapConsumerRecordFunction();
  }

  @Override
  public String getAlias() {
    return "kafka";
  }

  private boolean usingKafkaManagedOffsets() {
    return (doesRecordProgress() && !config.hasPath(OFFSETS_OUTPUT_CONFIG));
  }

  @SuppressWarnings({ "serial", "rawtypes" })
  private static class UnwrapConsumerRecordFunction implements PairFunction {
    @Override
    public Tuple2 call(Object recordObject) throws Exception {
      ConsumerRecord record = (ConsumerRecord)recordObject;
      return new Tuple2<>(record.key(), record.value());
    }
  }
  
  private boolean doesRecordProgress() {
    boolean managed = (config.hasPath(OFFSETS_MANAGE_CONFIG) && 
        !config.getBoolean(OFFSETS_MANAGE_CONFIG)) ? false : true;
    if (managed && !config.hasPath(GROUP_ID_CONFIG)) {
      throw new RuntimeException("Kafka input can not manage offsets without a provided group ID");
    }
    
    return managed;
  }

  @Override
  public void recordProgress(JavaRDD<?> batch) throws Exception {
    if (doesRecordProgress() && (batch.rdd() instanceof HasOffsetRanges)) {
      OffsetRange[] offsetRanges = ((HasOffsetRanges)batch.rdd()).offsetRanges();

      if (usingKafkaManagedOffsets()) {
         ((CanCommitOffsets) getDStream().dstream()).commitAsync(offsetRanges);
      }
      else { 
        // Plan the offset ranges as an upsert 
        List<Row> planned = Lists.newArrayList();
        StructType schema = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("group_id", DataTypes.StringType, false),
            DataTypes.createStructField("topic", DataTypes.StringType, false),
            DataTypes.createStructField("partition", DataTypes.IntegerType, false),
            DataTypes.createStructField("offset", DataTypes.LongType, false)));
        for (OffsetRange offsetRange : offsetRanges) {
          Row offsetRow = new RowWithSchema(schema, groupID, offsetRange.topic(), offsetRange.partition(),
              offsetRange.untilOffset());
          Row plan = PlannerUtils.setMutationType(offsetRow, MutationType.UPSERT);
          planned.add(plan);
        }
      
        // Upsert the offset ranges at the output
        RandomOutput output = getOffsetsOutput();
        output.applyRandomMutations(planned);
      
        // Retrieve back the offset ranges and assert that they were stored correctly
        Map<TopicPartition, Long> storedOffsets = getLastOffsets();
        for (OffsetRange offsetRange : offsetRanges) {
          TopicPartition tp = new TopicPartition(offsetRange.topic(), offsetRange.partition());
          // Depending on RandomOutput key configuration, this TopicPartition may not even exist.
          // As an example, if key.field.names only contains (group_id, topic) but not the partition.
          if (!storedOffsets.containsKey(tp)) {
            throw new RuntimeException(String.format("Kafka input failed to assert that offset ranges " +
                "were stored correctly!. For group ID '%s', topic '%s' and partition '%d' was not found'",
                groupID, offsetRange.topic(), offsetRange.partition()));
          }

          if (!storedOffsets.get(tp).equals(offsetRange.untilOffset())) {
            String exceptionMessage = String.format(
                "Kafka input failed to assert that offset ranges were stored correctly! " + 
                "For group ID '%s', topic '%s', partition '%d' expected offset '%d' but found offset '%d'",
                groupID, offsetRange.topic(), tp.partition(), offsetRange.untilOffset(), storedOffsets.get(tp));
            throw new RuntimeException(exceptionMessage);
          }
        }
      }
    }
  }
  
  private RandomOutput getOffsetsOutput() {
    if (offsetsOutput == null) {
      Config outputConfig = config.getConfig(OFFSETS_OUTPUT_CONFIG);
      Output output = OutputFactory.create(outputConfig);
      if (!(output instanceof RandomOutput) ||
          !((RandomOutput)output).getSupportedRandomMutationTypes().contains(MutationType.UPSERT)) {
        throw new RuntimeException("Output used for Kafka offsets must support random upsert mutations");
      }
      
      offsetsOutput = (RandomOutput)output;
    }
    
    return offsetsOutput;
  }

  private Map<TopicPartition, Long> getLastOffsets() throws Exception {
    Map<TopicPartition, Long> offsetRanges = Maps.newHashMap();
    // Create filter for groupid/topic
    for (String topic : topics) {
      StructType filterSchema = DataTypes.createStructType(Lists.newArrayList(
          DataTypes.createStructField("group_id", DataTypes.StringType, false),
          DataTypes.createStructField("topic", DataTypes.StringType, false)));
      Row groupIDTopicFilter = new RowWithSchema(filterSchema, groupID, topic);
      Iterable<Row> filters = Collections.singleton(groupIDTopicFilter);

      // Get results
      RandomOutput output = getOffsetsOutput();
      Iterable<Row> results = output.getExistingForFilters(filters);

      // Transform results into map
      for (Row result : results) {
        Integer partition = result.getInt(result.fieldIndex("partition"));
        Long offset = result.getLong(result.fieldIndex("offset"));
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        offsetRanges.put(topicPartition, offset);
      }
    }
    
    return offsetRanges;
  }
}
