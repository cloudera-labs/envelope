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

package com.cloudera.labs.envelope.kafka;

import com.cloudera.labs.envelope.component.InstantiatedComponent;
import com.cloudera.labs.envelope.component.InstantiatesComponents;
import com.cloudera.labs.envelope.input.CanRecordProgress;
import com.cloudera.labs.envelope.input.StreamInput;
import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.output.Output;
import com.cloudera.labs.envelope.output.OutputFactory;
import com.cloudera.labs.envelope.output.RandomOutput;
import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.schema.DeclaresProvidingSchema;
import com.cloudera.labs.envelope.schema.UsesExpectedSchema;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.translate.Translator;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.utils.PlannerUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class KafkaInput implements StreamInput, CanRecordProgress, ProvidesAlias,
    ProvidesValidations, InstantiatesComponents, UsesExpectedSchema {

  public static final String BROKERS_CONFIG = "brokers";
  public static final String TOPICS_CONFIG = "topics";
  public static final String WINDOW_ENABLED_CONFIG = "window.enabled";
  public static final String WINDOW_MILLISECONDS_CONFIG = "window.milliseconds";
  public static final String WINDOW_SLIDE_MILLISECONDS_CONFIG = "window.slide.milliseconds";
  public static final String OFFSETS_MANAGE_CONFIG = "offsets.manage";
  public static final String OFFSETS_OUTPUT_CONFIG = "offsets.output";
  public static final String GROUP_ID_CONFIG = "group.id";

  private static final String KEY_FIELD_NAME = "key";

  @VisibleForTesting Set<String> topics;
  @VisibleForTesting RandomOutput offsetsOutput;
  @VisibleForTesting String groupID;
  private Config config;
  private StructType expectedSchema;
  private Map<String, Object> kafkaParams;
  private JavaDStream<?> dStream;

  @Override
  public void configure(Config config) {
    this.config = config;

    groupID = ConfigUtils.getOrElse(config, GROUP_ID_CONFIG, UUID.randomUUID().toString());

    kafkaParams = Maps.newHashMap();
    kafkaParams.put("bootstrap.servers", config.getString(BROKERS_CONFIG));
    kafkaParams.put("group.id", groupID);
    kafkaParams.put("enable.auto.commit", "false");
    KafkaCommon.addCustomParams(kafkaParams, config);

    topics = Sets.newHashSet(config.getStringList(TOPICS_CONFIG));

    if (ConfigUtils.getOrElse(config, WINDOW_ENABLED_CONFIG, false) && doesRecordProgress(config)) {
      throw new RuntimeException("Kafka input offset management not compatible with stream windowing.");
    }
  }

  @Override
  public void receiveExpectedSchema(StructType expectedSchema) {
    this.expectedSchema = expectedSchema;

    List<String> fieldNames = Lists.newArrayList(KEY_FIELD_NAME, Translator.VALUE_FIELD_NAME);

    for (String fieldName : fieldNames) {
      if (Lists.newArrayList(expectedSchema.fieldNames()).contains(fieldName)) {
        DataType fieldDataType = expectedSchema.fields()[expectedSchema.fieldIndex(fieldName)].dataType();

        if (fieldDataType.equals(DataTypes.StringType)) {
          kafkaParams.put(fieldName + ".deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        }
        else if (fieldDataType.equals(DataTypes.BinaryType)) {
          kafkaParams.put(fieldName + ".deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        }
        else {
          throw new RuntimeException("Translator expects '" + fieldName + "' field to be of type '" + fieldDataType +
              "' but Kafka input only supports providing '" + fieldName + "' field as either string or   binary.");
        }
      }
      else {
        // If the translator doesn't expect the field then provide it as binary
        kafkaParams.put(fieldName + ".deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      }
    }
  }

  @Override
  public JavaDStream<?> getDStream() throws Exception {
    if (dStream == null) {
      JavaStreamingContext jssc = Contexts.getJavaStreamingContext();
      Map<TopicPartition, Long> lastOffsets = null;
      if (doesRecordProgress(config) && !usingKafkaManagedOffsets(config)) {
        lastOffsets = getLastOffsets();
      }

      if (lastOffsets != null) {
        dStream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
            ConsumerStrategies.Subscribe(topics, kafkaParams, lastOffsets));
      } else {
        dStream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
            ConsumerStrategies.Subscribe(topics, kafkaParams));
      }

      if (ConfigUtils.getOrElse(config, WINDOW_ENABLED_CONFIG, false)) {
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
  public Function<?, Row> getMessageEncoderFunction() {
    DataType keyDataType = getExpectedFieldDataType(KEY_FIELD_NAME);
    DataType valueDataType = getExpectedFieldDataType(Translator.VALUE_FIELD_NAME);

    return new UnwrapConsumerRecordFunction(keyDataType, valueDataType);
  }

  @Override
  public StructType getProvidingSchema() {
    return ((UnwrapConsumerRecordFunction)getMessageEncoderFunction()).getProvidingSchema();
  }

  @Override
  public String getAlias() {
    return "kafka";
  }

  private boolean usingKafkaManagedOffsets(Config config) {
    return doesRecordProgress(config) && !config.hasPath(OFFSETS_OUTPUT_CONFIG);
  }

  private static class UnwrapConsumerRecordFunction implements Function<ConsumerRecord, Row>,
      DeclaresProvidingSchema {
    private DataType keyDataType, valueDataType;

    UnwrapConsumerRecordFunction(DataType keyDataType, DataType valueDataType) {
      this.keyDataType = keyDataType;
      this.valueDataType = valueDataType;
    }

    @Override
    public Row call(ConsumerRecord record) {
      return new RowWithSchema(
          getProvidingSchema(),
          record.key(),
          record.value(),
          record.timestamp(),
          record.topic(),
          record.partition(),
          record.offset());
    }

    @Override
    public StructType getProvidingSchema() {
      return DataTypes.createStructType(Lists.newArrayList(
          DataTypes.createStructField(KEY_FIELD_NAME, keyDataType, true),
          DataTypes.createStructField(Translator.VALUE_FIELD_NAME, valueDataType, false),
          DataTypes.createStructField("timestamp", DataTypes.LongType, true),
          DataTypes.createStructField("topic", DataTypes.StringType, true),
          DataTypes.createStructField("partition", DataTypes.IntegerType, true),
          DataTypes.createStructField("offset", DataTypes.LongType, true)
      ));
    }
  }

  private DataType getExpectedFieldDataType(String fieldName) {
    if (Lists.newArrayList(expectedSchema.fieldNames()).contains(fieldName)) {
      return expectedSchema.fields()[expectedSchema.fieldIndex(fieldName)].dataType();
    }
    else {
      // If the translator doesn't expect the field then provide it as binary
      return DataTypes.BinaryType;
    }
  }
  
  private boolean doesRecordProgress(Config config) {
    boolean managed = ConfigUtils.getOrElse(config, OFFSETS_MANAGE_CONFIG, true);
    if (managed && !config.hasPath(GROUP_ID_CONFIG)) {
      throw new RuntimeException("Kafka input can not manage offsets without a provided group ID");
    }
    
    return managed;
  }

  @Override
  public void recordProgress(JavaRDD<?> batch) throws Exception {
    if (doesRecordProgress(config) && (batch.rdd() instanceof HasOffsetRanges)) {
      OffsetRange[] offsetRanges = ((HasOffsetRanges)batch.rdd()).offsetRanges();

      if (usingKafkaManagedOffsets(config)) {
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
      RandomOutput output = getOffsetsOutput(config, true);
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
  
  private RandomOutput getOffsetsOutput(Config config, boolean configure) {
    if (configure) {
      if (offsetsOutput == null) {
        Config outputConfig = config.getConfig(OFFSETS_OUTPUT_CONFIG);
        Output output = OutputFactory.create(outputConfig, true);

        if (!(output instanceof RandomOutput) ||
            !((RandomOutput) output).getSupportedRandomMutationTypes().contains(MutationType.UPSERT)) {
          throw new RuntimeException("Output used for Kafka offsets must support random upsert mutations");
        }

        offsetsOutput = (RandomOutput)output;
      }

      return offsetsOutput;
    }
    else {
      Config outputConfig = config.getConfig(OFFSETS_OUTPUT_CONFIG);
      return (RandomOutput)OutputFactory.create(outputConfig, false);
    }
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
      RandomOutput output = getOffsetsOutput(config, true);
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

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(BROKERS_CONFIG, ConfigValueType.STRING)
        .mandatoryPath(TOPICS_CONFIG, ConfigValueType.LIST)
        .optionalPath(GROUP_ID_CONFIG, ConfigValueType.STRING)
        .optionalPath(WINDOW_ENABLED_CONFIG, ConfigValueType.BOOLEAN)
        .ifPathHasValue(WINDOW_ENABLED_CONFIG, true,
            Validations.single().mandatoryPath(WINDOW_MILLISECONDS_CONFIG, ConfigValueType.NUMBER))
        .optionalPath(OFFSETS_MANAGE_CONFIG, ConfigValueType.BOOLEAN)
        .ifPathHasValue(OFFSETS_MANAGE_CONFIG, true,
            Validations.single().mandatoryPath(OFFSETS_OUTPUT_CONFIG, ConfigValueType.OBJECT))
        .handlesOwnValidationPath("translator")
        .handlesOwnValidationPath(OFFSETS_OUTPUT_CONFIG)
        .handlesOwnValidationPath(KafkaCommon.PARAMETER_CONFIG_PREFIX)
        .build();
  }

  @Override
  public Set<InstantiatedComponent> getComponents(Config config, boolean configure) {
    Set<InstantiatedComponent> components = Sets.newHashSet();

    if (doesRecordProgress(config) && !usingKafkaManagedOffsets(config)) {
      Output offsetsOutput = getOffsetsOutput(config, configure);
      components.add(new InstantiatedComponent(
          offsetsOutput, config.getConfig(OFFSETS_OUTPUT_CONFIG), "Offsets Output"));
    }

    return components;
  }

}
