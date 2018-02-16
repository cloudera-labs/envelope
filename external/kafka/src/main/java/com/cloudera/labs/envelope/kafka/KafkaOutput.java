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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.cloudera.labs.envelope.kafka.serde.AvroSerializer;
import com.cloudera.labs.envelope.kafka.serde.DelimitedSerializer;
import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.output.BulkOutput;
import com.cloudera.labs.envelope.plan.MutationType;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

import scala.Tuple2;

public class KafkaOutput implements BulkOutput, ProvidesAlias {

  public static final String BROKERS_CONFIG_NAME = "brokers";
  public static final String TOPIC_CONFIG_NAME = "topic";
  public static final String SERIALIZER_CONFIG_PREFIX = "serializer.";
  public static final String SERIALIZER_TYPE_CONFIG_NAME = SERIALIZER_CONFIG_PREFIX + "type";
  public static final String DELIMITED_SERIALIZER = "delimited";
  public static final String AVRO_SERIALIZER = "avro";
  
  private Config config;

  @Override
  public void configure(Config config) {    
    this.config = config;
  }

  @Override
  public void applyBulkMutations(List<Tuple2<MutationType, Dataset<Row>>> planned) {
    for (Tuple2<MutationType, Dataset<Row>> mutation : planned) {
      MutationType mutationType = mutation._1();
      Dataset<Row> mutationDF = mutation._2();

      if (mutationType.equals(MutationType.INSERT)) {
        mutationDF.javaRDD().foreach(new SendRowToKafkaFunction(config));
      }
    }
  }

  @Override
  public Set<MutationType> getSupportedBulkMutationTypes() {
    return Sets.newHashSet(MutationType.INSERT);
  }

  @Override
  public String getAlias() {
    return "kafka";
  }

  @SuppressWarnings("serial")
  private static class SendRowToKafkaFunction implements VoidFunction<Row> {
    private KafkaProducer<Row, Row> producer;
    private String topic;
    private String brokers;
    private String serializerType;
    private Config config;

    public SendRowToKafkaFunction(Config config) {
      this.brokers = config.getString(BROKERS_CONFIG_NAME);
      this.topic = config.getString(TOPIC_CONFIG_NAME);
      this.serializerType = config.getString(SERIALIZER_TYPE_CONFIG_NAME);
      this.config = config;
    }

    private void initialize() {
      Serializer<Row> keySerializer, valueSerializer;
      switch (serializerType) {
        case DELIMITED_SERIALIZER:
          keySerializer = new DelimitedSerializer();
          valueSerializer = new DelimitedSerializer();
          break;
        case AVRO_SERIALIZER:
          keySerializer = new AvroSerializer();
          valueSerializer = new AvroSerializer();
          break;
        default:
          throw new RuntimeException("Kafka output does not support serializer type: " + serializerType);
      }
      
      Map<String, ?> serializerConfiguration = getSerializerConfiguration();
      keySerializer.configure(serializerConfiguration, true);
      valueSerializer.configure(serializerConfiguration, false);
      
      Map<String, Object> producerProps = Maps.newHashMap();
      producerProps.put("bootstrap.servers", brokers);

      KafkaCommon.addCustomParams(producerProps, config);
      
      producer = new KafkaProducer<>(producerProps, keySerializer, valueSerializer);
    }

    @Override
    public void call(Row mutation) throws Exception {
      if (producer == null) {
        initialize();
      }
      
      producer.send(new ProducerRecord<Row, Row>(topic, mutation));
    }
    
    private Map<String, ?> getSerializerConfiguration() {
      Map<String, Object> configs = Maps.newHashMap();
      
      for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
        String propertyName = entry.getKey();
        if (propertyName.startsWith(SERIALIZER_CONFIG_PREFIX)) {
          String paramName = propertyName.substring(SERIALIZER_CONFIG_PREFIX.length());
          String paramValue = config.getString(propertyName);

          configs.put(paramName, paramValue);
        }
      }
      
      return configs;
    }
  }

}
