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
package com.cloudera.labs.envelope.output;

import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import com.cloudera.labs.envelope.plan.MutationType;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

import scala.Tuple2;

public class KafkaOutput implements BulkOutput {

  public static final String BROKERS_CONFIG_NAME = "brokers";
  public static final String TOPIC_CONFIG_NAME = "topic";
  public static final String FIELD_DELIMITER_CONFIG_NAME = "field.delimiter";

  private Config config;


  @Override
  public void configure(Config config) {
    this.config = config;
  }

  @SuppressWarnings("serial")
  @Override
  public void applyBulkMutations(List<Tuple2<MutationType, DataFrame>> planned) {
    for (Tuple2<MutationType, DataFrame> mutation : planned) {
      MutationType mutationType = mutation._1();
      DataFrame mutationDF = mutation._2();

      if (mutationType.equals(MutationType.INSERT)) {
        mutationDF.javaRDD().foreach(
          new SendRowToKafkaFunction(
            config.getString(TOPIC_CONFIG_NAME),
            config.getString(BROKERS_CONFIG_NAME),
            getDelimiter()
          )
        );
      }
    }
  }

  @Override
  public Set<MutationType> getSupportedBulkMutationTypes() {
    return Sets.newHashSet(MutationType.INSERT);
  }

  private String getDelimiter() {
    if (!config.hasPath(FIELD_DELIMITER_CONFIG_NAME)) return ",";

    String delimiter = config.getString(FIELD_DELIMITER_CONFIG_NAME);

    if (delimiter.startsWith("chars:")) {
      String[] codePoints = delimiter.substring("chars:".length()).split(",");

      StringBuilder delimiterBuilder = new StringBuilder();
      for (String codePoint : codePoints) {
        delimiterBuilder.append(Character.toChars(Integer.parseInt(codePoint)));
      }

      return delimiterBuilder.toString();
    }
    else {
      return delimiter;
    }
  }


  private static class SendRowToKafkaFunction implements VoidFunction<Row> {
    private KafkaProducer<String, String> producer;
    private String topic;
    private String brokers;
    private String delimiter;
    private Joiner joiner;

    public SendRowToKafkaFunction(String topic, String brokers, String delimiter) {
      this.topic = topic;
      this.brokers = brokers;
      this.delimiter = delimiter;
    }

    private void initialize() {
      Properties producerProps = new Properties();
      producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      producerProps.put("bootstrap.servers", brokers);

      Serializer<String> serializer = new StringSerializer();
      producer = new KafkaProducer<String, String>(producerProps, serializer, serializer);

      joiner = Joiner.on(delimiter);
    }

    @Override
    public void call(Row mutation) throws Exception {
      if (producer == null) {
        initialize();
      }

      List<Object> values = Lists.newArrayList();

      for (int fieldIndex = 0; fieldIndex < mutation.size(); fieldIndex++) {
        values.add(mutation.get(fieldIndex));
      }

      String message = joiner.join(values);

      producer.send(new ProducerRecord<String, String>(topic, message));
    }
  }

}
