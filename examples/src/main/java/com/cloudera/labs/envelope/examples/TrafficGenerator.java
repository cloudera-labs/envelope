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

package com.cloudera.labs.envelope.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class TrafficGenerator {

  public static void main(final String[] args) throws Exception {
    final Properties props = new Properties();
    props.put("bootstrap.servers", args[0]);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    @SuppressWarnings("resource")
    KafkaProducer<String, String> producer = new KafkaProducer<>(props);

    Random random = new Random();

    while (true) {
      Long timestamp = System.currentTimeMillis();
      Integer numVehicles = random.nextInt(100);

      String message = timestamp + "," + numVehicles;

      producer.send(new ProducerRecord<String, String>(args[1], message));

      Thread.sleep(1000);
    }
  }

}
