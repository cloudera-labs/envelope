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

import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestKafkaInput {
  private static Config appConfig = ConfigUtils.configFromResource("/kafka/kafka-input-test.conf").getConfig("input");
  private KafkaInput kafka;

  @Before
  public void before() throws Exception {
    kafka = new KafkaInput();
    kafka.configure(appConfig);
  }

  @Test
  public void testCustomParams() throws Exception {
    Map<String, Object> kafkaParams = Maps.newHashMap();
    kafka.addCustomParams(kafkaParams);
    assertEquals("SSL", kafkaParams.get("security.protocol"));
    assertEquals("/path/to/truststore.jks", kafkaParams.get("ssl.truststore.location"));
    assertEquals("changeme", kafkaParams.get("ssl.truststore.password"));
  }
}