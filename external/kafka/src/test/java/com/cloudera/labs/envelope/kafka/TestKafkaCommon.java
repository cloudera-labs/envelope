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

import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestKafkaCommon {
  private static Config appConfig = ConfigUtils.configFromResource("/kafka/kafka-input-test.conf").getConfig("input");

  @Test
  public void testCustomParams() throws Exception {
    Map<String, Object> kafkaParams = Maps.newHashMap();
    KafkaCommon.addCustomParams(kafkaParams, appConfig);
    assertEquals("SSL", kafkaParams.get("security.protocol"));
    assertEquals("/path/to/truststore.jks", kafkaParams.get("ssl.truststore.location"));
    assertEquals("changeme", kafkaParams.get("ssl.truststore.password"));
  }
}
