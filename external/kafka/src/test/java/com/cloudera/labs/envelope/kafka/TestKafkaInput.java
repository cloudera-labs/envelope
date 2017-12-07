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
