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

import com.cloudera.labs.envelope.output.OutputFactory;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Tested;
import mockit.Verifications;
import mockit.integration.junit4.JMockit;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.kafka010.DirectKafkaInputDStream;
import org.apache.spark.streaming.kafka010.KafkaRDD;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Map;

import static com.cloudera.labs.envelope.validate.ValidationAssert.assertValidationFailures;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JMockit.class)
public class TestKafkaInput {

  @Mocked Config config;
  @Mocked JavaRDD<?> javaRDD;
  @Mocked KafkaRDD<String, String> kafkaRDD;
  @Mocked JavaDStream jDStream;
  @Mocked DirectKafkaInputDStream dkiDStream;
  @Tested KafkaInput mockedKafkaInput;

  @Test
  public void testNoTopicConfigure() {
    KafkaInput kafkaInput = new KafkaInput();
    assertValidationFailures(kafkaInput, config);
  }

  @Test
  public void testMultipleTopicsConfigure() {
    KafkaInput kafkaInput = new KafkaInput();
    new Expectations() {
      {
        config.getStringList(KafkaInput.TOPICS_CONFIG);
        returns(Lists.newArrayList("foo", "bar", "bar"));
        config.getString(KafkaInput.ENCODING_CONFIG);
        returns("string");
      }
    };

    kafkaInput.configure(config);
    assertEquals(kafkaInput.topics.size(), 2);
    assertTrue(kafkaInput.topics.contains("foo"));
    assertTrue(kafkaInput.topics.contains("bar"));
  }

  @Test
  public void testKafkaManagedOffsets() throws Exception {
    new MockUp<KafkaInput>() {
      @Mock
      public JavaDStream<?> getDStream() {
        return jDStream;
      }
    };

    new Expectations() {
      {
        config.getStringList(KafkaInput.TOPICS_CONFIG);
        returns(Lists.newArrayList("foo"));
        config.getString(KafkaInput.ENCODING_CONFIG);
        returns("string");
        config.hasPath(KafkaInput.GROUP_ID_CONFIG);
        returns(true);
        config.hasPath(KafkaInput.OFFSETS_OUTPUT_CONFIG);
        returns(false);
        javaRDD.rdd();
        returns(kafkaRDD);
        kafkaRDD.offsetRanges();
        returns(new OffsetRange[]{});
        jDStream.dstream();
        returns(dkiDStream);
      }
    };

    mockedKafkaInput.configure(config);
    mockedKafkaInput.recordProgress(javaRDD);

    new Verifications() {
      {
        dkiDStream.commitAsync(new OffsetRange[]{}); times = 1;
      }
    };
  }

  /**
   * Tests upserting offsets in a RandomOutput when
   * multiple topics are consumed.
   * Assumption: There is no separate testcase for
   * {@link KafkaInput#getLastOffsets} because code path is
   * already exercised by {@link KafkaInput#recordProgress}
   * call.
   * @throws Exception
   */
  @Test
  public void testRecordProgressMultiTopic() throws Exception {
    KafkaInput kafkaInput = new KafkaInput();
    new Expectations() {
      {
        config.getStringList(KafkaInput.TOPICS_CONFIG);
        returns(Lists.newArrayList("foo", "bar"));
        config.getString(KafkaInput.ENCODING_CONFIG);
        returns("string");
        config.hasPath(KafkaInput.OFFSETS_MANAGE_CONFIG);
        returns(true);
        config.getAnyRef(KafkaInput.OFFSETS_MANAGE_CONFIG);
        returns(true);
        config.hasPath(KafkaInput.OFFSETS_OUTPUT_CONFIG);
        returns(true);
        config.hasPath(KafkaInput.GROUP_ID_CONFIG);
        returns(true);
        config.getString(KafkaInput.GROUP_ID_CONFIG);
        returns("groupId1");
        config.hasPath(OutputFactory.TYPE_CONFIG_NAME);
        returns(true);
        config.getString(OutputFactory.TYPE_CONFIG_NAME);
        returns("com.cloudera.labs.envelope.kafka.DummyKafkaOffsetStore");
        OffsetRange [] range = new OffsetRange[] {
            OffsetRange.create("foo", 0, 0, 100),
            OffsetRange.create("foo", 1, 101, 1000),
            OffsetRange.create("bar", 0, 0, 700),
            OffsetRange.create("bar", 1, 701, 1000),
            OffsetRange.create("bar", 2, 1001, 100000)};
        javaRDD.rdd();
        returns(kafkaRDD);
        kafkaRDD.offsetRanges();
        returns(range);
      }
    };

    kafkaInput.configure(config);
    assertEquals(kafkaInput.offsetsOutput, null);
    kafkaInput.recordProgress(javaRDD);
    Map<String, Row> store = ((DummyKafkaOffsetStore)kafkaInput.offsetsOutput).store;
    assertEquals(store.size(), 5);
    assertEquals(store.get("groupId1foo0").get(3), 100L);
    assertEquals(store.get("groupId1bar2").get(3), 100000L);
    new Expectations() {
      {
        OffsetRange [] range = new OffsetRange[] {
            OffsetRange.create("foo", 0, 0, 900),
            OffsetRange.create("foo", 1, 101, 1000),
            OffsetRange.create("bar", 0, 0, 700),
            OffsetRange.create("bar", 1, 701, 1000),
            OffsetRange.create("bar", 2, 1001, 5000)};
        javaRDD.rdd();
        returns(kafkaRDD);
        kafkaRDD.offsetRanges();
        returns(range);
      }
    };

    kafkaInput.recordProgress(javaRDD);
    assertEquals(store.get("groupId1foo0").get(3), 900L);
    assertEquals(store.get("groupId1bar2").get(3), 5000L);
    assertEquals(((DummyKafkaOffsetStore)kafkaInput.offsetsOutput).store.size(), 5);
  }

}
