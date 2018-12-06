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

package com.cloudera.labs.envelope.translate;

import com.cloudera.labs.envelope.spark.Contexts;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TestTranslateFunction {

  @Test
  public void testAppendRaw() {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(TranslatorFactory.TYPE_CONFIG_NAME, DummyTranslator.class.getName());
    configMap.put(TranslateFunction.APPEND_RAW_ENABLED_CONFIG, true);
    Config config = ConfigFactory.parseMap(configMap);

    TranslateFunction tf = new TranslateFunction(config);
    tf.receiveProvidedSchema(tf.getExpectingSchema());
    Dataset<Row> raw = Contexts.getSparkSession().createDataFrame(
        Lists.newArrayList(RowFactory.create("hello?")), tf.getExpectingSchema());
    Dataset<Row> translated = raw.flatMap(tf, RowEncoder.apply(tf.getProvidingSchema()));

    assertEquals(2, translated.schema().size());
    assertEquals("_value", translated.schema().fields()[1].name());
    assertEquals("hello?", translated.collectAsList().get(0).getString(1));
  }

  @Test
  public void testExplicitDontAppendRaw() {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(TranslatorFactory.TYPE_CONFIG_NAME, DummyTranslator.class.getName());
    configMap.put(TranslateFunction.APPEND_RAW_ENABLED_CONFIG, false);
    Config config = ConfigFactory.parseMap(configMap);

    TranslateFunction tf = new TranslateFunction(config);
    tf.receiveProvidedSchema(tf.getExpectingSchema());
    Dataset<Row> raw = Contexts.getSparkSession().createDataFrame(
        Lists.newArrayList(RowFactory.create("hello?")), tf.getExpectingSchema());
    Dataset<Row> translated = raw.flatMap(tf, RowEncoder.apply(tf.getProvidingSchema()));

    assertEquals(1, translated.schema().size());
    assertNotEquals("_value", translated.schema().fields()[0].name());
  }

  @Test
  public void testImplicitDontAppendRaw() {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(TranslatorFactory.TYPE_CONFIG_NAME, DummyTranslator.class.getName());
    Config config = ConfigFactory.parseMap(configMap);

    TranslateFunction tf = new TranslateFunction(config);
    tf.receiveProvidedSchema(tf.getExpectingSchema());
    Dataset<Row> raw = Contexts.getSparkSession().createDataFrame(
        Lists.newArrayList(RowFactory.create("hello?")), tf.getExpectingSchema());
    Dataset<Row> translated = raw.flatMap(tf, RowEncoder.apply(tf.getProvidingSchema()));

    assertEquals(1, translated.schema().size());
    assertNotEquals("_value", translated.schema().fields()[0].name());
  }

}
