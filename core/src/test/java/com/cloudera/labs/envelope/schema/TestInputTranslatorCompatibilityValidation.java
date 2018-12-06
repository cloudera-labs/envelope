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

package com.cloudera.labs.envelope.schema;

import com.cloudera.labs.envelope.input.InputFactory;
import com.cloudera.labs.envelope.input.StreamInput;
import com.cloudera.labs.envelope.run.DataStep;
import com.cloudera.labs.envelope.run.StreamingStep;
import com.cloudera.labs.envelope.translate.Translator;
import com.cloudera.labs.envelope.translate.TranslatorFactory;
import com.cloudera.labs.envelope.utils.SchemaUtils;
import com.cloudera.labs.envelope.validate.ValidationAssert;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.junit.Test;

import java.util.Map;

public class TestInputTranslatorCompatibilityValidation {

  @Test
  public void testInputTranslatorCompatible() {
    Map<String, Object> translatorConfigMap = Maps.newHashMap();
    translatorConfigMap.put(
        TranslatorFactory.TYPE_CONFIG_NAME, StringExpectingTranslator.class.getName());

    Map<String, Object> inputConfigMap = Maps.newHashMap();
    inputConfigMap.put(
        InputFactory.TYPE_CONFIG_NAME, StringProvidingStreamInput.class.getName());
    inputConfigMap.put(
        StreamingStep.TRANSLATOR_WITHIN_INPUT_PROPERTY, translatorConfigMap);

    Map<String, Object> stepConfigMap = Maps.newHashMap();
    stepConfigMap.put(DataStep.INPUT_TYPE, inputConfigMap);
    Config stepConfig = ConfigFactory.parseMap(stepConfigMap);

    StreamingStep step = new StreamingStep("to_validate");

    ValidationAssert.assertNoValidationFailures(step, stepConfig);
  }

  @Test
  public void testInputTranslatorIncompatible() {
    Map<String, Object> translatorConfigMap = Maps.newHashMap();
    translatorConfigMap.put(
        TranslatorFactory.TYPE_CONFIG_NAME, BinaryExpectingTranslator.class.getName());

    Map<String, Object> inputConfigMap = Maps.newHashMap();
    inputConfigMap.put(
        InputFactory.TYPE_CONFIG_NAME, StringProvidingStreamInput.class.getName());
    inputConfigMap.put(
        StreamingStep.TRANSLATOR_WITHIN_INPUT_PROPERTY, translatorConfigMap);

    Map<String, Object> stepConfigMap = Maps.newHashMap();
    stepConfigMap.put(DataStep.INPUT_TYPE, inputConfigMap);
    Config stepConfig = ConfigFactory.parseMap(stepConfigMap);

    StreamingStep step = new StreamingStep("to_validate");

    ValidationAssert.assertValidationFailures(step, stepConfig);
  }

  public static class StringProvidingStreamInput implements StreamInput {
    public StringProvidingStreamInput() { }

    @Override
    public JavaDStream<?> getDStream() throws Exception {
      return null;
    }

    @Override
    public Function<?, Row> getMessageEncoderFunction() {
      return null;
    }

    @Override
    public void configure(Config config) {

    }

    @Override
    public StructType getProvidingSchema() {
      return SchemaUtils.stringValueSchema();
    }
  }

  public static class StringExpectingTranslator implements Translator {
    public StringExpectingTranslator() { }

    @Override
    public void configure(Config config) {

    }

    @Override
    public Iterable<Row> translate(Row message) throws Exception {
      return null;
    }

    @Override
    public StructType getExpectingSchema() {
      return SchemaUtils.stringValueSchema();
    }

    @Override
    public StructType getProvidingSchema() {
      return SchemaUtils.stringValueSchema();
    }
  }

  public static class BinaryExpectingTranslator implements Translator {
    public BinaryExpectingTranslator() { }

    @Override
    public void configure(Config config) {

    }

    @Override
    public Iterable<Row> translate(Row message) throws Exception {
      return null;
    }

    @Override
    public StructType getExpectingSchema() {
      return SchemaUtils.binaryValueSchema();
    }

    @Override
    public StructType getProvidingSchema() {
      return SchemaUtils.stringValueSchema();
    }
  }

}
