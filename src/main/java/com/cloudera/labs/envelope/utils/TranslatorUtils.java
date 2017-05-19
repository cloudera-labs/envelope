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
package com.cloudera.labs.envelope.utils;

import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.labs.envelope.input.translate.Translator;
import com.cloudera.labs.envelope.input.translate.TranslatorFactory;
import com.typesafe.config.Config;

import scala.Tuple2;

public class TranslatorUtils {

  public static final String APPEND_RAW_ENABLED_CONFIG_NAME = "append.raw.enabled";
  public static final String APPEND_RAW_KEY_FIELD_NAME_CONFIG_NAME = "append.raw.key.field.name";
  public static final String APPEND_RAW_VALUE_FIELD_NAME_CONFIG_NAME = "append.raw.value.field.name";
  public static final String APPEND_RAW_DEFAULT_KEY_FIELD_NAME = "_key";
  public static final String APPEND_RAW_DEFAULT_VALUE_FIELD_NAME = "_value";
  
  public static boolean doesAppendRaw(Config config) {
    return config.hasPath(APPEND_RAW_ENABLED_CONFIG_NAME) && config.getBoolean(APPEND_RAW_ENABLED_CONFIG_NAME);
  }
  
  public static String getAppendRawKeyFieldName(Config config) {
    if (config.hasPath(APPEND_RAW_KEY_FIELD_NAME_CONFIG_NAME)) {
      return config.getString(APPEND_RAW_KEY_FIELD_NAME_CONFIG_NAME);
    }
    else {
      return APPEND_RAW_DEFAULT_KEY_FIELD_NAME;
    }
  }
  
  public static String getAppendRawValueFieldName(Config config) {
    if (config.hasPath(APPEND_RAW_VALUE_FIELD_NAME_CONFIG_NAME)) {
      return config.getString(APPEND_RAW_VALUE_FIELD_NAME_CONFIG_NAME);
    }
    else {
      return APPEND_RAW_DEFAULT_VALUE_FIELD_NAME;
    }
  }
  
  @SuppressWarnings("serial")
  public static class TranslateFunction<K, V> implements FlatMapFunction<Tuple2<K, V>, Row> {
    private Config config;
    private Translator<K, V> translator;

    private static Logger LOG = LoggerFactory.getLogger(TranslateFunction.class);

    public TranslateFunction(Config config) {
      this.config = config;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<Row> call(Tuple2<K, V> keyAndValue) throws Exception {
      K key = keyAndValue._1;
      V value = keyAndValue._2;

      if (translator == null) {
        translator = (Translator<K, V>) TranslatorFactory.create(config);
        LOG.info("Translator created: " + translator.getClass().getName());
      }

      return translator.translate(key, value).iterator();
    }
  }
  
}
