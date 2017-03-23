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
package com.cloudera.labs.envelope.input.translate;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

import scala.Tuple2;

@SuppressWarnings("serial")
public class TranslateFunction<T> implements FlatMapFunction<Tuple2<T, T>, Row> {
  private Config config;
  private Translator<T> translator;

  private static Logger LOG = LoggerFactory.getLogger(TranslateFunction.class);

  public TranslateFunction(Config config) {
    this.config = config;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Iterable<Row> call(Tuple2<T, T> keyAndMessage) throws Exception {
    T key = keyAndMessage._1;
    T message = keyAndMessage._2;

    if (translator == null) {
      translator = (Translator<T>)TranslatorFactory.create(config);
      LOG.info("Translator created: " + translator.getClass().getName());
    }

    return translator.translate(key, message);
  }
}
