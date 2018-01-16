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
package com.cloudera.labs.envelope.input.translate;

import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

import scala.Tuple2;

@SuppressWarnings("serial")
public class TranslateFunction<K, V> implements FlatMapFunction<Tuple2<K, V>, Row> {
  
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
      translator = (Translator<K, V>)TranslatorFactory.create(config);
      LOG.info("Translator created: " + translator.getClass().getName());
    }
    
    Iterable<Row> translated = translator.translate(key, value);
    
    // TODO: Optionally append raw message

    return translated.iterator();
  }
  
  public StructType getSchema() {
    return TranslatorFactory.create(config).getSchema();
  }
  
}
