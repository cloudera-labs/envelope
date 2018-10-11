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

package com.cloudera.labs.envelope.input.translate;

import com.cloudera.labs.envelope.component.InstantiatedComponent;
import com.cloudera.labs.envelope.component.InstantiatesComponents;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Iterator;
import java.util.Set;

@SuppressWarnings("serial")
public class TranslateFunction<K, V> implements FlatMapFunction<Tuple2<K, V>, Row>, InstantiatesComponents {
  
  private Config config;
  private Translator<K, V> translator;

  private static Logger LOG = LoggerFactory.getLogger(TranslateFunction.class);

  public TranslateFunction(Config config) {
    this.config = config;
  }

  @Override
  public Iterator<Row> call(Tuple2<K, V> keyAndValue) throws Exception {
    K key = keyAndValue._1;
    V value = keyAndValue._2;
    
    Iterable<Row> translated = getTranslator(true).translate(key, value);
    
    // TODO: Optionally append raw message

    return translated.iterator();
  }
  
  @SuppressWarnings("unchecked")
  public Translator<K, V> getTranslator(boolean configure) {
    if (configure) {
      if (translator == null) {
        translator = (Translator<K, V>)TranslatorFactory.create(config, configure);
        LOG.debug("Translator created: " + translator.getClass().getName());
      }
      return translator;
    }
    else {
      return (Translator<K, V>)TranslatorFactory.create(config, configure);
    }
  }
  
  public StructType getSchema() {
    return TranslatorFactory.create(config, true).getSchema();
  }

  @Override
  public Set<InstantiatedComponent> getComponents(Config config, boolean configure) {
    Translator translator = getTranslator(configure);

    Set<InstantiatedComponent> components = Sets.newHashSet();
    components.add(new InstantiatedComponent(translator, config, "Translator"));

    return components;
  }
  
}
