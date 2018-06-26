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

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;

public class RawStringTranslator implements Translator<String, String>, ProvidesAlias {

  @Override
  public void configure(Config config) { }

  @Override
  public Iterable<Row> translate(String key, String value) throws Exception {
    return Collections.singleton(RowFactory.create(key, value));
  }

  @Override
  public StructType getSchema() {
    return RowUtils.structTypeFor(
        Lists.newArrayList("key", "value"),
        Lists.newArrayList("string", "string")
    );
  }

  @Override
  public String getAlias() {
    return "rawstring";
  }

}
