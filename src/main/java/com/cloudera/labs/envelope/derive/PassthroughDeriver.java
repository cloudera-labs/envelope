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
package com.cloudera.labs.envelope.derive;

import java.util.Iterator;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.typesafe.config.Config;

public class PassthroughDeriver implements Deriver {

  @Override
  public void configure(Config config) {}

  @Override
  public Dataset<Row> derive(Map<String, Dataset<Row>> dependencies) throws Exception {
    if (dependencies.isEmpty()) {
      throw new RuntimeException("Passthrough deriver requires at least one dependency");
    }

    Iterator<Dataset<Row>> dependencyIterator = dependencies.values().iterator();

    Dataset<Row> unioned = dependencyIterator.next();
    while (dependencyIterator.hasNext()) {
      unioned = unioned.union(dependencyIterator.next());
    }

    return unioned;
  }

}
