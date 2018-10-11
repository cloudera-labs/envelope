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

package com.cloudera.labs.envelope.derive;

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Iterator;
import java.util.Map;

public class PassthroughDeriver implements Deriver, ProvidesAlias {

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
      Dataset<Row> next = dependencyIterator.next();

      if (!unioned.schema().equals(next.schema())) {
        throw new RuntimeException("All dependencies of the passthrough deriver must have the same schema");
      }

      unioned = unioned.union(next);
    }

    return unioned;
  }

  @Override
  public String getAlias() {
    return "passthrough";
  }
  
}
