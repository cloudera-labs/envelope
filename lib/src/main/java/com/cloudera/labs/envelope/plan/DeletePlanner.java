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
package com.cloudera.labs.envelope.plan;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

import scala.Tuple2;

public class DeletePlanner implements BulkPlanner {

  @Override
  public List<Tuple2<MutationType, Dataset<Row>>> planMutationsForSet(Dataset<Row> arriving) {
    return Lists.newArrayList(Collections.singleton(new Tuple2<>(MutationType.DELETE, arriving)));
  }

  @Override
  public Set<MutationType> getEmittedMutationTypes() {
    return Sets.newHashSet(MutationType.DELETE);
  }
  
  @Override
  public void configure(Config config) {}

  @Override
  public String getAlias() {
    return "delete";
  }
}
