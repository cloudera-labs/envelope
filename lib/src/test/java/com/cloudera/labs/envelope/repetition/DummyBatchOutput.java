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

package com.cloudera.labs.envelope.repetition;

import com.cloudera.labs.envelope.output.BulkOutput;
import com.cloudera.labs.envelope.plan.MutationType;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.List;
import java.util.Set;

public class DummyBatchOutput implements BulkOutput {

  private static final Logger LOG = LoggerFactory.getLogger(DummyBatchOutput.class);
  private static List<Row> outputs = Lists.newArrayList();

  @Override
  public Set<MutationType> getSupportedBulkMutationTypes() {
    return Sets.newHashSet(MutationType.INSERT);
  }

  @Override
  public void applyBulkMutations(List<Tuple2<MutationType, Dataset<Row>>> planned) {
    for (Tuple2<MutationType, Dataset<Row>> type : planned) {
      LOG.info("Adding {} outputs", type._2().count());
      outputs.addAll(type._2().collectAsList());
    }
  }

  @Override
  public void configure(Config config) {

  }

  public static List<Row> getOutputs() {
    return outputs;
  }
  
}
