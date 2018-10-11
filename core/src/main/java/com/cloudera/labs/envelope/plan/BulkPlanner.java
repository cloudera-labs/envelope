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

package com.cloudera.labs.envelope.plan;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.List;

/**
 * Bulk planners generate bulk mutations of a DataFrame at a time.
 */
public interface BulkPlanner extends Planner {

  /**
   * Plan the bulk mutations for the arriving DataFrame from the step.
   * @param arriving The DataFrame from the step.
   * @return A list of bulk mutations, where each mutation is composed of a tuple of a mutation
   * type and a mutation DataFrame. The mutations will be applied in the same order as the list.
   */
  List<Tuple2<MutationType, Dataset<Row>>> planMutationsForSet(Dataset<Row> arriving);

}
