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

import org.apache.spark.sql.Row;

import java.util.List;

/**
 * Random planners generate individual mutations of a row at a time.
 */
public interface RandomPlanner extends Planner {

  /**
   * Plan the random mutations for the arriving key of the step, using the arriving records and
   * the existing records.
   * @param key The row containing only the key fields and values.
   * @param arrivingForKey The arriving records of the key, from the DataFrame of the step.
   * @param existingForKey The existing records of the key, from the output of the step.
   * @return The list of random mutations for the key to be applied to the output. The output
   * will apply them in the same order as the list.
   */
  List<Row> planMutationsForKey(Row key, List<Row> arrivingForKey, List<Row> existingForKey);

  /**
   * Get the list of field names that constitute the natural key of the arriving records. This is
   * used by Envelope to group arriving records by natural key, and to retrieve existing records
   * from the output.
   */
  List<String> getKeyFieldNames();

}
