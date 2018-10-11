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

package com.cloudera.labs.envelope.output;

import com.cloudera.labs.envelope.plan.MutationType;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Set;

/**
 * Random outputs write data out in individual mutations of a row at a time.
 */
public interface RandomOutput extends Output {

  /**
   * Get the set of mutation types that this output supports.
   */
  Set<MutationType> getSupportedRandomMutationTypes();

  /**
   * Apply the random mutations to the external sink of the output.
   * @param planned The list of random mutations, where each mutation is composed of an object of a
   * mutation type and the mutation data as a Spark SQL Row. The output must apply the mutations in
   * the same order as the list.
   */
  void applyRandomMutations(List<Row> planned) throws Exception;

  /**
   * Get the existing records from the output that matches the given filters.
   * @param filters An iterable collection of filters, where each filter is a Row that the existing
   * records must exactly match all values on. This is typically used for key lookups, where
   * the iterable collection is a batch of keys (each defined as a Row).
   * @return The iterable collection of existing records that match the filters. There can be
   * zero-to-one-to-many existing record rows per filter row.
   */
  Iterable<Row> getExistingForFilters(Iterable<Row> filters) throws Exception;

}
