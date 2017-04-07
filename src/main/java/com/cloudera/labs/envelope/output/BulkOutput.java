/**
 * Copyright © 2016-2017 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.output;

import java.util.List;
import java.util.Set;

import org.apache.spark.sql.DataFrame;

import com.cloudera.labs.envelope.plan.MutationType;

import scala.Tuple2;

/**
 * Bulk outputs write data out in bulk mutations of a DataFrame at a time.
 */
public interface BulkOutput extends Output {

  /**
   * Get the set of mutation types that this output supports.
   */
  Set<MutationType> getSupportedBulkMutationTypes();

  /**
   * Apply the bulk mutations to the external sink of the output.
   * @param planned The list of bulk mutations, where each mutation is composed of a tuple of a
   * mutation type and the mutation data as a DataFrame. The output must apply the mutations in
   * the same order as the list.
   */
  void applyBulkMutations(List<Tuple2<MutationType, DataFrame>> planned) throws Exception;

}
