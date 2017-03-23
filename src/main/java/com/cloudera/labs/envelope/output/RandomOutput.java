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

import org.apache.spark.sql.Row;

import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.plan.PlannedRow;

public interface RandomOutput extends Output {

  Set<MutationType> getSupportedRandomMutationTypes();

  void applyRandomMutations(List<PlannedRow> planned) throws Exception;

  Iterable<Row> getExistingForFilters(Iterable<Row> filters) throws Exception;

}
