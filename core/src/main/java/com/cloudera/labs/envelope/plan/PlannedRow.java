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

import org.apache.spark.sql.Row;

/**
 * A typed record in the storage schema with an attached mutation plan.
 */
public class PlannedRow {

  private MutationType mutationType;
  private Row row;

  public PlannedRow(Row row, MutationType mutationType) {
    this.row = row;
    this.mutationType = mutationType;
  }

  public MutationType getMutationType() {
    return mutationType;
  }

  public Row getRow() {
    return row;
  }

  public void setMutationType(MutationType mutationType) {
    this.mutationType = mutationType;
  }

  public void setRow(Row row) {
    this.row = row;
  }

  @Override
  public String toString() {
    return String.format("Plan: [Mutation: %s], [Row: %s]", mutationType, row.mkString(","));
  }

}
