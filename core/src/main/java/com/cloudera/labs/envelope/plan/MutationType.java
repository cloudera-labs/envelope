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

/**
 * The type of mutation being planned.
 */
public enum MutationType {
  
  /**
   * Do not apply this mutation.
   */
  NONE

  /**
   * Insert the mutation as new rows. Do not impact existing rows.
   */
  , INSERT

  /**
   * Update the matching existing rows with the values of the mutation. Do not add new rows.
   */
  , UPDATE

  /**
   * Delete the existing rows that match the mutation. May contain non-key fields.
   */
  , DELETE

  /**
   * Insert or update the mutation based on whether the key of the mutation already exists.
   */
  , UPSERT

  /**
   * Replace all existing rows with the mutation.
   */
  , OVERWRITE
  ;

  public static final String MUTATION_TYPE_FIELD_NAME = "_mutation";
  
}
