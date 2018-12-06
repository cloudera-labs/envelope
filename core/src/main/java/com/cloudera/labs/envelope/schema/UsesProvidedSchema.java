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

package com.cloudera.labs.envelope.schema;

import org.apache.spark.sql.types.StructType;

/**
 * A component should implement this interface if it requires the schema of the component
 * providing data to it.
 *
 * Currently only supported for translators to receive the schema of their associated inputs.
 */
public interface UsesProvidedSchema {

  /**
   * Receive the provided schema of the component providing data to this component.
   */
  void receiveProvidedSchema(StructType providedSchema);

}
