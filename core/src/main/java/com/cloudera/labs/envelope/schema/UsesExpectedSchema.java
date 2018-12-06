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
 * A component should implement this interface when it requires the expected schema of
 * the component that will be acting on this component.
 *
 * Currently only supported for inputs to receive the expecting schema of their
 * associated translators.
 */
public interface UsesExpectedSchema {

  /**
   * Receive the expected schema of the component acting on this component.
   */
  void receiveExpectedSchema(StructType expectedSchema);

}
