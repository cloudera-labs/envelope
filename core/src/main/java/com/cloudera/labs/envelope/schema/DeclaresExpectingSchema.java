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
 * A component should implement this interface when it is able to declare the schema to which it
 * expects provided data to conform.
 *
 * The providing component can use this schema (via the {@link UsesExpectedSchema} interface)
 * in consideration of what schema it will provide. If the providing component uses the
 * expected schema then that must be declared by this interface, otherwise it is optional.
 *
 * Envelope can use this expecting schema to validate that the providing component
 * is meeting the expected schema. This makes implementing this interface a best practice even
 * when it is known to be optional.
 *
 * Note: Currently only supported for translators to declare the expected schema of their
 * associated inputs.
 */
public interface DeclaresExpectingSchema {

  /**
   * The schema that the component is expecting from the data provided to it.
   */
  StructType getExpectingSchema();

}
