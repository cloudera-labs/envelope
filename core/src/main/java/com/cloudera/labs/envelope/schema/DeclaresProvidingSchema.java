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
 * A component should implement this interface when it is able to declare the schema of the data
 * it is providing.
 *
 * The downstream component that uses the data from this component can use this schema (via the
 * {@link UsesProvidedSchema} interface) in consideration of their operation. If the downstream
 * component uses the provided schema then that must be declared by this interface, otherwise
 * it is optional.
 *
 * Envelope can use this providing schema to validate that it meets the expected schema of the
 * downstream component. This makes implementing this interface a best practice even
 * when it is known to be optional.
 *
 * Note: Currently only supported for inputs to declare the providing schema to their
 * associated translators.
 */
public interface DeclaresProvidingSchema {

  /**
   * The schema of the data that the component is providing to other components.
   */
  StructType getProvidingSchema();

}
