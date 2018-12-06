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

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Negotiates the schemas between two components, where the parent component provides
 * data to the child component. This allows components to dynamically determine their
 * schemas based on each others requirements, and for Envelope to validate schema compatibility.
 *
 * The negotiation occurs in this sequence:
 * 1. The child component declares the schema it is expecting from the parent component
 * 2. The parent component is given that expected schema
 * 3. The parent component declares the schema it is providing to the child component
 * 4. The child component is given that provided schema
 *
 * A component can optionally contribute to the schema negotiation by implementing one or more
 * of the interfaces {@link DeclaresExpectingSchema}, {@link UsesExpectedSchema},
 * {@link DeclaresProvidingSchema}, {@link UsesProvidedSchema}. However, if the parent component
 * uses the expected schema then the child component must declare it, and if the child component
 * uses the provided schema then the parent component must declare it.
 *
 * Note: Currently schema negotiation is only implemented for inputs and their translators.
 */
public class SchemaNegotiator {

  public static void negotiate(Object parent, Object child) {
    // If the parent requires it, send it the child's expecting schema
    if (parent instanceof UsesExpectedSchema && child instanceof DeclaresExpectingSchema) {
      ((UsesExpectedSchema)parent).receiveExpectedSchema(
          ((DeclaresExpectingSchema)child).getExpectingSchema());
    }

    // If the child requires it, send it the parent's providing schema
    if (child instanceof UsesProvidedSchema && parent instanceof DeclaresProvidingSchema) {
      ((UsesProvidedSchema)child).receiveProvidedSchema(
          ((DeclaresProvidingSchema)parent).getProvidingSchema());
    }
  }

}
