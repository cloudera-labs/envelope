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

import com.cloudera.labs.envelope.utils.SchemaUtils;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestSchemaNegotiator {

  @Test
  public void testParentUsesExpected() {
    UsesExpectedSchema parent = new UsesExpectedSchema() {
      private Boolean received = false;
      @Override
      public void receiveExpectedSchema(StructType expectedSchema) {
        if (expectedSchema.equals(SchemaUtils.binaryValueSchema())) {
          received = true;
        }
      }
      @Override
      public String toString() {
        return received.toString();
      }
    };

    DeclaresExpectingSchema child = new DeclaresExpectingSchema() {
      @Override
      public StructType getExpectingSchema() {
        return SchemaUtils.binaryValueSchema();
      }
    };

    SchemaNegotiator.negotiate(parent, child);

    assertTrue(Boolean.valueOf(parent.toString()));
  }

  @Test
  public void testChildUsesProvided() {
    DeclaresProvidingSchema parent = new DeclaresProvidingSchema() {
      @Override
      public StructType getProvidingSchema() {
        return SchemaUtils.binaryValueSchema();
      }
    };

    UsesProvidedSchema child = new UsesProvidedSchema() {
      private Boolean received = false;
      @Override
      public void receiveProvidedSchema(StructType providedSchema) {
        if (providedSchema.equals(SchemaUtils.binaryValueSchema())) {
          received = true;
        }
      }
      @Override
      public String toString() {
        return received.toString();
      }
    };

    SchemaNegotiator.negotiate(parent, child);

    assertTrue(Boolean.valueOf(child.toString()));
  }

  @Test
  public void testNeitherImplement() {
    String hello = "hello";
    String world = "world";

    SchemaNegotiator.negotiate(hello, world);
  }

}
