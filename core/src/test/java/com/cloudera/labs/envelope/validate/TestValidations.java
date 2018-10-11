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

package com.cloudera.labs.envelope.validate;

import com.google.common.collect.Sets;
import com.typesafe.config.ConfigValueType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestValidations {

  @Test
  public void testValidationsBuilder() {
    Validations v = Validations
        .builder()
        .mandatoryPath("mandatory1")
        .mandatoryPath("mandatory2", ConfigValueType.STRING)
        .optionalPath("optional1")
        .optionalPath("optional2", ConfigValueType.NUMBER)
        .atMostOnePathExists("atmostone1", "atmostone2")
        .atMostOnePathExists(ConfigValueType.BOOLEAN, "atmostone3", "atmostone4")
        .exactlyOnePathExists("exactlyone1", "exactlyone2")
        .exactlyOnePathExists(ConfigValueType.STRING, "exactlyone3", "exactlyone4")
        .ifPathHasValue("hasvalue", true, Validations.single().allowedValues("hasvalue", true))
        .ifPathExists("pathexists", Validations.single().mandatoryPath("pathexists"))
        .allowedValues("allowed", "hello", "world")
        .build();

    // The specified validations plus the automatic non-empty validations
    assertEquals(26, v.size());
  }

  @Test
  public void testValidationBuilder() {
    Validation v = Validations.single().mandatoryPath("mandatory");

    assertEquals(v.getKnownPaths(), Sets.newHashSet("mandatory"));
  }

  @Test
  public void testAllowEmpty() {
    Validations v = Validations.builder()
        .mandatoryPath("mandatory")
        .allowEmptyValue("mandatory")
        .build();

    assertEquals(1, v.size());
    assertTrue(v.iterator().next() instanceof MandatoryPathValidation);
  }

  @Test
  public void testAllowsUnrecognizedPaths() {
    Validations v = Validations.builder()
        .allowUnrecognizedPaths()
        .build();

    assertTrue(v.allowsUnrecognizedPaths());
  }

  @Test
  public void testOwnValidationsPaths() {
    Validations v = Validations.builder()
        .handlesOwnValidationPath("hello")
        .build();

    assertEquals(Sets.newHashSet("hello"), v.getOwnValidatingPaths());
  }

}
