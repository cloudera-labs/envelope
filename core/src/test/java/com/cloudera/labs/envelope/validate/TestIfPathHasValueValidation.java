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
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestIfPathHasValueValidation {

  @Test
  public void testValidWhenPathHasValue() {
    Validation v = new IfPathHasValueValidation("hello", 2,
        Validations.single().allowedValues("hello", 1, 2, 3));
    ValidationResult vr = v.validate(ConfigFactory.parseString("hello = 2"));
    assertEquals(vr.getValidity(), Validity.VALID);
  }

  @Test
  public void testInvalidWhenPathHasValue() {
    Validation v = new IfPathHasValueValidation("hello", 2,
        Validations.single().allowedValues("hello", 3, 4, 5));
    ValidationResult vr = v.validate(ConfigFactory.parseString("hello = 2"));
    assertEquals(vr.getValidity(), Validity.INVALID);
  }

  @Test
  public void testPathDoesNotHaveValue() {
    Validation v = new IfPathHasValueValidation("hello", 4,
        Validations.single().allowedValues("hello", 1, 2, 3));
    ValidationResult vr = v.validate(ConfigFactory.parseString("hello = 2"));
    assertEquals(vr.getValidity(), Validity.VALID);
  }

  @Test
  public void testPathDoesNotExist() {
    Validation v = new IfPathHasValueValidation("hello", 4,
        Validations.single().allowedValues("hello", 1, 2, 3));
    ValidationResult vr = v.validate(ConfigFactory.parseString("world = 2"));
    assertEquals(vr.getValidity(), Validity.VALID);
  }

  @Test
  public void testKnownPaths() {
    Validation v = new IfPathHasValueValidation("hello", 2,
        Validations.single().allowedValues("world", 1, 2, 3));
    assertEquals(Sets.newHashSet("hello", "world"), v.getKnownPaths());
  }

}
