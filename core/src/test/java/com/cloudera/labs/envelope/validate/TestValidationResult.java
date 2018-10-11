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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestValidationResult {

  private static class AlwaysValid implements Validation {

    @Override
    public ValidationResult validate(Config config) {
      return new ValidationResult(this, Validity.VALID, "hello", new RuntimeException("world"));
    }

    @Override
    public Set<String> getKnownPaths() {
      return null;
    }
  }

  private static class AlwaysInvalid implements Validation {

    @Override
    public ValidationResult validate(Config config) {
      return new ValidationResult(this, Validity.INVALID, "world");
    }

    @Override
    public Set<String> getKnownPaths() {
      return null;
    }
  }

  @Test
  public void testWithException() {
    ValidationResult vr = new AlwaysValid().validate(ConfigFactory.empty());

    assertEquals("hello", vr.getMessage());
    assertEquals(vr.getValidity(), Validity.VALID);
    assertTrue(vr.hasException());
    assertEquals("world", vr.getException().getMessage());
  }

  @Test
  public void testWithoutException() {
    ValidationResult vr = new AlwaysInvalid().validate(ConfigFactory.empty());

    assertEquals("world", vr.getMessage());
    assertEquals(vr.getValidity(), Validity.INVALID);
    assertFalse(vr.hasException());
  }

}
