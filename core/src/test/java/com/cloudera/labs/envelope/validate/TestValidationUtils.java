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

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestValidationUtils {

  @Test
  public void testHasValidationFailures() {
    List<ValidationResult> vrs = Lists.newArrayList(
        new ValidationResult(Validity.VALID, "hello"),
        new ValidationResult(Validity.INVALID, "world")
    );

    assertTrue(ValidationUtils.hasValidationFailures(vrs));
  }

  @Test
  public void testHasNoValidationFailures() {
    List<ValidationResult> vrs = Lists.newArrayList(
        new ValidationResult(Validity.VALID, "hello"),
        new ValidationResult(Validity.VALID, "world")
    );

    assertFalse(ValidationUtils.hasValidationFailures(vrs));
  }

  @Test
  public void testPrefixValidationResultMessages() {
    List<ValidationResult> vrs = Lists.newArrayList(
        new ValidationResult(Validity.VALID, "hello"),
        new ValidationResult(Validity.INVALID, "world")
    );

    ValidationUtils.prefixValidationResultMessages(vrs, "test");

    assertEquals(vrs.get(0).getMessage(), "test: hello");
    assertEquals(vrs.get(1).getMessage(), "test: world");
  }

}
