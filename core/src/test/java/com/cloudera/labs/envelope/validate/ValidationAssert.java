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

import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ValidationAssert {

  public static void assertNoValidationFailures(ProvidesValidations validee, Config config) {
    List<ValidationResult> results = Validator.validate(validee, config);
    boolean hasFailures = ValidationUtils.hasValidationFailures(results);

    if (hasFailures) {
      ValidationUtils.logValidationResults(results);
    }

    assertFalse("Configuration is invalid, but was supposed to be valid. " +
        "See above validation results.", hasFailures);
  }

  public static void assertValidationFailures(ProvidesValidations validee, Config config) {
    List<ValidationResult> results = Validator.validate(validee, config);
    boolean hasFailures = ValidationUtils.hasValidationFailures(results);

    if (!hasFailures) {
      ValidationUtils.logValidationResults(results);
    }

    assertTrue("Configuration is valid, but was supposed to be invalid. " +
        "See above validation results.", hasFailures);
  }

}
