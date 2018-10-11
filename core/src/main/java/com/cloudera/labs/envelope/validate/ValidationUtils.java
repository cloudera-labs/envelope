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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ValidationUtils {
  
  private static Logger LOG = LoggerFactory.getLogger(ValidationUtils.class);

  public static boolean hasValidationFailures(List<ValidationResult> results) {
    for (ValidationResult result : results) {
      if (result.getValidity() == Validity.INVALID) {
        return true;
      }
    }
    
    return false;
  }
  
  public static void logValidationResults(List<ValidationResult> results) {
    for (ValidationResult result : results) {
      if (result.getValidity() == Validity.INVALID) {
        LOG.error("Configuration validation failure: " + result.getMessage());
        if (result.hasException()) {
          result.getException().printStackTrace();
        }
      }
      else {
        LOG.debug("Configuration validation success: " + result.getMessage());
      }
    }
  }
  
  public static void prefixValidationResultMessages(List<ValidationResult> results, String prefix) {
    for (ValidationResult result : results) {
      result.setMessage(prefix + ": " + result.getMessage());
    }
  }
  
}
