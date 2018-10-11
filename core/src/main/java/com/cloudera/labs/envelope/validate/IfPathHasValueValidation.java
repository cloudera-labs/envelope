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
import com.typesafe.config.Config;

import java.util.Set;

public class IfPathHasValueValidation implements Validation {
  
  private String ifPath;
  private Object hasValue;
  private Validation thenValidation;
  
  public IfPathHasValueValidation(String ifPath, Object hasValue, Validation thenValidation) {
    this.ifPath = ifPath;
    this.hasValue = hasValue;
    this.thenValidation = thenValidation;
  }

  @Override
  public ValidationResult validate(Config config) {
    if (config.hasPath(ifPath)) {
      if (config.getAnyRef(ifPath).equals(hasValue)) {
        return thenValidation.validate(config);
      }
      else {
        return new ValidationResult(this, Validity.VALID,
            "Conditional configuration '" + ifPath + "' did not have value '" +
                hasValue + "', so it was not required that the validation (" +
                thenValidation + ") be checked");
      }
    }
    else {
      return new ValidationResult(this, Validity.VALID,
          "Conditional configuration '" + ifPath +
              "' does not exist, so it was not required that the validation (" +
              thenValidation + ") be checked");
    }
  }
  
  @Override
  public String toString() {
    return "Conditional validation that if path '" + ifPath + "' exists and has value '" +
        hasValue + "' then validation (" + thenValidation + ") is checked";
  }

  @Override
  public Set<String> getKnownPaths() {
    Set<String> knownPaths = Sets.newHashSet(ifPath);
    knownPaths.addAll(thenValidation.getKnownPaths());
    
    return Sets.newHashSet(knownPaths);
  }
  
}
