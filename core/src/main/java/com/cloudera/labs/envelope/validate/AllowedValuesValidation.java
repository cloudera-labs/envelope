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

public class AllowedValuesValidation implements Validation {
  
  private String path;
  private Set<Object> allowedValues;
  
  public AllowedValuesValidation(String path, Object... allowedValues) {
    this.path = path;
    this.allowedValues = Sets.newHashSet(allowedValues);
  }

  @Override
  public ValidationResult validate(Config config) {
    if (config.hasPath(path)) {
      if (allowedValues.contains(config.getAnyRef(path))) {
        return new ValidationResult(this, Validity.VALID, "Configuration '" + path +
            "' has one of the allowed values: " + allowedValues);
      }
      else {
        return new ValidationResult(this, Validity.INVALID, "Configuration '" + path +
            "' does not have one of the allowed values: " + allowedValues);
      }
    }
    else {
      return new ValidationResult(this, Validity.VALID, "Configuration '" + path +
          "' does not exist, so will not check it for allowed values");
    }
  }
  
  @Override
  public String toString() {
    return "Allowed values validation that configuration '" + path +
        "' has one of values " + allowedValues;
  }

  @Override
  public Set<String> getKnownPaths() {
    return Sets.newHashSet(path);
  }

}
