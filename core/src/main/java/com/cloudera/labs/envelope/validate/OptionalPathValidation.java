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

import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;

import java.util.Set;

public class OptionalPathValidation implements Validation {

  private String path;
  private ConfigValueType type;
  
  public OptionalPathValidation(String path) {
    this.path = path;
  }
  
  public OptionalPathValidation(String path, ConfigValueType type) {
    this.path = path;
    this.type = type;
  }

  @Override
  public ValidationResult validate(Config config) {
    if (config.hasPath(path)) {
      if (type != null) {
        if (config.getValue(path).valueType().equals(type) ||
            ConfigUtils.canBeCoerced(config, path, type))
        {
          return new ValidationResult(this, Validity.VALID,
              "Optional configuration was found with required type: " + path);
        }
        else {
          return new ValidationResult(this, Validity.INVALID,
              "Optional configuration '" + path + "' was found but does not have required type: " + type);
        }
      }
      else {
        return new ValidationResult(this, Validity.VALID,
            "Optional configuration was found: " + path);
      }
    }
    else {
      return new ValidationResult(this, Validity.VALID,
          "Optional configuration was not found: " + path);
    }
  }
  
  @Override
  public String toString() {
    if (type != null) {
      return "Optional path validation that configuration '" + path +
          "' may exist, and if it does it must have type '" + type + "'";
    }
    else {
      return "Optional path validation that configuration '" + path + "' may exist";
    }
  }

  @Override
  public Set<String> getKnownPaths() {
    return Sets.newHashSet(path);
  }
  
}
