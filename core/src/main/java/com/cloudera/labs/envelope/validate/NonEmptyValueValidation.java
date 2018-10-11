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
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigValueType;

import java.util.Set;

public class NonEmptyValueValidation implements Validation {

  private String path;

  public NonEmptyValueValidation(String path) {
    this.path = path;
  }

  @Override
  public ValidationResult validate(Config config) {
    try {
      config.getAnyRef(path);
    }
    // There is a 'getIsNull' API as of Config v1.3, but we are on v1.2.1
    catch (ConfigException.Null e) {
      return new ValidationResult(this, Validity.INVALID, "Configuration '" + path +
          "' is null but must be non-empty");
    }
    catch (Exception e) {
      // Deal with these scenarios in the following code
    }

    if (config.hasPath(path)) {
      if (config.getValue(path).valueType() == ConfigValueType.STRING &&
          config.getString(path).isEmpty()) {
        return new ValidationResult(this, Validity.INVALID, "Configuration '" + path +
            "' is an empty string but must be non-empty");
      }
      else if (config.getValue(path).valueType() == ConfigValueType.LIST &&
               config.getAnyRefList(path).size() == 0) {
        return new ValidationResult(this, Validity.INVALID, "Configuration '" + path +
            "' is an empty list but must be non-empty");
      }
      else {
        return new ValidationResult(this, Validity.VALID, "Configuration '" + path +
            "' is non-empty");
      }
    }
    else {
      return new ValidationResult(this, Validity.VALID, "Configuration '" + path +
          "' does not exist so not checking if non-empty");
    }
  }

  @Override
  public String toString() {
    return "Non-empty value validation that '" + path + "' must not contain a null or empty string";
  }

  @Override
  public Set<String> getKnownPaths() {
    return Sets.newHashSet(path);
  }

}
