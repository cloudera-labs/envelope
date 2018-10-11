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

public class AtMostOnePathExistsValidation implements Validation {

  private Set<String> paths;
  private ConfigValueType type;

  public AtMostOnePathExistsValidation(String... paths) {
    this.paths = Sets.newHashSet(paths);
  }

  public AtMostOnePathExistsValidation(ConfigValueType type, String... paths) {
    this.paths = Sets.newHashSet(paths);
    this.type = type;
  }
  
  @Override
  public ValidationResult validate(Config config) {
    int found = 0;
    
    for (String path : paths) {
      if (config.hasPath(path)) {
        if (found == 1) {
          return new ValidationResult(this, Validity.INVALID,
              "More than one of the following configurations was found, " +
                  "when at most one must exist: " + paths);
        }
        else {
          if (type != null &&
              !config.getValue(path).valueType().equals(type) &&
              !ConfigUtils.canBeCoerced(config, path, type))
          {
            return new ValidationResult(this, Validity.INVALID,
                "The configuration '" + path + "' was found but not of required type: " + type);
          }

          found++;
        }
      }
    }
    
    if (found == 1) {
      return new ValidationResult(this, Validity.VALID,
          "One of the following configurations was found, when at most one must exist: " + paths);
    }
    else {
      return new ValidationResult(this, Validity.VALID,
          "None of the following configurations was found, when at most one must exist: " + paths);
    }
  }
  
  @Override
  public String toString() {
    return "At most one path exists validation that there is at most one of paths " + paths;
  }

  @Override
  public Set<String> getKnownPaths() {
    return paths;
  }
  
}
