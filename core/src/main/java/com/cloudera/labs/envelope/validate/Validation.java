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

import java.util.Set;

/**
 * A configuration validation rule for an Envelope component.
 */
public interface Validation {

  /**
   * @param config The configuration to be validated for the component.
   * @return The result of the validation.
   */
  ValidationResult validate(Config config);

  /**
   * @return The set of configuration paths that the validation rule is aware of. The set of
   * known paths from all validation rules of the component allow Envelope to check for
   * provided configurations that are not known.
   */
  Set<String> getKnownPaths();
  
}
