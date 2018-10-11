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

package com.cloudera.labs.envelope.component;

import com.typesafe.config.Config;

import java.util.Set;

/**
 * Components that themselves instantiate other components must implement this interface.
 * This is used by Envelope to traverse the tree of components for purposes such as configuration
 * validation and identifying security requirements.
 */
public interface InstantiatesComponents {

  /**
   * @param config The configuration of the callee component
   * @param configure Whether the callee component should configure its instantiated components
   * @return The set of the callee component's instantiated components.
   */
  Set<InstantiatedComponent> getComponents(Config config, boolean configure) throws Exception;
  
}
