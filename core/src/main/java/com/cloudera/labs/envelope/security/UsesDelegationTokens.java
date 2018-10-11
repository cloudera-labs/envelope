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

package com.cloudera.labs.envelope.security;

public interface UsesDelegationTokens {

  /**
   * Get the {@link TokenProvider} used by this component
   * This is called once by Envelope and only in the driver after instantiation. Only returns an
   * instance if security is required or null otherwise.
   * @return A TokenProvider, or null if none required
   */
  TokenProvider getTokenProvider();

}
