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

import org.apache.hadoop.security.token.Token;

/**
 * Obtains security tokens for components interacting with external systems. Called on the driver by the Envelope
 * {@link TokenStoreManager} class.
 */
public interface TokenProvider {

  /**
   * Get a new security token for the service. Services which do not use Hadoop tokens can create a generic
   * EnvelopeToken using the {@link SecurityUtils#createToken(byte[], int)} method.
   *
   * @return a Hadoop {@link Token}
   */
  Token obtainToken() throws Exception;

  /**
   * When should a new token be obtained.
   * @return renewal interval in milliseconds
   */
  long getRenewalIntervalMillis();

  /**
   * Get the alias for this provider. This should be unique per external system.
   * @return the alias for this provider
   */
  String getAlias();

}
