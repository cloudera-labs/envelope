/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.validate;

/**
 * The result of running a validation rule against a component configuration.
 */
public class ValidationResult {

  private Validity validity;
  private String message;
  private Exception exception;

  /**
   * @param validity Whether the configuration was validity for the validation rule.
   * @param message A human readable message back to the user that describes the outcome of the
   *                validation. Messages should be provided even if the configuration was found
   *                to be validity.
   */
  public ValidationResult(Validity validity, String message) {
    this(validity, message, null);
  }

  /**
   * @param validity Whether the configuration was validity for the validation rule.
   * @param message A human readable message back to the user that describes the outcome of the
   *                validation. A message should be provided even if the configuration was found
   *                to be validity.
   * @param exception The exception raised during the validation. If there was no exception then
   *                  null can be provided, otherwise use the exception-less constructor of this
   *                  class.
   */
  public ValidationResult(Validity validity, String message, Exception exception) {
    this.validity = validity;
    this.exception = exception;
    setMessage(message);
  }

  public Validity getValidity() {
    return validity;
  }

  public String getMessage() {
    return message;
  }
  
  public void setMessage(String message) {
    if (message == null) {
      throw new RuntimeException("Validation result message can not be null");
    }
    
    this.message = message;
  }

  public boolean hasException() {
    return exception != null;
  }

  public Exception getException() {
    return exception;
  }

}
