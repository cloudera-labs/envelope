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

/**
 * Components that provide configuration validation rules should implement this interface.
 * Before configuring the component Envelope will automatically retrieve these validation rules
 * and run them against the pipeline configuration for the component.
 */
public interface ProvidesValidations {

  /**
   * @return The configuration validation rules for the component. Use {@link Validations#builder} or
   * {@link Validations#single} to create a Validations object.
   */
  Validations getValidations();
  
}
