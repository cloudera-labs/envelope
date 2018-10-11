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
import com.typesafe.config.ConfigValueType;

import java.util.HashSet;
import java.util.Set;

/**
 * The set of validation rules that can be run against a component. Use {@link Validations#builder()}
 * or {@link Validations#single()} to create a {@code Validations} object.
 */
@SuppressWarnings("serial")
public class Validations extends HashSet<Validation> {

  private boolean allowUnrecognizedPaths = false;
  private Set<String> ownValidatingPaths = Sets.newHashSet();
  private Set<String> allowEmptyValuePaths = Sets.newHashSet();
  
  private Validations() {}

  /**
   * @return A builder for a single validation rule. Typically used to provide a validation rule
   * within another validation rule.
   */
  public static ValidationBuilder single() {
    return new ValidationBuilder();
  }

  /**
   * @return A builder for a set of validation rules. The builder allows validation rules to
   * be chained together, and then finalized with {@code build()}.
   */
  public static ValidationsBuilder builder() {
    return new ValidationsBuilder();
  }
  
  public boolean allowsUnrecognizedPaths() {
    return allowUnrecognizedPaths;
  }
  
  public Set<String> getOwnValidatingPaths() {
    return ownValidatingPaths;
  }

  public static class ValidationBuilder {
    private ValidationBuilder() {}

    /**
     * This configuration must be present in the component configuration.
     */
    public Validation mandatoryPath(String path) {
      return new MandatoryPathValidation(path);
    }

    /**
     * This configuration must be present in the component configuration, and must have
     * this data type.
     */
    public Validation mandatoryPath(String path, ConfigValueType type) {
      return new MandatoryPathValidation(path, type);
    }

    /**
     * This configuration may or may not be present in the component configuration.
     */
    public Validation optionalPath(String path) {
      return new OptionalPathValidation(path);
    }

    /**
     * This configuration may or may not be present in the component configuration, but if
     * it is present then it must have this data type.
     */
    public Validation optionalPath(String path, ConfigValueType type) {
      return new OptionalPathValidation(path, type);
    }

    /**
     * Zero or one of these configurations can exist in the component configuration.
     */
    public Validation atMostOnePathExists(String... paths) {
      return new AtMostOnePathExistsValidation(paths);
    }

    /**
     * Zero or one of these configurations can exist in the component configuration, but
     * if there is one then it must have this data type.
     */
    public Validation atMostOnePathExists(ConfigValueType type, String... paths) {
      return new AtMostOnePathExistsValidation(type, paths);
    }

    /**
     * Exactly one of these configurations must exist in the component configuration.
     */
    public Validation exactlyOnePathExists(String... paths) {
      return new ExactlyOnePathExistsValidation(paths);
    }

    /**
     * Exactly one of these configurations must exist in the component configuration, and must
     * have this data type.
     */
    public Validation exactlyOnePathExists(ConfigValueType type, String... paths) {
      return new ExactlyOnePathExistsValidation(type, paths);
    }

    /**
     * If this configuration has this value, then run this configuration validation.
     */
    public Validation ifPathHasValue(String ifPath, Object hasValue, Validation thenValidation) {
      return new IfPathHasValueValidation(ifPath, hasValue, thenValidation);
    }

    /**
     * If this configuration exists, then run this configuration validation.
     */
    public Validation ifPathExists(String ifPath, Validation thenValidation) {
      return new IfPathExistsValidation(ifPath, thenValidation);
    }

    /**
     * The only values that are allowed for this configuration.
     */
    public Validation allowedValues(String path, Object... values) {
      return new AllowedValuesValidation(path, values);
    }
  }

  public static class ValidationsBuilder {
    private Validations v = new Validations();
    
    private ValidationsBuilder() {}

    /**
     * Add this validation. Typically used to add validations that are not already known to
     * the {@code Validations} builder.
     */
    public ValidationsBuilder add(Validation validation) {
      v.add(validation);
      return this;
    }

    /**
     * Add all of these validations, and consider them already validated elsewhere. Typically used
     * to add validations from the super class of the component.
     */
    public ValidationsBuilder addAll(Validations validations) {
      v.addAll(validations);
      // TODO: why do we need this?
      v.getOwnValidatingPaths().addAll(validations.getOwnValidatingPaths());
      return this;
    }

    /**
     * This configuration must be present in the component configuration.
     */
    public ValidationsBuilder mandatoryPath(String path) {
      v.add(new MandatoryPathValidation(path));
      return this;
    }

    /**
     * This configuration must be present in the component configuration, and must have
     * this data type.
     */
    public ValidationsBuilder mandatoryPath(String path, ConfigValueType type) {
      v.add(new MandatoryPathValidation(path, type));
      return this;
    }

    /**
     * This configuration may or may not be present in the component configuration.
     */
    public ValidationsBuilder optionalPath(String path) {
      v.add(new OptionalPathValidation(path));
      return this;
    }

    /**
     * This configuration may or may not be present in the component configuration, but if
     * it is present then it must have this data type.
     */
    public ValidationsBuilder optionalPath(String path, ConfigValueType type) {
      v.add(new OptionalPathValidation(path, type));
      return this;
    }

    /**
     * Zero or one of these configurations can exist in the component configuration.
     */
    public ValidationsBuilder atMostOnePathExists(String... paths) {
      v.add(new AtMostOnePathExistsValidation(paths));
      return this;
    }

    /**
     * Zero or one of these configurations can exist in the component configuration, but
     * if there is one then it must have this data type.
     */
    public ValidationsBuilder atMostOnePathExists(ConfigValueType type, String... paths) {
      v.add(new AtMostOnePathExistsValidation(type, paths));
      return this;
    }

    /**
     * Exactly one of these configurations must exist in the component configuration.
     */
    public ValidationsBuilder exactlyOnePathExists(String... paths) {
      v.add(new ExactlyOnePathExistsValidation(paths));
      return this;
    }

    /**
     * Exactly one of these configurations must exist in the component configuration, and must
     * have this data type.
     */
    public ValidationsBuilder exactlyOnePathExists(ConfigValueType type, String... paths) {
      v.add(new ExactlyOnePathExistsValidation(type, paths));
      return this;
    }

    /**
     * If this configuration has this value, then run this configuration validation.
     */
    public ValidationsBuilder ifPathHasValue(String ifPath, Object hasValue, Validation thenValidation) {
      v.add(new IfPathHasValueValidation(ifPath, hasValue, thenValidation));
      return this;
    }

    /**
     * If this configuration exists, then run this configuration validation.
     */
    public ValidationsBuilder ifPathExists(String ifPath, Validation thenValidation) {
      v.add(new IfPathExistsValidation(ifPath, thenValidation));
      return this;
    }

    /**
     * The only values that are allowed for this configuration.
     */
    public ValidationsBuilder allowedValues(String path, Object... values) {
      v.add(new AllowedValuesValidation(path, values));
      return this;
    }

    /**
     * Allow this component to have paths that are not known to any of the validations. Otherwise,
     * by default Envelope will fail a component configuration if it finds unknown configurations.
     */
    public ValidationsBuilder allowUnrecognizedPaths() {
      v.allowUnrecognizedPaths = true;
      return this;
    }

    /**
     * Allow this configuration to have an empty value, such as a null or an empty string. Otherwise,
     * by default Envelope will assume that configurations can not have empty values.
     */
    public ValidationsBuilder allowEmptyValue(String path) {
      v.allowEmptyValuePaths.add(path);
      return this;
    }

    /**
     * Mark this configuration as a component that handles its own validation. This is important for
     * when a component instantiates other components.
     */
    public ValidationsBuilder handlesOwnValidationPath(String path) {
      v.ownValidatingPaths.add(path);
      return this;
    }
    
    public Validations build() {
      v.addAll(getNonEmptyValidations());
      return v;
    }

    private Set<Validation> getNonEmptyValidations() {
      Set<Validation> nonEmptyValidations = Sets.newHashSet();

      Set<String> knownPaths = Sets.newHashSet();
      for (Validation validation : v) {
        knownPaths.addAll(validation.getKnownPaths());
      }

      for (String knownPath : knownPaths) {
        if (!v.allowEmptyValuePaths.contains(knownPath)) {
          nonEmptyValidations.add(new NonEmptyValueValidation(knownPath));
        }
      }

      return nonEmptyValidations;
    }
  }
  
}
