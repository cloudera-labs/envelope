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

import com.cloudera.labs.envelope.component.InstantiatedComponent;
import com.cloudera.labs.envelope.component.InstantiatesComponents;
import com.cloudera.labs.envelope.run.BatchStep;
import com.cloudera.labs.envelope.run.StreamingStep;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for Envelope to run configuration validations.
 */
public class Validator {

  public static final String CONFIGURATION_VALIDATION_ENABLED_PROPERTY = "configuration.validation.enabled";
  public static final boolean CONFIGURATION_VALIDATION_ENABLED_DEFAULT = true;

  private static Logger LOG = LoggerFactory.getLogger(Validator.class);

  // Validate the instantiated validatable component against the proposed configuration.
  public static List<ValidationResult> validate(ProvidesValidations forValidation, Config config) {
    // Allow the configuration validation to be disabled at any scope. If it is disabled
    // at a scope then all configuration validations underneath it will also be disabled.
    if (!ConfigUtils.getOrElse(config, CONFIGURATION_VALIDATION_ENABLED_PROPERTY, CONFIGURATION_VALIDATION_ENABLED_DEFAULT)) {
      return Lists.newArrayList(new ValidationResult(Validity.VALID,
          "Configuration validation disabled at this scope"));
    }

    Validations vs = forValidation.getValidations();
    List<ValidationResult> vrs = Lists.newArrayList();
    Set<String> knownPaths = Sets.newHashSet();
    // The paths that we always forgive being unrecognized. This is to prevent every component
    // requiring a validation on the 'type' configuration, which is used by Envelope to
    // instantiate the component and typically not by the component itself.
    Set<String> forgivenUnrecognizedPaths = Sets.newHashSet("type");
    // We have a strange situation where two configs are owned one component above where they are
    // provided. This messes things up. Until we fix ENV-286 we will forgive the components
    // that specify the configs for not knowing about them.
    // TODO: fix ENV-286 and then remove these items
    forgivenUnrecognizedPaths.addAll(Sets.newHashSet(
        BatchStep.REPARTITION_COLUMNS_PROPERTY,
        BatchStep.REPARTITION_NUM_PARTITIONS_PROPERTY,
        BatchStep.COALESCE_NUM_PARTITIONS_PROPERTY,
        StreamingStep.REPARTITION_NUM_PARTITIONS_PROPERTY));

    // Validate the configuration for each of the validation rules of the component, and keep
    // track of all of the known paths so we can later check for unknown paths.
    for (Validation v : vs) {
      try {
        vrs.add(v.validate(config));
        knownPaths.addAll(v.getKnownPaths());
      }
      catch (Exception e) {
        // If a validation itself throws an exception then we want to swallow it with a debug
        // log message because it would be a very poor user experience for the validation process
        // to fail when the point of validation is to assist with avoiding failures.
        if (LOG.isDebugEnabled()) {
          LOG.debug("Validation '" + v.getClass().getName() + "' for class '" +
              forValidation.getClass().getName() + "' threw below exception for config '" +
              config + "'");
          e.printStackTrace();
        }
      }
    }

    // If the component itself instantiates other components then we want to dive down into those
    // instantiated components and perform configuration validation there too. In this way we
    // traverse the entire tree of components in the pipeline. We do not do this traversal if the
    // configuration in the current component was found to be invalid because that may lead to
    // a large cascade of errors down the component stack that are not meaningful to the user.
    if (forValidation instanceof InstantiatesComponents &&
        !ValidationUtils.hasValidationFailures(vrs))
    {
      Set<InstantiatedComponent> forValidationComponents = Sets.newHashSet();

      // Gather all of the components that this component instantiates
      try {
        forValidationComponents.addAll(((InstantiatesComponents)forValidation).getComponents(config, false));
      } catch (Exception e) {
        if (e.getCause() instanceof ClassNotFoundException) {
          vrs.add(new ValidationResult(Validity.INVALID,
              "Could not find a class that was expected to be used in the pipeline. " +
                  "A common cause for this is when the jar file for a plugin is not provided on the " +
                  "classpath using --jars. See the exception below for more information on the missing class.", e));
        }
        else {
          vrs.add(new ValidationResult(Validity.INVALID,
              "Could not create component(s) defined by class: " +
                  forValidation.getClass().getName() + ". For more information see the exception below.", e));
        }
      }

      // Validate each of the instantiated components
      for (InstantiatedComponent forValidationComponent : forValidationComponents) {
        if (forValidationComponent.getComponent() instanceof ProvidesValidations) {
          List<ValidationResult> results = Validator.validate(
              (ProvidesValidations)forValidationComponent.getComponent(), forValidationComponent.getConfig());
          ValidationUtils.prefixValidationResultMessages(results, forValidationComponent.getLabel());
          vrs.addAll(results);
        }
      }
    }

    // Check the component configuration for paths that were not declared as known by any of the
    // validation rules. If those are not allowed, and are not in the set of forgiven paths, and
    // are not handled further down the component stack, then that is itself a validation failure.
    if (!vs.allowsUnrecognizedPaths()) {
      Set<String> unrecognizedPaths = Sets.newHashSet();
      
      for (Entry<String, ConfigValue> configEntry : config.entrySet()) {
        String path = configEntry.getKey();
        
        if (!knownPaths.contains(path) && !forgivenUnrecognizedPaths.contains(path)) {
          boolean doesOwnValidation = false;
          for (String ownValidatingPath : vs.getOwnValidatingPaths()) {
            if (path.startsWith(ownValidatingPath)) {
              doesOwnValidation = true;
            }
          }
          
          if (!doesOwnValidation) {
            unrecognizedPaths.add(path);
          }
        }
      }
      
      if (!unrecognizedPaths.isEmpty()) {
        vrs.add(new ValidationResult(Validity.INVALID, "Unrecognized configuration(s) found: " + unrecognizedPaths));
      }
      else {
        vrs.add(new ValidationResult(Validity.VALID, "No unrecognized configurations found"));
      }
    }
    
    return vrs;
  }
  
}
