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

package com.cloudera.labs.envelope.run;

import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;

import java.util.Set;

/**
 * A step is a unit of work to be submitted in dependency order.
 */
public abstract class Step implements ProvidesValidations {

  public static final String DEPENDENCIES_CONFIG = "dependencies";

  protected String name;
  protected Config config;
  
  private boolean submitted = false;
  private Set<String> dependencyNames;

  public Step(String name) {
    this.name = name;
  }

  public void configure(Config config) {
    this.config = config;

    if (config.hasPath(DEPENDENCIES_CONFIG)) {
      dependencyNames = Sets.newHashSet(config.getStringList(DEPENDENCIES_CONFIG));
    }
    else {
      dependencyNames = Sets.newHashSet();
    }
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
  
  public Config getConfig() {
    return config;
  }
  
  public void setConfig(Config config) {
    this.config = config;
  }

  public boolean hasSubmitted() {
    return submitted;
  }

  public void setSubmitted(boolean submitted) {
    this.submitted = submitted;
  }

  public Set<String> getDependencyNames() {
    return dependencyNames;
  }
  
  public void setDependencyNames(Set<String> dependencyNames) {
    this.dependencyNames = dependencyNames;
  }
  
  public abstract Step copy();

  // Can be overridden if the step holds additional state
  public void reset() {
    setSubmitted(false);
  }
  
  @Override
  public Validations getValidations() {
    return Validations.builder()
        .optionalPath(DEPENDENCIES_CONFIG, ConfigValueType.LIST)
        .build();
  }
  
  @Override
  public String toString() {
    return getName() + " " + getDependencyNames() + " " + hasSubmitted();
  }

}
