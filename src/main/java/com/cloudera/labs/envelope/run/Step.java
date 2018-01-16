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
package com.cloudera.labs.envelope.run;

import java.util.Set;

import com.google.common.collect.Sets;
import com.typesafe.config.Config;

/**
 * A step is a unit of work to be submitted in dependency order.
 */
public abstract class Step {

  protected String name;
  protected Config config;
  
  private boolean submitted = false;
  private Set<String> dependencyNames;
  
  public Step(String name, Config config) {
    this.name = name;
    this.config = config;
    
    if (config.hasPath("dependencies")) {
      dependencyNames = Sets.newHashSet(config.getStringList("dependencies"));
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
  
  @Override
  public String toString() {
    return getName() + " " + getDependencyNames() + " " + hasSubmitted();
  }

}
