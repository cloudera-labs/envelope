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

import java.util.Objects;

/**
 * A class that Envelope uses to retrieve instantiated components.
 */
public class InstantiatedComponent {

  private Component component;
  private Config config;
  private String label;

  public InstantiatedComponent(Component component, Config config, String label) {
    this.component = component;
    this.config = config;
    this.label = label;
  }

  public Component getComponent() {
    return component;
  }

  public Config getConfig() {
    return config;
  }

  public String getLabel() {
    return label;
  }

  @Override
  public int hashCode() {
    return Objects.hash(component, config, label);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) return false;
    if (!(obj instanceof InstantiatedComponent)) return false;

    InstantiatedComponent ic = (InstantiatedComponent)obj;
    return component.equals(ic.getComponent()) &&
           config.equals(ic.getConfig()) &&
           label.equals(ic.getLabel());
  }

}
