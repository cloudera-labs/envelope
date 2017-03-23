/**
 * Copyright © 2016-2017 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

public abstract class Step {

  protected String name;
  protected Config config;

  public Step(String name, Config config) throws Exception {
    this.name = name;
    this.config = config;
  }

  public String getName() {
    return name;
  }

  public Set<String> getDependencyNames() {
    if (!config.hasPath("dependencies")) {
      return Sets.newHashSet();
    }

    Set<String> dependencyNames = Sets.newHashSet(config.getStringList("dependencies"));

    return dependencyNames;
  }

}
