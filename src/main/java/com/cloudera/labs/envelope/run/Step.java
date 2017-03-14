package com.cloudera.labs.envelope.run;

import java.util.Set;

import org.apache.kudu.client.shaded.com.google.common.collect.Sets;

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
