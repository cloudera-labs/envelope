package com.cloudera.labs.envelope.plan;

import java.util.Set;

import com.typesafe.config.Config;

public interface Planner {

  void configure(Config config);

  Set<MutationType> getEmittedMutationTypes();

}
